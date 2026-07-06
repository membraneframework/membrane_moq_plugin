use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use rustler::{Atom, LocalPid, NifResult, Resource, ResourceArc};

use hang::moq_net;

use crate::messages::{self, Token};
use crate::session::SessionResource;
use crate::track_format::{audio_params, video_params, TrackParams};
use crate::{atoms, runtime};

enum Command {
    Subscribe {
        track: String,
        token: Token,
        priority: u8,
    },
    Unsubscribe {
        token: Token,
    },
}

pub(crate) struct BroadcastConsumerResource {
    commands: mpsc::UnboundedSender<Command>,
    shutdown: mpsc::UnboundedSender<()>,
}

impl Resource for BroadcastConsumerResource {}

#[derive(Clone, PartialEq)]
struct Rendition {
    params: TrackParams,
    container: hang::catalog::Container,
}

type CatalogSnapshot = HashMap<String, Rendition>;

struct ConsumerState {
    renditions: CatalogSnapshot,
    pumps: JoinSet<()>,
    cancels: HashMap<Token, watch::Sender<bool>>,
    pending: HashMap<Token, (String, u8, watch::Receiver<bool>)>,
}

struct Ctx<'a> {
    broadcast: &'a moq_net::BroadcastConsumer,
    path: &'a str,
    latency: Duration,
    pid: &'a LocalPid,
}

#[rustler::nif]
pub(crate) fn create_broadcast_consumer(
    session: ResourceArc<SessionResource>,
    path: String,
    pid: LocalPid,
    latency_ns: u64,
) -> NifResult<(Atom, ResourceArc<BroadcastConsumerResource>)> {
    let latency = Duration::from_nanos(latency_ns);

    // A clone with its own announcement cursor, so each broadcast consumer
    // awaits its broadcast independently of any sibling consumers.
    let origin = session.consume.lock().unwrap().consume();

    let (commands_tx, commands_rx) = mpsc::unbounded_channel::<Command>();
    let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(run_broadcast(
        origin,
        path,
        pid,
        latency,
        commands_rx,
        shutdown_rx,
    ));

    Ok((
        atoms::ok(),
        ResourceArc::new(BroadcastConsumerResource {
            commands: commands_tx,
            shutdown: shutdown_tx,
        }),
    ))
}

#[rustler::nif]
pub(crate) fn subscribe_track(
    consumer: ResourceArc<BroadcastConsumerResource>,
    track: String,
    token: Token,
    priority: u8,
) -> Atom {
    let _ = consumer.commands.send(Command::Subscribe {
        track,
        token,
        priority,
    });
    atoms::ok()
}

#[rustler::nif]
pub(crate) fn unsubscribe_track(
    consumer: ResourceArc<BroadcastConsumerResource>,
    token: Token,
) -> Atom {
    let _ = consumer.commands.send(Command::Unsubscribe { token });
    atoms::ok()
}

#[rustler::nif]
pub(crate) fn close_broadcast_consumer(consumer: ResourceArc<BroadcastConsumerResource>) -> Atom {
    let _ = consumer.shutdown.send(());
    atoms::ok()
}

async fn run_broadcast(
    origin: moq_net::OriginConsumer,
    path: String,
    pid: LocalPid,
    latency: Duration,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
    mut shutdown_rx: mpsc::UnboundedReceiver<()>,
) {
    let broadcast = tokio::select! {
        broadcast = origin.announced_broadcast(path.as_str()) => match broadcast {
            Some(broadcast) => broadcast,
            None => {
                messages::send_broadcast_closed(
                    &pid,
                    &path,
                    format!("broadcast {path:?} was not announced before the session closed"),
                );
                return;
            }
        },
        _ = shutdown_rx.recv() => return,
    };

    let mut catalog = match subscribe_catalog(&broadcast) {
        Ok(catalog) => catalog,
        Err(e) => {
            messages::send_broadcast_closed(&pid, &path, e.to_string());
            return;
        }
    };

    messages::send_broadcast_ready(&pid, &path);

    let ctx = Ctx {
        broadcast: &broadcast,
        path: &path,
        latency,
        pid: &pid,
    };

    let mut state = ConsumerState {
        renditions: HashMap::new(),
        pumps: JoinSet::new(),
        cancels: HashMap::new(),
        pending: HashMap::new(),
    };

    let close_reason = loop {
        tokio::select! {
            command = commands_rx.recv() => match command {
                Some(command) => state = handle_command(command, state, &ctx),
                // The resource was dropped without a close; treat as shutdown.
                None => return,
            },
            _ = shutdown_rx.recv() => return,
            _ = state.pumps.join_next(), if !state.pumps.is_empty() => {}
            snapshot = catalog.next() => {
                state = match handle_new_catalog(snapshot, state, &ctx) {
                    Ok(state) => state,
                    Err(reason) => break reason,
                };
            }
        }
    };

    messages::send_broadcast_closed(&pid, &path, close_reason);
}

fn handle_command(command: Command, mut state: ConsumerState, ctx: &Ctx) -> ConsumerState {
    match command {
        Command::Subscribe {
            track,
            token,
            priority,
        } => {
            let (cancel_tx, cancel_rx) = watch::channel(false);
            state.cancels.insert(token, cancel_tx);
            match state.renditions.get(&track) {
                Some(rendition) => {
                    messages::send_track_format(ctx.pid, token, &rendition.params);
                    let pump = Pump::new(ctx, track, rendition, token, priority);
                    state.pumps.spawn(pump.run(cancel_rx));
                }
                None => {
                    state.pending.insert(token, (track, priority, cancel_rx));
                }
            }
        }
        Command::Unsubscribe { token } => {
            if let Some(cancel) = state.cancels.remove(&token) {
                let _ = cancel.send(true);
            }
            state.pending.remove(&token);
        }
    }
    state
}

fn handle_new_catalog(
    snapshot: Result<Option<moq_mux::catalog::hang::Catalog<()>>, moq_mux::Error>,
    mut state: ConsumerState,
    ctx: &Ctx,
) -> Result<ConsumerState, String> {
    let new_renditions = update_catalog(ctx, snapshot, &state.renditions)?;

    // Start pumps for any parked commands the snapshot just resolved.
    state
        .pending
        .extract_if(|_, (track, _, _)| new_renditions.contains_key(track.as_str()))
        .for_each(|(token, (track, priority, cancel_rx))| {
            let rendition = &new_renditions[&track];
            messages::send_track_format(ctx.pid, token, &rendition.params);
            let pump = Pump::new(ctx, track, rendition, token, priority);
            state.pumps.spawn(pump.run(cancel_rx));
        });

    state.renditions = new_renditions;
    Ok(state)
}

fn update_catalog(
    ctx: &Ctx,
    snapshot: Result<Option<moq_mux::catalog::hang::Catalog<()>>, moq_mux::Error>,
    old_catalog: &CatalogSnapshot,
) -> Result<CatalogSnapshot, String> {
    let snapshot = match snapshot {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return Err("broadcast ended".to_string()),
        Err(e) => return Err(format!("catalog error: {e}")),
    };

    let new_catalog = catalog_renditions(&snapshot);

    let pid_dead = |_| "consumer pid is dead".to_string();
    for (name, rendition) in old_catalog {
        if new_catalog.get(name) != Some(rendition) {
            messages::send_track_removed(ctx.pid, ctx.path, name).map_err(pid_dead)?;
        }
    }
    for (name, rendition) in &new_catalog {
        if old_catalog.get(name) != Some(rendition) {
            messages::send_track_added(ctx.pid, ctx.path, name, &rendition.params)
                .map_err(pid_dead)?;
        }
    }

    Ok(new_catalog)
}

/// Everything one track subscription needs to pump its frames to Elixir.
struct Pump {
    broadcast: moq_net::BroadcastConsumer,
    track: String,
    container: hang::catalog::Container,
    token: Token,
    priority: u8,
    pid: LocalPid,
    latency: Duration,
}

impl Pump {
    fn new(ctx: &Ctx, track: String, rendition: &Rendition, token: Token, priority: u8) -> Self {
        Self {
            broadcast: ctx.broadcast.clone(),
            track,
            container: rendition.container.clone(),
            token,
            priority,
            pid: *ctx.pid,
            latency: ctx.latency,
        }
    }

    async fn run(self, mut cancel: watch::Receiver<bool>) {
        let reason = tokio::select! {
            _ = cancel.changed() => return,
            result = self.pump_track() => match result {
                Ok(()) => "track ended".to_string(),
                Err(e) => format!("track error: {e}"),
            }
        };

        messages::send_track_ended(&self.pid, self.token, reason);
    }

    async fn pump_track(&self) -> anyhow::Result<()> {
        let track = &self.track;

        let wire = moq_mux::catalog::hang::Container::try_from(&self.container)
            .map_err(|e| anyhow::anyhow!("unsupported container on track {track}: {e}"))?;

        let track_ref = moq_net::Track {
            name: track.clone(),
            priority: self.priority,
        };
        let track_consumer = self
            .broadcast
            .subscribe_track(&track_ref)
            .map_err(|e| anyhow::anyhow!("subscribe_track({track}) failed: {e}"))?;

        let mut consumer =
            moq_mux::container::Consumer::new(track_consumer, wire).with_latency(self.latency);

        pump_frames(&mut consumer, self.token, &self.pid).await
    }
}

fn subscribe_catalog(
    broadcast: &moq_net::BroadcastConsumer,
) -> anyhow::Result<moq_mux::catalog::hang::Consumer<()>> {
    let catalog_track = broadcast
        .subscribe_track(&hang::Catalog::default_track())
        .map_err(|e| anyhow::anyhow!("subscribe_track(catalog) failed: {e}"))?;
    Ok(moq_mux::catalog::hang::Consumer::<()>::new(catalog_track))
}

fn catalog_renditions(snapshot: &moq_mux::catalog::hang::Catalog) -> CatalogSnapshot {
    let mut renditions = HashMap::new();
    for (name, config) in &snapshot.video.renditions {
        let rendition = Rendition {
            params: video_params(config),
            container: config.container.clone(),
        };
        renditions.insert(name.clone(), rendition);
    }
    for (name, config) in &snapshot.audio.renditions {
        let rendition = Rendition {
            params: audio_params(config),
            container: config.container.clone(),
        };
        renditions.insert(name.clone(), rendition);
    }
    renditions
}

async fn pump_frames(
    consumer: &mut moq_mux::container::Consumer<moq_mux::catalog::hang::Container>,
    token: Token,
    pid: &LocalPid,
) -> anyhow::Result<()> {
    while let Some(frame) = consumer.read().await? {
        let timestamp_ns = frame.timestamp.as_nanos() as i64;

        messages::send_frame(pid, token, &frame.payload, timestamp_ns, frame.keyframe)
            .map_err(|_| anyhow::anyhow!("consumer pid is dead"))?;
    }
    Ok(())
}
