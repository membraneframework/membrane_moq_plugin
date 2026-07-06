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
    Subscribe { track: String, token: Token },
    Unsubscribe { token: Token },
}

pub(crate) struct BroadcastConsumerResource {
    commands: mpsc::UnboundedSender<Command>,
    shutdown: mpsc::UnboundedSender<()>,
}

impl Resource for BroadcastConsumerResource {}

type CatalogSnapshot = HashMap<String, TrackParams>;

struct ConsumerState {
    params: CatalogSnapshot,
    pumps: JoinSet<()>,
    cancels: HashMap<Token, watch::Sender<bool>>,
    pending: HashMap<Token, (String, watch::Receiver<bool>)>,
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
) -> Atom {
    let _ = consumer.commands.send(Command::Subscribe { track, token });
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
        params: HashMap::new(),
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
        Command::Subscribe { track, token } => {
            let (cancel_tx, cancel_rx) = watch::channel(false);
            state.cancels.insert(token, cancel_tx);
            match state.params.get(&track) {
                Some(track_params) => {
                    messages::send_track_format(ctx.pid, token, track_params);
                    state.pumps.spawn(run_pump(
                        ctx.broadcast.clone(),
                        track,
                        token,
                        *ctx.pid,
                        ctx.latency,
                        cancel_rx,
                    ));
                }
                None => {
                    state.pending.insert(token, (track, cancel_rx));
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
    let new_params = update_catalog(ctx, snapshot, &state.params)?;

    // Start pumps for any parked commands the snapshot just resolved.
    let ready: Vec<Token> = state
        .pending
        .iter()
        .filter(|(_, (track, _))| new_params.contains_key(track))
        .map(|(token, _)| *token)
        .collect();
    for token in ready {
        let (track, cancel_rx) = state.pending.remove(&token).unwrap();
        messages::send_track_format(ctx.pid, token, &new_params[&track]);
        state.pumps.spawn(run_pump(
            ctx.broadcast.clone(),
            track,
            token,
            *ctx.pid,
            ctx.latency,
            cancel_rx,
        ));
    }

    state.params = new_params;
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

    let new_catalog = catalog_params(&snapshot);

    let pid_dead = |_| "consumer pid is dead".to_string();
    for (name, params) in old_catalog {
        if new_catalog.get(name) != Some(params) {
            messages::send_track_removed(ctx.pid, ctx.path, name).map_err(pid_dead)?;
        }
    }
    for (name, params) in &new_catalog {
        if old_catalog.get(name) != Some(params) {
            messages::send_track_added(ctx.pid, ctx.path, name, params).map_err(pid_dead)?;
        }
    }

    Ok(new_catalog)
}

async fn run_pump(
    broadcast: moq_net::BroadcastConsumer,
    track_name: String,
    token: Token,
    pid: LocalPid,
    latency: Duration,
    mut cancel: watch::Receiver<bool>,
) {
    let reason = tokio::select! {
        _ = cancel.changed() => return,
        result = pump_track(&broadcast, &track_name, token, &pid, latency) => match result {
            Ok(()) => "track ended".to_string(),
            Err(e) => format!("track error: {e}"),
        }
    };

    messages::send_track_ended(&pid, token, reason);
}

async fn pump_track(
    broadcast: &moq_net::BroadcastConsumer,
    track_name: &str,
    token: Token,
    pid: &LocalPid,
    latency: Duration,
) -> anyhow::Result<()> {
    let track_ref = moq_net::Track {
        name: track_name.to_string(),
        priority: 0,
    };
    let track_consumer = broadcast
        .subscribe_track(&track_ref)
        .map_err(|e| anyhow::anyhow!("subscribe_track({track_name}) failed: {e}"))?;

    let mut consumer =
        moq_mux::container::Consumer::new(track_consumer, moq_mux::container::legacy::Wire)
            .with_latency(latency);

    pump_frames(&mut consumer, token, pid).await
}

fn subscribe_catalog(
    broadcast: &moq_net::BroadcastConsumer,
) -> anyhow::Result<moq_mux::catalog::hang::Consumer<()>> {
    let catalog_track = broadcast
        .subscribe_track(&hang::Catalog::default_track())
        .map_err(|e| anyhow::anyhow!("subscribe_track(catalog) failed: {e}"))?;
    Ok(moq_mux::catalog::hang::Consumer::<()>::new(catalog_track))
}

fn catalog_params(snapshot: &moq_mux::catalog::hang::Catalog) -> CatalogSnapshot {
    let mut params = HashMap::new();
    for (name, config) in &snapshot.video.renditions {
        params.insert(name.clone(), video_params(config));
    }
    for (name, config) in &snapshot.audio.renditions {
        params.insert(name.clone(), audio_params(config));
    }
    params
}

async fn pump_frames(
    consumer: &mut moq_mux::container::Consumer<moq_mux::container::legacy::Wire>,
    token: Token,
    pid: &LocalPid,
) -> anyhow::Result<()> {
    while let Some(frame) = consumer.read().await? {
        let timestamp_us = frame.timestamp.as_micros() as i64;

        messages::send_frame(pid, token, &frame.payload, timestamp_us, frame.keyframe)
            .map_err(|_| anyhow::anyhow!("consumer pid is dead"))?;
    }
    Ok(())
}
