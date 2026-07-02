use std::collections::HashMap;
use std::time::Duration;

use url::Url;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use rustler::{Atom, LocalPid, NifResult, Resource, ResourceArc};

use hang::moq_net;
use moq_native::ClientConfig;

use crate::messages::{self, Token};
use crate::track_format::{audio_params, video_params, TrackParams};
use crate::{atoms, runtime};

enum Command {
    Subscribe { track: String, token: Token },
    Unsubscribe { token: Token },
}

pub(crate) struct SubscriberResource {
    commands: mpsc::UnboundedSender<Command>,
    shutdown: mpsc::UnboundedSender<()>,
}

impl Resource for SubscriberResource {}

type CatalogSnapshot = HashMap<String, TrackParams>;

/// Mutable state the session loop threads through each iteration by value, so
/// handlers own it rather than reaching into scattered `&mut` locals.
struct LoopState {
    /// Latest codec params per track name, from the most recent catalog snapshot.
    params: CatalogSnapshot,
    pumps: JoinSet<()>,
    cancels: HashMap<Token, watch::Sender<bool>>,
    /// Subscribe commands whose track has not yet appeared in the catalog. Held
    /// here (with their cancel receiver) until a snapshot advertises the track.
    pending: HashMap<Token, (String, watch::Receiver<bool>)>,
}

/// Immutable context shared by every loop iteration.
struct Ctx<'a> {
    broadcast: &'a moq_net::BroadcastConsumer,
    latency: Duration,
    pid: &'a LocalPid,
}

#[rustler::nif]
pub(crate) fn start_subscriber(
    url: String,
    broadcast: String,
    pid: LocalPid,
    disable_tls_verify: bool,
    latency_ns: u64,
) -> NifResult<(Atom, ResourceArc<SubscriberResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;
    let latency = Duration::from_nanos(latency_ns);

    let (commands_tx, commands_rx) = mpsc::unbounded_channel::<Command>();
    let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let config = crate::session::client_config(disable_tls_verify);

        let result = run_session(
            url,
            broadcast,
            &pid,
            config,
            latency,
            commands_rx,
            shutdown_rx,
        )
        .await;

        if let Err(e) = result {
            messages::send_setup_failed(&pid, e.to_string());
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SubscriberResource {
            commands: commands_tx,
            shutdown: shutdown_tx,
        }),
    ))
}

#[rustler::nif]
pub(crate) fn subscribe_track(
    subscriber: ResourceArc<SubscriberResource>,
    track: String,
    token: Token,
) -> Atom {
    let _ = subscriber
        .commands
        .send(Command::Subscribe { track, token });
    atoms::ok()
}

#[rustler::nif]
pub(crate) fn unsubscribe_track(subscriber: ResourceArc<SubscriberResource>, token: Token) -> Atom {
    let _ = subscriber.commands.send(Command::Unsubscribe { token });
    atoms::ok()
}

#[rustler::nif]
pub(crate) fn stop_subscriber(subscriber: ResourceArc<SubscriberResource>) -> Atom {
    let _ = subscriber.shutdown.send(());
    atoms::ok()
}

async fn run_session(
    url: Url,
    broadcast_name: String,
    pid: &LocalPid,
    config: ClientConfig,
    latency: Duration,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
    mut shutdown_rx: mpsc::UnboundedReceiver<()>,
) -> anyhow::Result<()> {
    let origin = moq_net::Origin::random().produce();
    let mut origin_consumer = origin.consume();

    let client = config.init()?.with_consume(origin);
    let session = client.connect(url).await?;

    let broadcast = tokio::select! {
        broadcast = await_broadcast(&mut origin_consumer, &broadcast_name) => {
            broadcast.ok_or_else(|| anyhow::anyhow!(
                "broadcast {broadcast_name:?} was not announced before origin closed"
            ))?
        }
        _ = shutdown_rx.recv() => return Ok(()),
    };

    messages::send_connected(pid);

    let mut catalog = subscribe_catalog(&broadcast)?;

    let ctx = Ctx {
        broadcast: &broadcast,
        latency,
        pid,
    };

    let mut state = LoopState {
        params: HashMap::new(),
        pumps: JoinSet::new(),
        cancels: HashMap::new(),
        pending: HashMap::new(),
    };

    let disconnect_reason = loop {
        tokio::select! {
            command = commands_rx.recv() => match command {
                Some(command) => state = handle_command(command, state, &ctx),
                // The resource was dropped without a stop; treat as shutdown.
                None => return Ok(()),
            },
            _ = shutdown_rx.recv() => return Ok(()),
            _ = state.pumps.join_next(), if !state.pumps.is_empty() => {}
            snapshot = catalog.next() => {
                state = match handle_new_catalog(snapshot, state, &ctx) {
                    Ok(state) => state,
                    Err(reason) => break reason,
                };
            }
            result = session.closed() => break match result {
                Ok(()) => "session closed gracefully".to_string(),
                Err(e) => format!("session error: {e}"),
            },
        }
    };

    messages::send_disconnected(pid, disconnect_reason);
    Ok(())
}

fn handle_command(command: Command, mut state: LoopState, ctx: &Ctx) -> LoopState {
    match command {
        Command::Subscribe { track, token } => {
            let (cancel_tx, cancel_rx) = watch::channel(false);
            state.cancels.insert(token, cancel_tx);
            // Only subscribe to the track once the catalog advertises it:
            // publishers create tracks lazily on the first stream_format, so
            // subscribing earlier races the announcement and the relay answers
            // NotFound. If we already have params, start now; otherwise park the
            // command until a snapshot resolves it.
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
    mut state: LoopState,
    ctx: &Ctx,
) -> Result<LoopState, String> {
    let new_params = update_catalog(ctx.pid, snapshot, &state.params)?;

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

/// Diff the incoming catalog snapshot against the previous one, notifying Elixir
/// of added/removed tracks, and return the new per-track params. Errors carry a
/// disconnect reason that ends the session loop.
fn update_catalog(
    pid: &LocalPid,
    snapshot: Result<Option<moq_mux::catalog::hang::Catalog<()>>, moq_mux::Error>,
    old_catalog: &CatalogSnapshot,
) -> Result<CatalogSnapshot, String> {
    let snapshot = match snapshot {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => return Err("broadcast ended".to_string()),
        Err(e) => return Err(format!("catalog error: {e}")),
    };

    let new_catalog = catalog_params(&snapshot);

    let pid_dead = || "subscriber pid is dead".to_string();
    for (name, params) in old_catalog {
        if new_catalog.get(name) != Some(params) {
            messages::send_track_removed(pid, name).map_err(|_| pid_dead())?;
        }
    }
    for (name, params) in &new_catalog {
        if old_catalog.get(name) != Some(params) {
            messages::send_track_added(pid, name, params).map_err(|_| pid_dead())?;
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
    // The caller only spawns a pump once the catalog advertises this track, so
    // the subscribe below no longer races the announcement, and the stream
    // format has already been sent to Elixir from the catalog params.
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

async fn await_broadcast(
    consumer: &mut moq_net::OriginConsumer,
    name: &str,
) -> Option<moq_net::BroadcastConsumer> {
    while let Some((path, broadcast)) = consumer.announced().await {
        if path.as_str() == name {
            if let Some(broadcast) = broadcast {
                return Some(broadcast);
            }
        }
    }
    None
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
        // u128 → i64: real-world frame timestamps fit. If a stream somehow
        // overflows i64 microseconds (~292,000 years), the cast wraps and the
        // Elixir side sees a meaningless value, but no UB.
        let timestamp_us = frame.timestamp.as_micros() as i64;

        messages::send_frame(pid, token, &frame.payload, timestamp_us, frame.keyframe)
            .map_err(|_| anyhow::anyhow!("subscriber pid is dead"))?;
    }
    Ok(())
}
