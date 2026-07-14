mod pump;
mod subscriptions;

use std::collections::HashMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinError;

use rustler::{Atom, LocalPid, OwnedEnv, Resource, ResourceArc};

use hang::moq_net;

use crate::messages::{self, Token};
use crate::session::SessionResource;
use crate::track_format::{audio_params, video_params, TrackParams};
use crate::{atoms, lock_ignoring_poison, runtime};

use pump::Pump;
use subscriptions::Subscriptions;

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
    subs: Subscriptions,
    env: OwnedEnv,
}

struct Ctx<'a> {
    broadcast: &'a moq_net::BroadcastConsumer,
    path: &'a str,
    latency: Duration,
    pid: LocalPid,
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn create_broadcast_consumer(
    session: ResourceArc<SessionResource>,
    path: String,
    pid: LocalPid,
    latency_ns: u64,
) -> (Atom, ResourceArc<BroadcastConsumerResource>) {
    let latency = Duration::from_nanos(latency_ns);

    // A clone with its own announcement cursor, so each broadcast consumer
    // awaits its broadcast independently of any sibling consumers.
    let origin = lock_ignoring_poison(&session.consume).consume();

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

    (
        atoms::ok(),
        ResourceArc::new(BroadcastConsumerResource {
            commands: commands_tx,
            shutdown: shutdown_tx,
        }),
    )
}

#[allow(clippy::needless_pass_by_value)]
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

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn unsubscribe_track(
    consumer: ResourceArc<BroadcastConsumerResource>,
    token: Token,
) -> Atom {
    let _ = consumer.commands.send(Command::Unsubscribe { token });
    atoms::ok()
}

#[allow(clippy::needless_pass_by_value)]
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
    let mut env = OwnedEnv::new();

    let broadcast = tokio::select! {
        broadcast = origin.announced_broadcast(path.as_str()) =>
          if let Some(broadcast) = broadcast {
            broadcast
          } else {
            messages::send_broadcast_closed(&mut env, pid,
                &path,
                format!("broadcast {path:?} was not announced before the session closed"),
            );
            return;
          },
        _ = shutdown_rx.recv() => return,
    };

    let mut catalog = match subscribe_catalog(&broadcast) {
        Ok(catalog) => catalog,
        Err(e) => {
            messages::send_broadcast_closed(&mut env, pid, &path, e.to_string());
            return;
        }
    };

    messages::send_broadcast_ready(&mut env, pid, &path);

    let ctx = Ctx {
        broadcast: &broadcast,
        path: &path,
        latency,
        pid,
    };

    let mut state = ConsumerState {
        renditions: HashMap::new(),
        subs: Subscriptions::new(),
        env,
    };

    loop {
        tokio::select! {
            command = commands_rx.recv() => match command {
                Some(command) => state = handle_command(command, state, &ctx),
                // The resource was dropped without a close; treat as shutdown.
                None => return,
            },
            _ = shutdown_rx.recv() => return,
            Some(result) = state.subs.join_next(),
              if state.subs.has_pumps() => state = handle_pump_join(result, state, ctx.pid),
            snapshot = catalog.next() => {
                state = match handle_new_catalog(snapshot, state, &ctx) {
                    Ok(state) => state,
                    Err((mut env, reason)) => {
                        messages::send_broadcast_closed(&mut env, pid, &path, reason);
                        return;
                    }
                };
            }
        }
    }
}

fn handle_command(command: Command, mut state: ConsumerState, ctx: &Ctx) -> ConsumerState {
    match command {
        Command::Subscribe {
            track,
            token,
            priority,
        } => match state.renditions.get(&track) {
            // track is already announced
            Some(rendition) => {
                messages::send_track_format(&mut state.env, ctx.pid, token, &rendition.params);
                let pump = Pump::new(ctx, track.clone(), rendition, token, priority);
                state.subs.insert_active(token, track, priority, pump);
            }
            // track is not announced yet
            None => state.subs.insert_pending(token, track, priority),
        },

        Command::Unsubscribe { token } => state.subs.remove(token),
    }
    state
}

fn handle_pump_join(
    result: Result<(tokio::task::Id, ()), JoinError>,
    mut state: ConsumerState,
    pid: LocalPid,
) -> ConsumerState {
    let id = match &result {
        Ok((id, ())) => *id,
        Err(e) => e.id(),
    };

    // send a :track_error on pumps that terminated abnormally.
    // the guard works because:
    //   - for tasks terminating normally, `result != Err(_)`
    //   - for tasks removed via `remove`, `state.subs.reap(id) == None`
    if let Some((token, e)) = state.subs.reap(id).zip(result.err()) {
        messages::send_track_error(&mut state.env, pid, token, format!("pump task died: {e}"));
    }

    state
}

fn handle_new_catalog(
    snapshot: Result<Option<moq_mux::catalog::hang::Catalog<()>>, moq_mux::Error>,
    mut state: ConsumerState,
    ctx: &Ctx,
) -> Result<ConsumerState, (OwnedEnv, String)> {
    let new_renditions = match update_catalog(ctx, snapshot, &state.renditions, &mut state.env) {
        Ok(new_renditions) => new_renditions,
        Err(e) => return Err((state.env, e)),
    };

    // Cancel track pumps whose parameters changed in-place
    // The parent sees the fresh parameters with a new :track_added message,
    // and should resub to it
    let old_renditions = &state.renditions;
    let changed = state.subs.remove_active_where(|track| {
        let old = old_renditions.get(track);
        old.is_some()
            && new_renditions
                .get(track)
                .is_some_and(|new| Some(new) != old)
    });

    for token in changed {
        messages::send_track_ended(
            &mut state.env,
            ctx.pid,
            token,
            "rendition changed".to_string(),
        );
    }

    // Start pumps for any pending subscriptions the snapshot just resolved.
    for (token, track, priority, rendition) in state.subs.drain_resolved(&new_renditions) {
        messages::send_track_format(&mut state.env, ctx.pid, token, &rendition.params);
        let pump = Pump::new(ctx, track.clone(), rendition, token, priority);
        state.subs.insert_active(token, track, priority, pump);
    }

    state.renditions = new_renditions;
    Ok(state)
}

fn update_catalog(
    ctx: &Ctx,
    snapshot: Result<Option<moq_mux::catalog::hang::Catalog<()>>, moq_mux::Error>,
    old_catalog: &CatalogSnapshot,
    env: &mut OwnedEnv,
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
            messages::send_track_removed(env, ctx.pid, ctx.path, name).map_err(pid_dead)?;
        }
    }

    for (name, rendition) in &new_catalog {
        if old_catalog.get(name) != Some(rendition) {
            messages::send_track_added(env, ctx.pid, ctx.path, name, &rendition.params)
                .map_err(pid_dead)?;
        }
    }

    Ok(new_catalog)
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
