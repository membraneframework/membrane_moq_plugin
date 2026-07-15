mod pump;
mod subscriptions;

use std::time::Duration;

use tokio::sync::mpsc;

use rustler::{Atom, LocalPid, NifResult, OwnedEnv, Resource, ResourceArc};

use hang::moq_net;
use moq_mux::catalog::Stream as _;

use crate::messages::{self, Token};
use crate::session::SessionResource;
use crate::track_format::WireContainer;
use crate::{atoms, lock_ignoring_poison, runtime};

use pump::Pump;
use subscriptions::Subscriptions;

enum Command {
    Subscribe {
        track: String,
        wire: moq_mux::catalog::hang::Container,
        token: Token,
        priority: u8,
    },
    Unsubscribe {
        token: Token,
    },
}

pub(crate) struct BroadcastConsumerResource {
    commands: mpsc::UnboundedSender<Command>,
    /// Aborting the task drops its `Subscriptions`, which aborts every pump.
    abort: tokio::task::AbortHandle,
}

impl Resource for BroadcastConsumerResource {}

impl Drop for BroadcastConsumerResource {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

struct Ctx<'a> {
    broadcast: &'a moq_net::BroadcastConsumer,
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

    let origin = lock_ignoring_poison(&session.consume).consume();

    let (commands_tx, commands_rx) = mpsc::unbounded_channel::<Command>();

    let task = runtime().spawn(run_broadcast(origin, path, pid, latency, commands_rx));

    (
        atoms::ok(),
        ResourceArc::new(BroadcastConsumerResource {
            commands: commands_tx,
            abort: task.abort_handle(),
        }),
    )
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn subscribe_track(
    consumer: ResourceArc<BroadcastConsumerResource>,
    track: String,
    container: WireContainer,
    token: Token,
    priority: u8,
) -> NifResult<Atom> {
    let wire = container.resolve().ok_or_else(|| {
        crate::nif_error!("cannot subscribe to a track with an unrecognized wire container")
    })?;

    let _ = consumer.commands.send(Command::Subscribe {
        track,
        wire,
        token,
        priority,
    });
    Ok(atoms::ok())
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
    consumer.abort.abort();
    atoms::ok()
}

async fn run_broadcast(
    origin: moq_net::OriginConsumer,
    path: String,
    pid: LocalPid,
    latency: Duration,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
) {
    let mut env = OwnedEnv::new();

    let Some(broadcast) = origin.announced_broadcast(path.as_str()).await else {
        messages::send_broadcast_closed(
            &mut env,
            pid,
            &path,
            format!("broadcast {path:?} was not announced before the session closed"),
        );
        return;
    };

    let mut catalog = match subscribe_catalog(&path, &broadcast) {
        Ok(catalog) => catalog,
        Err(e) => {
            messages::send_broadcast_closed(&mut env, pid, &path, e.to_string());
            return;
        }
    };

    messages::send_broadcast_ready(&mut env, pid, &path);

    let ctx = Ctx {
        broadcast: &broadcast,
        latency,
        pid,
    };

    let mut subs = Subscriptions::new();

    loop {
        tokio::select! {
            command = commands_rx.recv() => match command {
                Some(Command::Subscribe { track, wire, token, priority }) => {
                    let pump = Pump::new(&ctx, track, wire, token, priority);
                    subs.insert(token, pump);
                }
                Some(Command::Unsubscribe { token }) => subs.remove(token),
                None => return,
            },
            Some((token, outcome)) = subs.join_next(),
              if subs.has_pumps() => match outcome {
                Ok(()) => messages::send_track_ended(&mut env, pid, token),
                Err(e) => messages::send_track_error(&mut env, pid, token, e.to_string()),
            },
            snapshot = catalog.next() => {
                let close_reason = match snapshot {
                    Ok(Some(snapshot)) => {
                        match messages::send_catalog(&mut env, pid, &path, &snapshot) {
                            Ok(()) => continue,
                            Err(messages::PidDead) => "consumer pid is dead".to_string(),
                        }
                    }
                    Ok(None) => "broadcast ended".to_string(),
                    Err(e) => format!("catalog error: {e}"),
                };
                messages::send_broadcast_closed(&mut env, pid, &path, close_reason);
                return;
            }
        }
    }
}

fn subscribe_catalog(
    path: &str,
    broadcast: &moq_net::BroadcastConsumer,
) -> anyhow::Result<moq_mux::catalog::Consumer<()>> {
    let format = moq_mux::catalog::CatalogFormat::detect(path)
        .unwrap_or(moq_mux::catalog::CatalogFormat::DEFAULT);
    moq_mux::catalog::Consumer::<()>::new(broadcast, format)
        .map_err(|e| anyhow::anyhow!("catalog subscribe ({format:?}) failed: {e}"))
}
