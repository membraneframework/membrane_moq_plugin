mod pump;
mod subscriptions;

use std::ops::ControlFlow;
use std::time::Duration;

use tokio::sync::mpsc;

use rustler::{Atom, LocalPid, NifResult, OwnedEnv, Resource, ResourceArc};

use hang::moq_net;
use moq_mux::catalog::Stream as _;

use crate::messages::{self, Token};
use crate::session::SessionResource;
use crate::track_format::Container;
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
    broadcast: &'a moq_net::broadcast::Consumer,
    latency: Duration,
    pid: LocalPid,
}

enum Exit {
    Closed(String),
    Detached,
}

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

#[rustler::nif]
pub(crate) fn subscribe_track(
    consumer: ResourceArc<BroadcastConsumerResource>,
    track: String,
    container: Option<Container>,
    token: Token,
    priority: u8,
) -> NifResult<Atom> {
    let container = container.ok_or_else(|| {
        crate::nif_error!("cannot subscribe to a track with an unrecognized wire container")
    })?;
    let catalog = hang::catalog::Container::from(container);

    let wire = moq_mux::catalog::hang::Container::try_from(&catalog)
        .map_err(|e| crate::nif_error!("container init failed: {e}"))?;

    let _ = consumer.commands.send(Command::Subscribe {
        track,
        wire,
        token,
        priority,
    });
    Ok(atoms::ok())
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
    consumer.abort.abort();
    atoms::ok()
}

async fn run_broadcast(
    origin: moq_net::origin::Consumer,
    path: String,
    pid: LocalPid,
    latency: Duration,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
) {
    let mut env = OwnedEnv::new();

    let Some(broadcast) = origin.announced_broadcast(&path).await else {
        messages::send_broadcast_closed(
            &mut env,
            pid,
            &path,
            format!("broadcast {path:?} was not announced before the session closed"),
        );
        return;
    };

    let mut catalog = match subscribe_catalog(&path, &broadcast).await {
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

    let exit = loop {
        let result = tokio::select! {
          command = commands_rx.recv() => handle_command(&mut subs, &ctx, command),
          Some((token, outcome)) = subs.join_next(),
            if subs.has_pumps() => handle_pump_join(&mut env, pid, token, outcome),
          snapshot = catalog.next() => handle_new_catalog(&mut env, pid, &path, snapshot)
        };

        if let ControlFlow::Break(exit) = result {
            break exit;
        }
    };

    if let Exit::Closed(reason) = exit {
        messages::send_broadcast_closed(&mut env, pid, &path, reason);
    }
}

fn handle_command(
    subs: &mut Subscriptions,
    ctx: &Ctx,
    command: Option<Command>,
) -> ControlFlow<Exit> {
    match command {
        None => return ControlFlow::Break(Exit::Detached),
        Some(Command::Subscribe {
            track,
            wire,
            token,
            priority,
        }) => {
            let pump = Pump::new(ctx, track, wire, token, priority);
            subs.insert(token, pump);
        }
        Some(Command::Unsubscribe { token }) => subs.remove(token),
    }
    ControlFlow::Continue(())
}

fn handle_pump_join(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
    outcome: anyhow::Result<()>,
) -> ControlFlow<Exit> {
    match outcome {
        Ok(()) => messages::send_track_ended(env, pid, token),
        Err(e) => messages::send_track_error(env, pid, token, e.to_string()),
    }
    ControlFlow::Continue(())
}

fn handle_new_catalog(
    env: &mut OwnedEnv,
    pid: LocalPid,
    path: &str,
    snapshot: Result<Option<moq_mux::catalog::hang::Catalog>, moq_mux::Error>,
) -> ControlFlow<Exit> {
    let close_reason = match snapshot {
        Ok(Some(snapshot)) => match messages::send_catalog(env, pid, path, &snapshot) {
            Ok(()) => return ControlFlow::Continue(()),
            Err(messages::PidDead) => "consumer pid is dead".to_string(),
        },
        Ok(None) => "broadcast ended".to_string(),
        Err(e) => format!("catalog error: {e}"),
    };
    ControlFlow::Break(Exit::Closed(close_reason))
}

async fn subscribe_catalog(
    path: &str,
    broadcast: &moq_net::broadcast::Consumer,
) -> anyhow::Result<moq_mux::catalog::Consumer<()>> {
    let format = moq_mux::catalog::CatalogFormat::detect(path)
        .unwrap_or(moq_mux::catalog::CatalogFormat::DEFAULT);
    moq_mux::catalog::Consumer::<()>::new(broadcast, format)
        .await
        .map_err(|e| anyhow::anyhow!("catalog subscribe ({format:?}) failed: {e}"))
}
