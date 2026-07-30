mod subscription_queue;

use std::ops::ControlFlow;
use std::time::Duration;

use rustler::{Atom, LocalPid, NifResult, OwnedEnv, Resource, ResourceArc};
use tokio::sync::mpsc;

use hang::moq_net;
use hang::moq_net::kio;
use moq_mux::catalog::Stream as _;

use crate::messages::{self, Token};
use crate::session::SessionResource;
use crate::track_format::Container;
use crate::{atoms, runtime};

use subscription_queue::{PollEventResult, SubscriptionQueue};

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
    Close,
}

pub(crate) struct BroadcastConsumerResource {
    commands: mpsc::UnboundedSender<Command>,
    task: tokio::task::AbortHandle,
}

impl Resource for BroadcastConsumerResource {}

impl Drop for BroadcastConsumerResource {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Exit {
    Closed(String),
    Detached,
}

struct CloseGuard {
    pid: LocalPid,
    path: String,
    reason: Option<String>,
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
        if let Some(reason) = self.reason.take() {
            let mut env = OwnedEnv::new();
            messages::send_broadcast_closed(&mut env, self.pid, &self.path, reason);
        }
    }
}

#[rustler::nif]
pub(crate) fn create_broadcast_consumer(
    session: ResourceArc<SessionResource>,
    path: String,
    pid: LocalPid,
    latency_ns: u64,
) -> (Atom, ResourceArc<BroadcastConsumerResource>) {
    let latency = Duration::from_nanos(latency_ns);

    let origin = session.consume.consume();

    let (commands_tx, commands_rx) = mpsc::unbounded_channel::<Command>();

    let task = runtime().spawn(run_broadcast(origin, path, pid, latency, commands_rx));

    (
        atoms::ok(),
        ResourceArc::new(BroadcastConsumerResource {
            commands: commands_tx,
            task: task.abort_handle(),
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
    let _ = consumer.commands.send(Command::Close);
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

    let mut guard = CloseGuard {
        pid,
        path,
        reason: Some("consumer task died unexpectedly".to_string()),
    };

    let Some(broadcast) = origin.announced_broadcast(&guard.path).await else {
        guard.reason = Some(format!(
            "broadcast {:?} was not announced before the session closed",
            guard.path
        ));
        return;
    };

    let mut catalog = match subscribe_catalog(&guard.path, &broadcast).await {
        Ok(catalog) => catalog,
        Err(e) => {
            guard.reason = Some(e.to_string());
            return;
        }
    };

    messages::send_broadcast_ready(&mut env, pid, &guard.path);

    let mut subs = SubscriptionQueue::new(broadcast, latency);

    let exit = loop {
        let result = tokio::select! {
          command = commands_rx.recv() => handle_command(&mut subs, command),
          snapshot = catalog.next() => handle_new_catalog(&mut env, pid, &guard.path, snapshot),
          event = kio::wait(|waiter| subs.poll_event(waiter)) => handle_event(&mut env, pid, event)
        };

        if let ControlFlow::Break(exit) = result {
            break exit;
        }

        tokio::task::coop::consume_budget().await;
    };

    guard.reason = match exit {
        Exit::Closed(reason) => Some(reason),
        Exit::Detached => None,
    };
}

fn handle_command(subs: &mut SubscriptionQueue, command: Option<Command>) -> ControlFlow<Exit> {
    match command {
        None | Some(Command::Close) => return ControlFlow::Break(Exit::Detached),
        Some(Command::Subscribe {
            track,
            wire,
            token,
            priority,
        }) => subs.insert(token, track, wire, priority),
        Some(Command::Unsubscribe { token }) => subs.remove(token),
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

fn handle_event(
    env: &mut OwnedEnv,
    pid: LocalPid,
    event: (Token, PollEventResult),
) -> ControlFlow<Exit> {
    let (token, result) = event;
    let send_result = match result {
        PollEventResult::Frame(frame) => messages::send_frame(env, pid, token, frame),
        PollEventResult::TrackError(reason) => {
            messages::send_track_error(env, pid, token, reason.to_string())
        }
        PollEventResult::TrackFinished => messages::send_track_finished(env, pid, token),
    };
    match send_result {
        Ok(()) => ControlFlow::Continue(()),
        Err(messages::PidDead) => ControlFlow::Break(Exit::Detached),
    }
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
