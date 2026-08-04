mod subscription_queue;

use std::ops::ControlFlow;
use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};
use tokio::sync::mpsc;

use hang::moq_net;
use hang::moq_net::kio;
use moq_mux::catalog::Stream as _;

use crate::messages::{self, Token};
use crate::runtime;
use crate::session::Session;

use subscription_queue::{PollEventResult, SubscriptionQueue};

enum Command {
    Subscribe {
        track: String,
        wire_container: moq_mux::catalog::hang::Container,
        token: Token,
        priority: u8,
    },
    Unsubscribe {
        token: Token,
    },
    Close,
}

pub(crate) struct Closed;

pub(crate) struct Consumer {
    commands: mpsc::UnboundedSender<Command>,
    task: tokio::task::AbortHandle,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Consumer {
    pub(crate) fn spawn(session: &Session, path: String, pid: LocalPid, latency: Duration) -> Self {
        let origin = session.consume.consume();
        let (commands_tx, commands_rx) = mpsc::unbounded_channel::<Command>();

        let task = runtime().spawn(async move {
            let mut guard = CloseGuard {
                env: OwnedEnv::new(),
                pid,
                path,
                reason: Some("consumer task died unexpectedly".to_string()),
            };

            guard.reason = run_broadcast(
                origin,
                &guard.path,
                &mut guard.env,
                pid,
                latency,
                commands_rx,
            )
            .await;
        });

        Self {
            commands: commands_tx,
            task: task.abort_handle(),
        }
    }

    pub(crate) fn subscribe(
        &self,
        track: String,
        wire_container: moq_mux::catalog::hang::Container,
        token: Token,
        priority: u8,
    ) -> Result<(), Closed> {
        self.commands
            .send(Command::Subscribe {
                track,
                wire_container,
                token,
                priority,
            })
            .map_err(|_send_error| Closed)
    }

    pub(crate) fn unsubscribe(&self, token: Token) {
        let _ = self.commands.send(Command::Unsubscribe { token });
    }

    pub(crate) fn close(&self) {
        let _ = self.commands.send(Command::Close);
    }
}

type ExitReason = Option<String>;

struct CloseGuard {
    env: OwnedEnv,
    pid: LocalPid,
    path: String,
    reason: ExitReason,
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
        let Some(reason) = self.reason.take() else {
            return;
        };
        let _ = messages::send_broadcast_closed(&mut self.env, self.pid, &self.path, reason);
    }
}

async fn run_broadcast(
    origin: moq_net::origin::Consumer,
    path: &str,
    env: &mut OwnedEnv,
    pid: LocalPid,
    latency: Duration,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
) -> ExitReason {
    let Some(broadcast) = origin.announced_broadcast(path).await else {
        return Some(format!(
            "broadcast {path:?} was not announced before the session closed"
        ));
    };

    let mut catalog = match subscribe_catalog(path, &broadcast).await {
        Ok(catalog) => catalog,
        Err(e) => return Some(e.to_string()),
    };

    if messages::send_broadcast_ready(env, pid, path).is_err() {
        return None;
    }

    let mut subs = SubscriptionQueue::new(broadcast, latency);

    loop {
        let result = tokio::select! {
          command = commands_rx.recv() => handle_command(env, pid, &mut subs, command),
          snapshot = catalog.next() => handle_new_catalog(env, pid, path, snapshot),
          event = kio::wait(|waiter| subs.poll_event(waiter)) => handle_event(env, pid, event)
        };

        if let ControlFlow::Break(exit_reason) = result {
            return exit_reason;
        }

        tokio::task::coop::consume_budget().await;
    }
}

fn handle_command(
    env: &mut OwnedEnv,
    pid: LocalPid,
    subs: &mut SubscriptionQueue,
    command: Option<Command>,
) -> ControlFlow<ExitReason> {
    match command {
        None | Some(Command::Close) => ControlFlow::Break(None),
        Some(Command::Unsubscribe { token }) => {
            subs.remove(token);
            ControlFlow::Continue(())
        }
        Some(Command::Subscribe {
            track,
            wire_container,
            token,
            priority,
        }) => {
            match subs
                .insert(token, track, wire_container, priority)
                .or_else(|e| messages::send_track_error(env, pid, token, e.to_string()))
            {
                Ok(()) => ControlFlow::Continue(()),
                Err(_pid_dead) => ControlFlow::Break(None),
            }
        }
    }
}

fn handle_new_catalog(
    env: &mut OwnedEnv,
    pid: LocalPid,
    path: &str,
    snapshot: Result<Option<moq_mux::catalog::hang::Catalog>, moq_mux::Error>,
) -> ControlFlow<ExitReason> {
    let close_reason = match snapshot {
        Ok(Some(snapshot)) => match messages::send_catalog(env, pid, path, &snapshot) {
            Ok(()) => return ControlFlow::Continue(()),
            Err(messages::PidDead) => return ControlFlow::Break(None),
        },
        Ok(None) => "broadcast ended".to_owned(),
        Err(e) => format!("catalog error: {e}"),
    };
    ControlFlow::Break(Some(close_reason))
}

fn handle_event(
    env: &mut OwnedEnv,
    pid: LocalPid,
    event: (Token, PollEventResult),
) -> ControlFlow<ExitReason> {
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
        Err(messages::PidDead) => ControlFlow::Break(None),
    }
}

async fn subscribe_catalog(
    path: &str,
    broadcast: &moq_net::broadcast::Consumer,
) -> Result<moq_mux::catalog::Consumer<()>, moq_mux::Error> {
    let format = moq_mux::catalog::CatalogFormat::detect(path)
        .unwrap_or(moq_mux::catalog::CatalogFormat::DEFAULT);
    moq_mux::catalog::Consumer::new(broadcast, format).await
}
