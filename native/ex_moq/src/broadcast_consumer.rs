mod subscription_queue;

use std::ops::ControlFlow;
use std::task::Poll;
use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};

use hang::moq_net;
use hang::moq_net::kio;
use moq_mux::catalog::Stream as _;

use crate::messages::{self, Token};
use crate::runtime;
use crate::session::Session;
use crate::track_format::WireContainer;

use subscription_queue::{PollEventResult, SubscriptionQueue};

type ExitReason = Option<String>;

struct CloseGuard {
    env: OwnedEnv,
    pid: LocalPid,
    path: String,
    reason: ExitReason,
    commands: kio::Queue<Command>,
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
        self.commands.close();

        let Some(reason) = self.reason.take() else {
            return;
        };
        let _ = messages::send_broadcast_closed(&mut self.env, self.pid, &self.path, reason);
    }
}

pub(crate) struct Closed;

enum Command {
    Subscribe {
        track: String,
        wire_container: WireContainer,
        token: Token,
        priority: u8,
    },
    Unsubscribe {
        token: Token,
    },
}

pub(crate) struct Handle {
    commands: kio::Queue<Command>,
    task: tokio::task::AbortHandle,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Handle {
    pub(crate) fn subscribe(
        &self,
        track: String,
        wire_container: WireContainer,
        token: Token,
        priority: u8,
    ) -> Result<(), Closed> {
        self.commands
            .try_push(Command::Subscribe {
                track,
                wire_container,
                token,
                priority,
            })
            .map_err(|_push_error| Closed)
    }

    pub(crate) fn unsubscribe(&self, token: Token) {
        let _ = self.commands.try_push(Command::Unsubscribe { token });
    }

    pub(crate) fn close(&self) {
        self.commands.close();
    }
}

pub(crate) fn spawn(session: &Session, path: String, pid: LocalPid, latency: Duration) -> Handle {
    let origin = session.subscribe.consume();
    let commands = kio::Queue::new();

    let task = runtime().spawn({
        let commands = commands.clone();
        async move {
            let mut guard = CloseGuard {
                env: OwnedEnv::new(),
                pid,
                path: path.clone(),
                reason: Some("consumer task died unexpectedly".to_string()),
                commands: commands.clone(),
            };

            guard.reason = Driver::run_broadcast(origin, path, pid, latency, commands).await;
        }
    });

    Handle {
        commands,
        task: task.abort_handle(),
    }
}

enum Event {
    Command(Result<Command, kio::Closed>),
    Catalog(Result<Option<moq_mux::catalog::hang::Catalog>, moq_mux::Error>),
    Track((Token, PollEventResult)),
}

struct Driver {
    env: OwnedEnv,
    pid: LocalPid,
    path: String,
    commands: kio::Queue<Command>,
    catalog: moq_mux::catalog::Consumer<()>,
    subs: SubscriptionQueue,
}

impl Driver {
    async fn run_broadcast(
        origin: moq_net::origin::Consumer,
        path: String,
        pid: LocalPid,
        latency: Duration,
        commands: kio::Queue<Command>,
    ) -> ExitReason {
        let Some(broadcast) = origin.announced_broadcast(&path).await else {
            return Some(format!(
                "broadcast {path:?} was not announced before the session closed"
            ));
        };

        let format = moq_mux::catalog::CatalogFormat::detect(&path)
            .unwrap_or(moq_mux::catalog::CatalogFormat::DEFAULT);
        let catalog = match moq_mux::catalog::Consumer::new(&broadcast, format).await {
            Ok(catalog) => catalog,
            Err(e) => return Some(e.to_string()),
        };

        let mut env = OwnedEnv::new();
        if messages::send_broadcast_ready(&mut env, pid, &path).is_err() {
            return None;
        }

        Self {
            env,
            pid,
            path,
            commands,
            catalog,
            subs: SubscriptionQueue::new(broadcast, latency),
        }
        .run()
        .await
    }

    async fn run(mut self) -> ExitReason {
        loop {
            let control_flow = match self.next_event().await {
                Event::Command(command) => self.handle_command(command),
                Event::Catalog(snapshot) => self.handle_catalog(snapshot),
                Event::Track(event) => self.handle_track(event),
            };

            if let ControlFlow::Break(reason) = control_flow {
                return reason;
            }

            // needed because kio primitives don't touch the budget.
            // without this line the loop might not yield to the scheduler,
            // starving other tasks.
            tokio::task::coop::consume_budget().await;
        }
    }

    async fn next_event(&mut self) -> Event {
        kio::wait(|waiter| {
            if let Poll::Ready(command) = self.commands.poll_pop(waiter) {
                return Poll::Ready(Event::Command(command));
            }
            if let Poll::Ready(snapshot) = self.catalog.poll_next(waiter) {
                return Poll::Ready(Event::Catalog(snapshot));
            }
            self.subs.poll_event(waiter).map(Event::Track)
        })
        .await
    }

    fn handle_command(&mut self, command: Result<Command, kio::Closed>) -> ControlFlow<ExitReason> {
        match command {
            Err(kio::Closed) => ControlFlow::Break(None),
            Ok(Command::Unsubscribe { token }) => {
                self.subs.remove(token);
                ControlFlow::Continue(())
            }
            Ok(Command::Subscribe {
                track,
                wire_container,
                token,
                priority,
            }) => {
                match self
                    .subs
                    .insert(token, track, wire_container, priority)
                    .or_else(|e| {
                        messages::send_track_error(&mut self.env, self.pid, token, e.to_string())
                    }) {
                    Ok(()) => ControlFlow::Continue(()),
                    Err(_pid_dead) => ControlFlow::Break(None),
                }
            }
        }
    }

    fn handle_catalog(
        &mut self,
        snapshot: Result<Option<moq_mux::catalog::hang::Catalog>, moq_mux::Error>,
    ) -> ControlFlow<ExitReason> {
        let close_reason = match snapshot {
            Ok(Some(snapshot)) => {
                match messages::send_catalog(&mut self.env, self.pid, &self.path, &snapshot) {
                    Ok(()) => return ControlFlow::Continue(()),
                    Err(messages::PidDead) => return ControlFlow::Break(None),
                }
            }
            Ok(None) => "broadcast ended".to_owned(),
            Err(e) => format!("catalog error: {e}"),
        };
        ControlFlow::Break(Some(close_reason))
    }

    fn handle_track(&mut self, event: (Token, PollEventResult)) -> ControlFlow<ExitReason> {
        let (token, result) = event;
        let send_result = match result {
            PollEventResult::Frame(frame) => {
                messages::send_frame(&mut self.env, self.pid, token, frame)
            }
            PollEventResult::TrackError(reason) => {
                messages::send_track_error(&mut self.env, self.pid, token, reason.to_string())
            }
            PollEventResult::TrackFinished => {
                messages::send_track_finished(&mut self.env, self.pid, token)
            }
        };
        match send_result {
            Ok(()) => ControlFlow::Continue(()),
            Err(messages::PidDead) => ControlFlow::Break(None),
        }
    }
}
