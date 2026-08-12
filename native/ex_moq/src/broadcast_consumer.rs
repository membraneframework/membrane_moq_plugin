mod subscription_queue;

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::task::Poll;
use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};

use hang::moq_net;
use hang::moq_net::kio;
use moq_mux::catalog::Stream as _;

use crate::messages::{self, Token};
use crate::runtime;
use crate::track_format::{CatalogContainer, WireContainer};

use subscription_queue::{SubscriptionQueue, TrackResult};

#[derive(rustler::NifTaggedEnum)]
pub(crate) enum CloseReason {
    Ended,
    NotAnnounced,
    Crashed,
    CatalogError(String),
}

#[derive(Debug, thiserror::Error)]
enum TrackErrorKind {
    #[error("not advertised in the catalog")]
    NotAdvertised,
    #[error(transparent)]
    Container(moq_mux::Error),
    #[error("subscribe failed: {0}")]
    SubscribeFailed(#[from] moq_net::Error),
    #[error("track read failed: {0}")]
    ReadFailed(#[from] moq_mux::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("track({track}): {source}")]
struct TrackError {
    track: String,
    source: TrackErrorKind,
}

struct CloseGuard {
    env: OwnedEnv,
    pid: LocalPid,
    path: String,
    reason: Option<CloseReason>,
    commands: kio::Queue<Command>,
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
        self.commands.close();

        if let Some(reason) = self.reason.take() {
            let _ = messages::send_broadcast_closed(&mut self.env, self.pid, &self.path, reason);
        };
    }
}

pub(crate) struct Closed;

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
        token: Token,
        priority: u8,
    ) -> Result<(), Closed> {
        self.commands
            .try_push(Command::Subscribe {
                track,
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

pub(crate) fn spawn(
    session: &crate::session::Handle,
    path: String,
    pid: LocalPid,
    latency: Duration,
) -> Handle {
    let origin = session.subscribe.consume();
    let commands = kio::Queue::new();

    let task = runtime().spawn({
        let commands = commands.clone();
        async move {
            let mut guard = CloseGuard {
                env: OwnedEnv::new(),
                pid,
                path: path.clone(),
                // default exit reason that gets reported on Driver panic
                reason: Some(CloseReason::Crashed),
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
    Track(Token, TrackResult),
}

struct Driver {
    env: OwnedEnv,
    pid: LocalPid,
    path: String,
    commands: kio::Queue<Command>,
    catalog: moq_mux::catalog::Consumer<()>,
    containers: HashMap<String, CatalogContainer>,
    subs: SubscriptionQueue,
}

impl Driver {
    async fn run_broadcast(
        origin: moq_net::origin::Consumer,
        path: String,
        pid: LocalPid,
        latency: Duration,
        commands: kio::Queue<Command>,
    ) -> Option<CloseReason> {
        let Some(broadcast) = origin.announced_broadcast(&path).await else {
            return Some(CloseReason::NotAnnounced);
        };

        let format = moq_mux::catalog::CatalogFormat::detect(&path)
            .unwrap_or(moq_mux::catalog::CatalogFormat::DEFAULT);
        let catalog = match moq_mux::catalog::Consumer::new(&broadcast, format).await {
            Ok(catalog) => catalog,
            Err(e) => return Some(CloseReason::CatalogError(e.to_string())),
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
            containers: HashMap::new(),
            subs: SubscriptionQueue::new(broadcast, latency),
        }
        .run()
        .await
    }

    async fn run(mut self) -> Option<CloseReason> {
        loop {
            let control_flow = match self.next_event().await {
                Event::Command(command) => self.handle_command(command),
                Event::Catalog(snapshot) => self.handle_catalog(snapshot),
                Event::Track(token, result) => self.handle_track(token, result),
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
            self.subs
                .poll_event(waiter)
                .map(|(token, result)| Event::Track(token, result))
        })
        .await
    }

    fn handle_command(
        &mut self,
        command: Result<Command, kio::Closed>,
    ) -> ControlFlow<Option<CloseReason>> {
        match command {
            Err(kio::Closed) => ControlFlow::Break(None),
            Ok(Command::Unsubscribe { token }) => {
                self.subs.remove(token);
                ControlFlow::Continue(())
            }
            Ok(Command::Subscribe {
                track,
                token,
                priority,
            }) => {
                let result = match self.get_container(&track) {
                    Ok(container) => self.subs.insert(token, track, container, priority),
                    Err(source) => Err(TrackError { track, source }),
                };

                match result.or_else(|e| {
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
    ) -> ControlFlow<Option<CloseReason>> {
        let close_reason = match snapshot {
            Ok(Some(snapshot)) => {
                self.containers = advertised_containers(&snapshot);

                match messages::send_catalog(&mut self.env, self.pid, &self.path, &snapshot) {
                    Ok(()) => return ControlFlow::Continue(()),
                    Err(messages::PidDead) => return ControlFlow::Break(None),
                }
            }
            Ok(None) => CloseReason::Ended,
            Err(e) => CloseReason::CatalogError(e.to_string()),
        };
        ControlFlow::Break(Some(close_reason))
    }

    fn handle_track(
        &mut self,
        token: Token,
        result: TrackResult,
    ) -> ControlFlow<Option<CloseReason>> {
        let send_result = match result {
            TrackResult::Frame(frame) => {
                messages::send_frame(&mut self.env, self.pid, token, frame)
            }
            TrackResult::Err(reason) => {
                messages::send_track_error(&mut self.env, self.pid, token, reason.to_string())
            }
            TrackResult::Finished => messages::send_track_finished(&mut self.env, self.pid, token),
        };
        match send_result {
            Ok(()) => ControlFlow::Continue(()),
            Err(messages::PidDead) => ControlFlow::Break(None),
        }
    }

    fn get_container(&self, track: &str) -> Result<WireContainer, TrackErrorKind> {
        self.containers
            .get(track)
            .ok_or(TrackErrorKind::NotAdvertised)?
            .try_into()
            .map_err(TrackErrorKind::Container)
    }
}

fn advertised_containers(
    catalog: &moq_mux::catalog::hang::Catalog,
) -> HashMap<String, CatalogContainer> {
    let videos = catalog
        .video
        .renditions
        .iter()
        .map(|(name, config)| (name.clone(), config.container.clone()));

    let audios = catalog
        .audio
        .renditions
        .iter()
        .map(|(name, config)| (name.clone(), config.container.clone()));

    videos.chain(audios).collect()
}
