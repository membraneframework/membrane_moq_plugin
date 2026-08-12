mod subscriptions;

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};

use hang::moq_net;
use moq_mux::catalog::Stream as _;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::messages::{self, Token};
use crate::runtime;
use crate::track_format::{CatalogContainer, WireContainer};

use subscriptions::Subscriptions;

#[derive(rustler::NifTaggedEnum)]
pub(crate) enum CloseReason {
    Ended,
    NotAnnounced,
    Crashed,
    CatalogError(String),
}

#[derive(Debug, thiserror::Error)]
enum TrackError {
    #[error("not advertised in the catalog")]
    NotAdvertised,
    #[error(transparent)]
    Container(moq_mux::Error),
    #[error("subscribe failed: {0}")]
    SubscribeFailed(#[from] moq_net::Error),
    #[error("track read failed: {0}")]
    ReadFailed(#[from] moq_mux::Error),
    #[error(transparent)]
    Panicked(tokio::task::JoinError),
}

struct CloseGuard {
    env: OwnedEnv,
    pid: LocalPid,
    path: String,
    reason: Option<CloseReason>,
}

impl Drop for CloseGuard {
    fn drop(&mut self) {
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
    Close,
}

pub(crate) struct Handle {
    commands: mpsc::UnboundedSender<Command>,
    task: AbortHandle,
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
            .send(Command::Subscribe {
                track,
                token,
                priority,
            })
            .map_err(|_push_error| Closed)
    }

    pub(crate) fn unsubscribe(&self, token: Token) {
        let _ = self.commands.send(Command::Unsubscribe { token });
    }

    pub(crate) fn close(&self) {
        let _ = self.commands.send(Command::Close);
    }
}

pub(crate) fn spawn(
    session: &crate::session::Handle,
    path: String,
    pid: LocalPid,
    latency: Duration,
) -> Handle {
    let origin = session.subscribe.consume();
    let (commands_tx, commands_rx) = mpsc::unbounded_channel();

    let task = runtime().spawn({
        async move {
            let mut guard = CloseGuard {
                env: OwnedEnv::new(),
                pid,
                path: path.clone(),
                // default exit reason that gets reported on Driver panic
                reason: Some(CloseReason::Crashed),
            };

            guard.reason = Driver::run_broadcast(origin, path, pid, latency, commands_rx).await;
        }
    });

    Handle {
        commands: commands_tx,
        task: task.abort_handle(),
    }
}

struct Driver {
    env: OwnedEnv,
    pid: LocalPid,
    path: String,
    commands: mpsc::UnboundedReceiver<Command>,
    catalog: moq_mux::catalog::Consumer<()>,
    containers: HashMap<String, CatalogContainer>,
    subs: Subscriptions,
}

impl Driver {
    async fn run_broadcast(
        origin: moq_net::origin::Consumer,
        path: String,
        pid: LocalPid,
        latency: Duration,
        commands: mpsc::UnboundedReceiver<Command>,
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
            subs: Subscriptions::new(broadcast, latency),
        }
        .run()
        .await
    }

    async fn run(mut self) -> Option<CloseReason> {
        loop {
            let control_flow = tokio::select! {
              command = self.commands.recv() => self.handle_command(command),
              snapshot = self.catalog.next() => self.handle_catalog(snapshot),
              finished = self.subs.finished() => self.handle_finished(finished),
            };

            if let ControlFlow::Break(reason) = control_flow {
                return reason;
            }

            // needed because the catalog.next() branch
            // does not consume tokio budget
            tokio::task::coop::consume_budget().await;
        }
    }

    fn handle_command(&mut self, command: Option<Command>) -> ControlFlow<Option<CloseReason>> {
        match command {
            None | Some(Command::Close) => ControlFlow::Break(None),
            Some(Command::Unsubscribe { token }) => {
                self.subs.unsubscribe(token);
                ControlFlow::Continue(())
            }
            Some(Command::Subscribe {
                track,
                token,
                priority,
            }) => {
                let result = self.get_container(&track).and_then(|container| {
                    self.subs
                        .subscribe(token, self.pid, track, container, priority)
                });

                match result.or_else(|e| {
                    messages::send_track_error(&mut self.env, self.pid, token, e.to_string())
                }) {
                    Ok(()) => ControlFlow::Continue(()),
                    Err(messages::PidDead) => ControlFlow::Break(None),
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

    fn handle_finished(
        &mut self,
        (token, result): (Token, Result<(), TrackError>),
    ) -> ControlFlow<Option<CloseReason>> {
        let send_result = match result {
            Ok(()) => messages::send_track_finished(&mut self.env, self.pid, token),
            Err(e) => messages::send_track_error(&mut self.env, self.pid, token, e.to_string()),
        };

        match send_result {
            Ok(()) => ControlFlow::Continue(()),
            Err(messages::PidDead) => ControlFlow::Break(None),
        }
    }

    fn get_container(&self, track: &str) -> Result<WireContainer, TrackError> {
        self.containers
            .get(track)
            .ok_or(TrackError::NotAdvertised)?
            .try_into()
            .map_err(TrackError::Container)
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
