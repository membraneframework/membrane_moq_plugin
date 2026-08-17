use std::collections::HashMap;
use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};

use hang::moq_net;
use tokio::task::{AbortHandle, JoinSet};

use crate::messages::{self, Token};
use crate::track_format::WireContainer;

use super::TrackError;

type WireConsumer = moq_mux::container::Consumer<WireContainer>;

pub(super) struct Subscriptions {
    broadcast: moq_net::broadcast::Consumer,
    latency: Duration,
    tasks: JoinSet<(Token, Result<(), TrackError>)>,
    aborts: HashMap<Token, AbortHandle>,
}

impl Subscriptions {
    pub(super) fn new(broadcast: moq_net::broadcast::Consumer, latency: Duration) -> Self {
        Self {
            broadcast,
            latency,
            tasks: JoinSet::new(),
            aborts: HashMap::new(),
        }
    }

    pub(super) fn subscribe(
        &mut self,
        token: Token,
        pid: LocalPid,
        track: String,
        container: WireContainer,
        priority: u8,
    ) -> Result<(), TrackError> {
        let consumer = self
            .broadcast
            .track(&track)
            .map_err(TrackError::SubscribeFailed)?;

        let latency = self.latency;
        let handle = self.tasks.spawn(async move {
            let result = run_subscription(token, pid, consumer, container, priority, latency);
            (token, result.await)
        });

        if let Some(previous) = self.aborts.insert(token, handle) {
            previous.abort();
        }
        Ok(())
    }

    pub(super) fn unsubscribe(&mut self, token: Token) {
        if let Some(handle) = self.aborts.remove(&token) {
            handle.abort();
        }
    }

    pub(super) async fn finished(&mut self) -> (Token, Result<(), TrackError>) {
        loop {
            match self.tasks.join_next_with_id().await {
                Some(Ok((_id, (token, result)))) => {
                    self.aborts.remove(&token);
                    return (token, result);
                }
                Some(Err(join_error)) if join_error.is_cancelled() => continue,
                Some(Err(join_error)) => {
                    let id = join_error.id();
                    let token = self
                        .aborts
                        .iter()
                        .find(|(_, handle)| handle.id() == id)
                        .map(|(token, _)| *token);

                    let Some(token) = token else { continue };

                    self.aborts.remove(&token);
                    return (token, Err(TrackError::Panicked(join_error)));
                }
                None => std::future::pending().await,
            }
        }
    }
}

async fn run_subscription(
    token: Token,
    pid: LocalPid,
    consumer: moq_net::track::Consumer,
    container: WireContainer,
    priority: u8,
    latency: Duration,
) -> Result<(), TrackError> {
    let subscriber = consumer
        .subscribe(moq_net::track::Subscription::default().with_priority(priority))
        .await?;

    let mut consumer = WireConsumer::new(subscriber, container).with_latency(latency);

    let mut env = OwnedEnv::new();

    loop {
        let Some(frame) = consumer.read().await.map_err(TrackError::ReadFailed)? else {
            break;
        };
        if let Err(messages::PidDead) = messages::send_frame(&mut env, pid, token, frame) {
            break;
        }
    }

    Ok(())
}
