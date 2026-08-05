use std::collections::VecDeque;
use std::task::Poll;
use std::time::Duration;

use moq_mux::container::Frame;

use hang::moq_net;
use hang::moq_net::kio;

use crate::messages::Token;
use crate::track_format::WireContainer;

type WireConsumer = moq_mux::container::Consumer<WireContainer>;

struct PendingSub {
    subscribing: moq_net::track::Subscribing,
    container: WireContainer,
}

enum Subscription {
    Pending(PendingSub),
    Streaming(Box<WireConsumer>),
}

#[derive(Debug, thiserror::Error)]
enum QueueError {
    #[error("subscribe failed: {0}")]
    SubscribeFailed(#[from] hang::moq_net::Error),
    #[error("track read failed: {0}")]
    ReadFailed(#[from] moq_mux::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("track({track}): {source}")]
pub(super) struct TrackError {
    track: String,
    source: QueueError,
}

enum Step {
    Yield(Frame, Subscription),
    Wait(Subscription),
    Finished,
    Err(QueueError),
}

pub(super) enum PollEventResult {
    Frame(Frame),
    TrackFinished,
    TrackError(TrackError),
}

struct Entry {
    token: Token,
    track: String,
    state: Subscription,
}

pub(super) struct SubscriptionQueue {
    entries: VecDeque<Entry>,
    broadcast: moq_net::broadcast::Consumer,
    latency: Duration,
}

impl SubscriptionQueue {
    pub(super) fn new(broadcast: moq_net::broadcast::Consumer, latency: Duration) -> Self {
        Self {
            entries: VecDeque::new(),
            broadcast,
            latency,
        }
    }

    pub(super) fn insert(
        &mut self,
        token: Token,
        track: String,
        container: WireContainer,
        priority: u8,
    ) -> Result<(), TrackError> {
        self.remove(token);

        let consumer = match self.broadcast.track(&track) {
            Ok(consumer) => consumer,
            Err(e) => {
                return Err(TrackError {
                    track,
                    source: e.into(),
                });
            }
        };

        let subscription = moq_net::track::Subscription::default().with_priority(priority);
        self.entries.push_back(Entry {
            token,
            track,
            state: Subscription::Pending(PendingSub {
                subscribing: consumer.subscribe(subscription).into_inner(),
                container,
            }),
        });
        Ok(())
    }

    pub(super) fn remove(&mut self, target: Token) {
        self.entries.retain(|entry| entry.token != target);
    }

    pub(super) fn poll_event(&mut self, waiter: &kio::Waiter) -> Poll<(Token, PollEventResult)> {
        for _ in 0..self.entries.len() {
            let entry = self.entries.pop_front().expect("Queue should be non-empty");

            match Self::step(entry.state, self.latency, waiter) {
                Step::Wait(state) => {
                    self.entries.push_back(Entry { state, ..entry });
                }
                Step::Yield(frame, state) => {
                    self.entries.push_back(Entry { state, ..entry });
                    return Poll::Ready((entry.token, PollEventResult::Frame(frame)));
                }
                Step::Finished => {
                    return Poll::Ready((entry.token, PollEventResult::TrackFinished));
                }
                Step::Err(source) => {
                    return Poll::Ready((
                        entry.token,
                        PollEventResult::TrackError(TrackError {
                            track: entry.track,
                            source,
                        }),
                    ));
                }
            }
        }

        Poll::Pending
    }

    fn step(state: Subscription, latency: Duration, waiter: &kio::Waiter) -> Step {
        match state {
            Subscription::Pending(pending) => match pending.subscribing.poll_ok(waiter) {
                Poll::Ready(Ok(subscriber)) => {
                    let consumer =
                        WireConsumer::new(subscriber, pending.container).with_latency(latency);
                    Self::step(Subscription::Streaming(Box::new(consumer)), latency, waiter)
                }
                Poll::Ready(Err(e)) => Step::Err(e.into()),
                Poll::Pending => Step::Wait(Subscription::Pending(pending)),
            },
            Subscription::Streaming(mut consumer) => match consumer.poll_read(waiter) {
                Poll::Ready(Ok(Some(frame))) => {
                    Step::Yield(frame, Subscription::Streaming(consumer))
                }
                // `None` from a read signals track EOS
                Poll::Ready(Ok(None)) => Step::Finished,
                Poll::Ready(Err(e)) => Step::Err(e.into()),
                Poll::Pending => Step::Wait(Subscription::Streaming(consumer)),
            },
        }
    }
}
