use std::collections::VecDeque;
use std::task::Poll;
use std::time::Duration;

use moq_mux::container::Frame;

use hang::moq_net;
use hang::moq_net::kio;

use crate::messages::Token;

type WireConsumer = moq_mux::container::Consumer<moq_mux::catalog::hang::Container>;

struct PendingSub {
    subscribing: moq_net::track::Subscribing,
    container: moq_mux::catalog::hang::Container,
}

enum Subscription {
    Failed(QueueError),
    Pending(PendingSub),
    Streaming(Box<WireConsumer>),
}

#[derive(Debug, thiserror::Error)]
enum QueueError {
    #[error("subscribe failed: {0}")]
    Pending(#[from] hang::moq_net::Error),
    #[error("track read failed: {0}")]
    Streaming(#[from] moq_mux::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("track({track}): {source}")]
pub(super) struct TrackError {
    track: String,
    source: QueueError,
}

enum Polled {
    Frame(Frame, Box<WireConsumer>),
    Pending(Subscription),
    Failed(QueueError),
    Finished,
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

    pub(super) fn poll_event(&mut self, waiter: &kio::Waiter) -> Poll<(Token, PollEventResult)> {
        for _ in 0..self.entries.len() {
            let Entry {
                token,
                track,
                state,
            } = self
                .entries
                .pop_front()
                .expect("Queue with positive length should be non-empty");

            let result = match state {
                Subscription::Failed(source) => Polled::Failed(source),
                Subscription::Pending(pending) => {
                    Self::handle_pending(pending, self.latency, waiter)
                }
                Subscription::Streaming(consumer) => Self::handle_streaming(consumer, waiter),
            };

            match result {
                Polled::Frame(frame, consumer) => {
                    self.entries.push_back(Entry {
                        token,
                        track,
                        state: Subscription::Streaming(consumer),
                    });
                    return Poll::Ready((token, PollEventResult::Frame(frame)));
                }
                Polled::Failed(source) => {
                    return Poll::Ready((
                        token,
                        PollEventResult::TrackError(TrackError { track, source }),
                    ));
                }
                Polled::Pending(state) => {
                    self.entries.push_back(Entry {
                        token,
                        track,
                        state,
                    });
                }
                Polled::Finished => {
                    return Poll::Ready((token, PollEventResult::TrackFinished));
                }
            }
        }

        Poll::Pending
    }

    pub(super) fn insert(
        &mut self,
        token: Token,
        track: String,
        container: moq_mux::catalog::hang::Container,
        priority: u8,
    ) {
        self.remove(token);
        let state = match self.broadcast.track(&track) {
            Ok(consumer) => {
                let subscription = moq_net::track::Subscription::default().with_priority(priority);
                Subscription::Pending(PendingSub {
                    subscribing: consumer.subscribe(subscription).into_inner(),
                    container,
                })
            }
            Err(e) => Subscription::Failed(e.into()),
        };
        self.entries.push_back(Entry {
            token,
            track,
            state,
        });
    }

    pub(super) fn remove(&mut self, target: Token) {
        self.entries.retain(|entry| entry.token != target);
    }

    fn handle_pending(pending: PendingSub, latency: Duration, waiter: &kio::Waiter) -> Polled {
        match pending.subscribing.poll_ok(waiter) {
            Poll::Ready(Ok(subscriber)) => {
                let consumer =
                    WireConsumer::new(subscriber, pending.container).with_latency(latency);
                Self::handle_streaming(Box::new(consumer), waiter)
            }
            Poll::Ready(Err(e)) => Polled::Failed(QueueError::Pending(e)),
            Poll::Pending => Polled::Pending(Subscription::Pending(pending)),
        }
    }

    fn handle_streaming(mut consumer: Box<WireConsumer>, waiter: &kio::Waiter) -> Polled {
        match consumer.poll_read(waiter) {
            Poll::Ready(Ok(Some(frame))) => Polled::Frame(frame, consumer),
            Poll::Ready(Ok(None)) => Polled::Finished,
            Poll::Ready(Err(e)) => Polled::Failed(QueueError::Streaming(e)),
            Poll::Pending => Polled::Pending(Subscription::Streaming(consumer)),
        }
    }
}
