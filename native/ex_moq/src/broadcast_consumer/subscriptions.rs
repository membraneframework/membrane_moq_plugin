use std::collections::HashMap;

use tokio::task::{AbortHandle, JoinSet};

use crate::messages::Token;

use super::pump::Pump;
use super::{CatalogSnapshot, Rendition};

struct PumpGuard(AbortHandle);

impl PumpGuard {
    fn task_id(&self) -> tokio::task::Id {
        self.0.id()
    }
}

impl Drop for PumpGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Sub {
    track: String,
    priority: u8,
    /// `None` until the track is announced in the catalog and a pump is spawned.
    pump: Option<PumpGuard>,
}

pub(super) struct Subscriptions {
    subs: HashMap<Token, Sub>,
    /// mapping from spawned task to subscription token.
    by_task: HashMap<tokio::task::Id, Token>,
    pumps: JoinSet<anyhow::Result<()>>,
}

impl Subscriptions {
    pub(super) fn new() -> Self {
        Self {
            subs: HashMap::new(),
            by_task: HashMap::new(),
            pumps: JoinSet::new(),
        }
    }

    pub(super) fn insert_pending(&mut self, token: Token, track: String, priority: u8) {
        self.remove(token);
        let sub = Sub {
            track,
            priority,
            pump: None,
        };
        self.subs.insert(token, sub);
    }

    pub(super) fn insert_active(&mut self, token: Token, track: String, priority: u8, pump: Pump) {
        self.remove(token);
        let abort_handle = self.pumps.spawn(pump.run());
        self.by_task.insert(abort_handle.id(), token);
        let sub = Sub {
            track,
            priority,
            pump: Some(PumpGuard(abort_handle)),
        };
        self.subs.insert(token, sub);
    }

    pub(super) fn remove(&mut self, token: Token) {
        self.subs
            .remove(&token)
            .and_then(|sub| sub.pump)
            .inspect(|pump| {
                self.by_task.remove(&pump.task_id());
            });
    }

    /// Routes a finished pump task back to its subscription and forgets both.
    /// Returns `None` for tasks aborted through `remove`.
    fn reap(&mut self, id: tokio::task::Id) -> Option<Token> {
        let token = self.by_task.remove(&id)?;
        self.subs.remove(&token);
        Some(token)
    }

    pub(super) fn remove_active_where(&mut self, mut pred: impl FnMut(&str) -> bool) -> Vec<Token> {
        let Self { subs, by_task, .. } = self;
        subs.extract_if(|_, sub| sub.pump.is_some() && pred(&sub.track))
            .map(|(token, sub)| {
                let Some(pump) = sub.pump else { unreachable!() };
                by_task.remove(&pump.task_id());
                token
            })
            .collect()
    }

    pub(super) fn drain_resolved<'r>(
        &mut self,
        renditions: &'r CatalogSnapshot,
    ) -> Vec<(Token, String, u8, &'r Rendition)> {
        self.subs
            .extract_if(|_, sub| sub.pump.is_none() && renditions.contains_key(sub.track.as_str()))
            .map(|(token, sub)| {
                let rendition = &renditions[&sub.track];
                (token, sub.track, sub.priority, rendition)
            })
            .collect()
    }

    pub(super) fn has_pumps(&self) -> bool {
        !self.pumps.is_empty()
    }

    /// Waits for the next pump task to finish
    /// and routes it back to its subscription, forgetting both.
    /// Tasks whose subscription was already removed are skipped.
    /// `None` means no tasks are left.
    pub(super) async fn join_next(&mut self) -> Option<(Token, anyhow::Result<()>)> {
        loop {
            let (id, outcome) = match self.pumps.join_next_with_id().await? {
                Ok((id, outcome)) => (id, outcome),
                Err(e) => (e.id(), Err(anyhow::anyhow!("pump task died: {e}"))),
            };

            if let Some(token) = self.reap(id) {
                return Some((token, outcome));
            }
        }
    }
}
