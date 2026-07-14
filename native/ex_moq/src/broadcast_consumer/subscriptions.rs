use std::collections::HashMap;

use tokio::task::{AbortHandle, JoinSet};

use crate::messages::Token;

use super::pump::Pump;

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

pub(super) struct Subscriptions {
    pumps: JoinSet<anyhow::Result<()>>,
    /// mapping from spawned task to subscription token.
    by_task: HashMap<tokio::task::Id, Token>,
    by_token: HashMap<Token, PumpGuard>,
}

impl Subscriptions {
    pub(super) fn new() -> Self {
        Self {
            pumps: JoinSet::new(),
            by_task: HashMap::new(),
            by_token: HashMap::new(),
        }
    }

    pub(super) fn insert(&mut self, token: Token, pump: Pump) {
        self.remove(token);
        let abort_handle = self.pumps.spawn(pump.run());
        self.by_task.insert(abort_handle.id(), token);
        self.by_token.insert(token, PumpGuard(abort_handle));
    }

    pub(super) fn remove(&mut self, token: Token) {
        if let Some(pump) = self.by_token.remove(&token) {
            self.by_task.remove(&pump.task_id());
        }
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

            if let Some(token) = self.by_task.remove(&id) {
                self.by_token.remove(&token);
                return Some((token, outcome));
            }
        }
    }
}
