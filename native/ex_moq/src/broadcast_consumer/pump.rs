use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};

use hang::moq_net;

use crate::messages::{self, Token};

use super::Ctx;

/// Everything one track subscription needs to pump its frames to Elixir.
pub(super) struct Pump {
    broadcast: moq_net::broadcast::Consumer,
    track: String,
    wire: moq_mux::catalog::hang::Container,
    token: Token,
    priority: u8,
    pid: LocalPid,
    latency: Duration,
    env: OwnedEnv,
}

impl Pump {
    pub(super) fn new(
        ctx: &Ctx,
        track: String,
        wire: moq_mux::catalog::hang::Container,
        token: Token,
        priority: u8,
    ) -> Self {
        Self {
            broadcast: ctx.broadcast.clone(),
            track,
            wire,
            token,
            priority,
            pid: ctx.pid,
            latency: ctx.latency,
            env: OwnedEnv::new(),
        }
    }

    pub(super) async fn run(mut self) -> anyhow::Result<()> {
        let handle = self
            .broadcast
            .track(&self.track)
            .map_err(|e| anyhow::anyhow!("track({}) failed: {e}", self.track))?;

        let mut subscription = moq_net::track::Subscription::default();
        subscription.priority = self.priority;
        let track_consumer = handle
            .subscribe(subscription)
            .await
            .map_err(|e| anyhow::anyhow!("subscribe({}) failed: {e}", self.track))?;

        let mut consumer =
            moq_mux::container::Consumer::new(track_consumer, self.wire).with_latency(self.latency);

        while let Some(frame) = consumer.read().await? {
            let timestamp_ns = frame.timestamp.as_nanos() as u64;

            messages::send_frame(
                &mut self.env,
                self.pid,
                self.token,
                &frame.payload,
                timestamp_ns,
                frame.keyframe,
            )
            .map_err(|_| anyhow::anyhow!("consumer pid is dead"))?;
        }

        Ok(())
    }
}
