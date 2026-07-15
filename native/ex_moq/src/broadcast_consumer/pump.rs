use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};

use hang::moq_net;

use crate::messages::{self, Token};

use super::Ctx;

/// Everything one track subscription needs to pump its frames to Elixir.
pub(super) struct Pump {
    broadcast: moq_net::BroadcastConsumer,
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
        let track = &self.track;

        let track_ref = moq_net::Track {
            name: track.clone(),
            priority: self.priority,
        };
        let track_consumer = self
            .broadcast
            .subscribe_track(&track_ref)
            .map_err(|e| anyhow::anyhow!("subscribe_track({track}) failed: {e}"))?;

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
