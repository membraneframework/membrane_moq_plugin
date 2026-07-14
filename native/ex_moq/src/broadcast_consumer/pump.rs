use std::time::Duration;

use rustler::{LocalPid, OwnedEnv};

use hang::moq_net;

use crate::messages::{self, Token};

use super::{Ctx, Rendition};

/// Everything one track subscription needs to pump its frames to Elixir.
pub(super) struct Pump {
    broadcast: moq_net::BroadcastConsumer,
    track: String,
    container: hang::catalog::Container,
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
        rendition: &Rendition,
        token: Token,
        priority: u8,
    ) -> Self {
        Self {
            broadcast: ctx.broadcast.clone(),
            track,
            container: rendition.container.clone(),
            token,
            priority,
            pid: ctx.pid,
            latency: ctx.latency,
            env: OwnedEnv::new(),
        }
    }

    pub(super) async fn run(mut self) {
        let reason = tokio::select! {
            result = self.pump_track() => match result {
                Ok(()) => "track ended".to_string(),
                Err(e) => format!("track error: {e}"),
            }
        };

        messages::send_track_ended(&mut self.env, self.pid, self.token, reason);
    }

    async fn pump_track(&mut self) -> anyhow::Result<()> {
        let track = &self.track;

        let wire = moq_mux::catalog::hang::Container::try_from(&self.container)
            .map_err(|e| anyhow::anyhow!("unsupported container on track {track}: {e}"))?;

        let track_ref = moq_net::Track {
            name: track.clone(),
            priority: self.priority,
        };
        let track_consumer = self
            .broadcast
            .subscribe_track(&track_ref)
            .map_err(|e| anyhow::anyhow!("subscribe_track({track}) failed: {e}"))?;

        let mut consumer =
            moq_mux::container::Consumer::new(track_consumer, wire).with_latency(self.latency);

        pump_frames(&mut consumer, self.token, self.pid, &mut self.env).await
    }
}

async fn pump_frames(
    consumer: &mut moq_mux::container::Consumer<moq_mux::catalog::hang::Container>,
    token: Token,
    pid: LocalPid,
    env: &mut OwnedEnv,
) -> anyhow::Result<()> {
    while let Some(frame) = consumer.read().await? {
        let timestamp_ns = frame.timestamp.as_nanos() as u64;

        messages::send_frame(
            env,
            pid,
            token,
            &frame.payload,
            timestamp_ns,
            frame.keyframe,
        )
        .map_err(|_| anyhow::anyhow!("consumer pid is dead"))?;
    }
    Ok(())
}
