use std::time::Duration;

use moq_native::ClientConfig;
use rustler::{Atom, Encoder, LocalPid, NifResult, OwnedBinary, OwnedEnv, Resource, ResourceArc};
use tokio::sync::mpsc;
use url::Url;

use crate::{atoms, runtime};

pub(crate) struct SubscriberResource {
    shutdown: mpsc::UnboundedSender<()>,
}

impl Resource for SubscriberResource {}

/// Connect to a MoQ relay and subscribe to a single (broadcast, track) pair.
///
/// The QUIC handshake and broadcast discovery happen asynchronously.
/// - `:moq_connected` is sent to `pid` once the relay has announced the
///   broadcast and the subscription is open.
/// - `{:moq_frame, payload :: binary, timestamp_us :: integer, keyframe? :: boolean}`
///   is sent for every received frame.
/// - `{:moq_setup_failed, reason :: String.t()}` is sent if establishing
///   the connection or finding the broadcast fails.
/// - `{:moq_disconnected, reason :: String.t()}` is sent when the track or
///   session ends (cleanly or with an error).
#[rustler::nif]
pub(crate) fn start_subscriber(
    url: String,
    broadcast: String,
    track: String,
    pid: LocalPid,
    disable_tls_verify: bool,
) -> NifResult<(Atom, ResourceArc<SubscriberResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;

    let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let config = {
            let mut config = ClientConfig::default();
            config.tls.disable_verify = Some(disable_tls_verify);
            config
        };

        if let Err(e) = run_subscriber(url, broadcast, track, &pid, config, shutdown_rx).await {
            send_setup_failed(&pid, e.to_string());
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SubscriberResource {
            shutdown: shutdown_tx,
        }),
    ))
}

#[rustler::nif]
pub(crate) fn stop_subscriber(resource: ResourceArc<SubscriberResource>) -> Atom {
    let _ = resource.shutdown.send(());
    atoms::ok()
}

async fn run_subscriber(
    url: Url,
    broadcast_name: String,
    track_name: String,
    pid: &LocalPid,
    config: ClientConfig,
    mut shutdown_rx: mpsc::UnboundedReceiver<()>,
) -> anyhow::Result<()> {
    // Subscriber role: prepare an OriginProducer that the relay will populate
    // with announced broadcasts, then consume from it.
    let origin = hang::moq_lite::Origin::random().produce();
    let origin_consumer = origin.consume();

    let client = config.init()?.with_consume(origin);
    let session = client.connect(url).await?;

    let broadcast = tokio::select! {
        broadcast = origin_consumer.announced_broadcast(&broadcast_name) => {
            broadcast.ok_or_else(|| anyhow::anyhow!(
                "broadcast {broadcast_name:?} was not announced before origin closed"
            ))?
        }
        _ = shutdown_rx.recv() => return Ok(()),
    };

    // Wait for the hang catalog to advertise the requested track before
    // subscribing to it. Publishers (including our own Sink) create tracks
    // lazily on the first stream_format, so a naive subscribe_track racing
    // the broadcast announcement returns NotFound from the relay.
    tokio::select! {
        result = wait_for_track(&broadcast, &track_name) => result?,
        _ = shutdown_rx.recv() => return Ok(()),
    }

    let track_ref = hang::moq_lite::Track {
        name: track_name.clone(),
        priority: 0,
    };
    let track_consumer = broadcast
        .subscribe_track(&track_ref)
        .map_err(|e| anyhow::anyhow!("subscribe_track({track_name}) failed: {e}"))?;

    let mut consumer =
        moq_mux::container::Consumer::new(track_consumer, moq_mux::container::Hang::Legacy)
            .with_latency(Duration::from_secs(1));

    send_connected(pid);

    let disconnect_reason = tokio::select! {
        result = pump_frames(&mut consumer, pid) => match result {
            Ok(()) => "track ended".to_string(),
            Err(e) => format!("track error: {e}"),
        },
        result = session.closed() => match result {
            Ok(()) => "session closed gracefully".to_string(),
            Err(e) => format!("session error: {e}"),
        },
        _ = shutdown_rx.recv() => return Ok(()),
    };

    send_disconnected(pid, disconnect_reason);
    Ok(())
}

async fn wait_for_track(
    broadcast: &hang::moq_lite::BroadcastConsumer,
    track_name: &str,
) -> anyhow::Result<()> {
    let catalog_track = broadcast
        .subscribe_track(&hang::Catalog::default_track())
        .map_err(|e| anyhow::anyhow!("subscribe_track(catalog) failed: {e}"))?;
    let mut catalog = moq_mux::catalog::Consumer::new(catalog_track);

    loop {
        let snapshot = catalog
            .next()
            .await?
            .ok_or_else(|| anyhow::anyhow!("catalog track closed before {track_name:?} appeared"))?;

        if snapshot.video.renditions.contains_key(track_name)
            || snapshot.audio.renditions.contains_key(track_name)
        {
            return Ok(());
        }
    }
}

async fn pump_frames(
    consumer: &mut moq_mux::container::Consumer<moq_mux::container::Hang>,
    pid: &LocalPid,
) -> anyhow::Result<()> {
    while let Some(frame) = consumer.read().await? {
        // u128 → i64: real-world frame timestamps fit. If a stream somehow
        // overflows i64 microseconds (~292,000 years), the cast wraps and the
        // Elixir side sees a meaningless value, but no UB.
        let timestamp_us = frame.timestamp.as_micros() as i64;
        let keyframe = frame.keyframe;
        let payload = frame.payload;
        let pid = *pid;

        let send_result = OwnedEnv::new().send_and_clear(&pid, |env| {
            let mut bin = OwnedBinary::new(payload.len())
                .expect("failed to allocate Erlang binary for moq frame");
            bin.as_mut_slice().copy_from_slice(&payload);
            (
                atoms::moq_frame(),
                bin.release(env),
                timestamp_us,
                keyframe,
            )
                .encode(env)
        });

        if send_result.is_err() {
            return Err(anyhow::anyhow!("subscriber pid is dead"));
        }
    }
    Ok(())
}

fn send_connected(pid: &LocalPid) {
    let _ = OwnedEnv::new().send_and_clear(pid, |env| atoms::moq_connected().to_term(env));
}

fn send_setup_failed(pid: &LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(pid, |env| (atoms::moq_setup_failed(), reason).encode(env));
}

fn send_disconnected(pid: &LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(pid, |env| (atoms::moq_disconnected(), reason).encode(env));
}
