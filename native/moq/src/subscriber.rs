use std::collections::HashMap;
use std::time::Duration;

use hang::moq_net;
use moq_native::ClientConfig;
use rustler::{
    Atom, Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, OwnedEnv, Resource, ResourceArc,
    Term,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;
use url::Url;

use crate::nif_types::{
    AacCodec, AudioTrackParams, H264Codec, H265Codec, TrackFormat, VideoTrackParams,
};
use crate::{atoms, runtime};

/// Token the Elixir side assigns to each subscription so it can route
/// `{:moq_frame, token, ...}` / `{:moq_track_ended, token, ...}` back to the
/// right pad. Opaque to Rust beyond being echoed in those messages.
type Token = i64;

enum Command {
    Subscribe { track: String, token: Token },
    Unsubscribe { token: Token },
}

pub(crate) struct SubscriberResource {
    commands: mpsc::UnboundedSender<Command>,
    shutdown: mpsc::UnboundedSender<()>,
}

impl Resource for SubscriberResource {}

/// Connect to a MoQ relay and prepare to subscribe to tracks of one broadcast.
///
/// The QUIC handshake and broadcast discovery happen asynchronously on a single
/// shared connection.
/// - `:moq_connected` is sent to `pid` once the relay has announced the broadcast.
/// - `{:moq_setup_failed, reason :: String.t()}` is sent if connecting or finding
///   the broadcast fails.
/// - `{:moq_disconnected, reason :: String.t()}` is sent when the session ends.
/// - `{:moq_tracks, [{name, format}]}` is sent whenever the broadcast catalog
///   changes, listing every advertised track and its codec parameters.
///
/// Tracks are subscribed via [`subscribe_track`]. Once the catalog advertises a
/// subscribed track, `{:moq_track_format, token, format}` is sent (`format` is
/// the same tagged codec-parameter term as in `{:moq_tracks, ...}`), followed by
/// `{:moq_frame, token, payload, timestamp_us, keyframe?}` per frame and a final
/// `{:moq_track_ended, token, reason}` when that track ends or errors.
///
/// `latency_ns` is how long each track buffers received frames before emitting
/// them, trading delay for resilience to network jitter and reordering.
#[rustler::nif]
pub(crate) fn start_subscriber(
    url: String,
    broadcast: String,
    pid: LocalPid,
    disable_tls_verify: bool,
    latency_ns: u64,
) -> NifResult<(Atom, ResourceArc<SubscriberResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;
    let latency = Duration::from_nanos(latency_ns);

    let (commands_tx, commands_rx) = mpsc::unbounded_channel::<Command>();
    let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let config = {
            let mut config = ClientConfig::default();
            config.tls.disable_verify = Some(disable_tls_verify);
            config
        };

        if let Err(e) = run_session(
            url,
            broadcast,
            &pid,
            config,
            latency,
            commands_rx,
            shutdown_rx,
        )
        .await
        {
            send_setup_failed(&pid, e.to_string());
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SubscriberResource {
            commands: commands_tx,
            shutdown: shutdown_tx,
        }),
    ))
}

/// Subscribe to `track` within the broadcast. Frames are tagged with `token` so
/// the caller can route them to the originating pad. No-op if the session task
/// has already ended.
#[rustler::nif]
pub(crate) fn subscribe_track(
    subscriber: ResourceArc<SubscriberResource>,
    track: String,
    token: Token,
) -> Atom {
    let _ = subscriber
        .commands
        .send(Command::Subscribe { track, token });
    atoms::ok()
}

/// Cancel the subscription identified by `token`. The pump drops silently
/// (no `{:moq_track_ended, ...}`), since the caller initiated the teardown.
/// Idempotent.
#[rustler::nif]
pub(crate) fn unsubscribe_track(subscriber: ResourceArc<SubscriberResource>, token: Token) -> Atom {
    let _ = subscriber.commands.send(Command::Unsubscribe { token });
    atoms::ok()
}

#[rustler::nif]
pub(crate) fn stop_subscriber(subscriber: ResourceArc<SubscriberResource>) -> Atom {
    let _ = subscriber.shutdown.send(());
    atoms::ok()
}

async fn run_session(
    url: Url,
    broadcast_name: String,
    pid: &LocalPid,
    config: ClientConfig,
    latency: Duration,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
    mut shutdown_rx: mpsc::UnboundedReceiver<()>,
) -> anyhow::Result<()> {
    // Subscriber role: prepare an OriginProducer that the relay will populate
    // with announced broadcasts, then consume from it.
    let origin = moq_net::Origin::random().produce();
    let mut origin_consumer = origin.consume();

    let client = config.init()?.with_consume(origin);
    let session = client.connect(url).await?;

    let broadcast = tokio::select! {
        broadcast = await_broadcast(&mut origin_consumer, &broadcast_name) => {
            broadcast.ok_or_else(|| anyhow::anyhow!(
                "broadcast {broadcast_name:?} was not announced before origin closed"
            ))?
        }
        _ = shutdown_rx.recv() => return Ok(()),
    };

    let _ = OwnedEnv::new().send_and_clear(pid, |env| atoms::moq_connected().to_term(env));

    // One pump per subscribed track, all sharing `broadcast` (and thus the
    // single connection). `cancels` lets us tear an individual pump down on
    // unsubscribe; `pumps` reaps finished pump tasks.
    let mut pumps: JoinSet<()> = JoinSet::new();
    let mut cancels: HashMap<Token, watch::Sender<bool>> = HashMap::new();

    // Watch the catalog and forward the advertised track list to Elixir as it
    // changes, so the source can announce tracks to its parent. Lives in the
    // same JoinSet, so it is aborted when this session task returns. When the
    // catalog closes (the broadcast was unannounced, e.g. the publisher left)
    // the watcher reports why via `catalog_done`, and we end the session so the
    // Elixir side can tear down and resubscribe, mirroring moq-gst's moqsrc.
    let (catalog_done_tx, mut catalog_done_rx) = oneshot::channel::<String>();
    pumps.spawn(run_catalog_watcher(
        broadcast.clone(),
        *pid,
        catalog_done_tx,
    ));

    let disconnect_reason = loop {
        tokio::select! {
            command = commands_rx.recv() => match command {
                Some(Command::Subscribe { track, token }) => {
                    let (cancel_tx, cancel_rx) = watch::channel(false);
                    cancels.insert(token, cancel_tx);
                    pumps.spawn(run_pump(broadcast.clone(), track, token, *pid, latency, cancel_rx));
                }
                Some(Command::Unsubscribe { token }) => {
                    if let Some(cancel) = cancels.remove(&token) {
                        let _ = cancel.send(true);
                    }
                }
                // The resource was dropped without a stop; treat as shutdown.
                None => return Ok(()),
            },
            _ = shutdown_rx.recv() => return Ok(()),
            // A pump finished; the JoinSet reaps it. It already sent its own
            // {:moq_track_ended, ...}, so there's nothing to forward here.
            _ = pumps.join_next(), if !pumps.is_empty() => {}
            // The catalog closed: the broadcast is gone. End the session so the
            // Elixir side disconnects and can resubscribe to a fresh announce.
            reason = &mut catalog_done_rx => break match reason {
                Ok(reason) => reason,
                Err(_) => "catalog watcher stopped".to_string(),
            },
            result = session.closed() => break match result {
                Ok(()) => "session closed gracefully".to_string(),
                Err(e) => format!("session error: {e}"),
            },
        }
    };

    send_disconnected(pid, disconnect_reason);
    Ok(())
}

/// Reads frames from one track and forwards them tagged with `token`, until the
/// track ends, errors, or `cancel` fires. A clean end or error sends
/// `{:moq_track_ended, token, reason}`; cancellation stays silent (the Elixir
/// side asked for it).
async fn run_pump(
    broadcast: moq_net::BroadcastConsumer,
    track_name: String,
    token: Token,
    pid: LocalPid,
    latency: Duration,
    mut cancel: watch::Receiver<bool>,
) {
    let reason = tokio::select! {
        _ = cancel.changed() => return,
        result = pump_track(&broadcast, &track_name, token, &pid, latency) => match result {
            Ok(()) => "track ended".to_string(),
            Err(e) => format!("track error: {e}"),
        }
    };

    send_track_ended(&pid, token, reason);
}

async fn pump_track(
    broadcast: &moq_net::BroadcastConsumer,
    track_name: &str,
    token: Token,
    pid: &LocalPid,
    latency: Duration,
) -> anyhow::Result<()> {
    // Wait for the hang catalog to advertise the requested track before
    // subscribing to it. Publishers (including our own Sink) create tracks
    // lazily on the first stream_format, so a naive subscribe_track racing
    // the broadcast announcement returns NotFound from the relay. The catalog
    // also tells us the track's codec parameters, which we forward as the stream
    // format before any frame so the Elixir side can build the pad's format.
    let params = wait_for_track(broadcast, track_name).await?;
    send_track_format(pid, token, &params);

    let track_ref = moq_net::Track {
        name: track_name.to_string(),
        priority: 0,
    };
    let track_consumer = broadcast
        .subscribe_track(&track_ref)
        .map_err(|e| anyhow::anyhow!("subscribe_track({track_name}) failed: {e}"))?;

    let mut consumer =
        moq_mux::container::Consumer::new(track_consumer, moq_mux::container::legacy::Wire)
            .with_latency(latency);

    pump_frames(&mut consumer, token, pid).await
}

/// Wait until `name` is announced on the origin and return its consumer.
///
/// `announced()` yields `(path, Some(consumer))` on announce and
/// `(path, None)` on unannounce; we skip everything but a matching announce.
async fn await_broadcast(
    consumer: &mut moq_net::OriginConsumer,
    name: &str,
) -> Option<moq_net::BroadcastConsumer> {
    while let Some((path, broadcast)) = consumer.announced().await {
        if path.as_str() == name {
            if let Some(broadcast) = broadcast {
                return Some(broadcast);
            }
        }
    }
    None
}

/// Block until the catalog advertises `track_name`, then return its codec
/// parameters. Mirrors the codecs the Sink can publish; anything else (or a
/// codec we don't translate) resolves to [`TrackParams::Unrecognized`].
async fn wait_for_track(
    broadcast: &moq_net::BroadcastConsumer,
    track_name: &str,
) -> anyhow::Result<TrackParams> {
    let catalog_track = broadcast
        .subscribe_track(&hang::Catalog::default_track())
        .map_err(|e| anyhow::anyhow!("subscribe_track(catalog) failed: {e}"))?;
    let mut catalog = moq_mux::catalog::hang::Consumer::<()>::new(catalog_track);

    loop {
        let snapshot = catalog.next().await?.ok_or_else(|| {
            anyhow::anyhow!("catalog track closed before {track_name:?} appeared")
        })?;

        if let Some(config) = snapshot.video.renditions.get(track_name) {
            return Ok(video_params(config));
        }
        if let Some(config) = snapshot.audio.renditions.get(track_name) {
            return Ok(audio_params(config));
        }
    }
}

async fn pump_frames(
    consumer: &mut moq_mux::container::Consumer<moq_mux::container::legacy::Wire>,
    token: Token,
    pid: &LocalPid,
) -> anyhow::Result<()> {
    while let Some(frame) = consumer.read().await? {
        // u128 → i64: real-world frame timestamps fit. If a stream somehow
        // overflows i64 microseconds (~292,000 years), the cast wraps and the
        // Elixir side sees a meaningless value, but no UB.
        let timestamp_us = frame.timestamp.as_micros() as i64;
        let keyframe = frame.keyframe;
        let payload = frame.payload;

        let send_result = OwnedEnv::new().send_and_clear(pid, |env| {
            let mut bin = OwnedBinary::new(payload.len())
                .expect("failed to allocate Erlang binary for moq frame");
            bin.as_mut_slice().copy_from_slice(&payload);
            (
                atoms::moq_frame(),
                token,
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

/// Owned, NIF-free description of one advertised track, ready to encode.
struct TrackEntry {
    name: String,
    params: TrackParams,
}

enum VideoCodecParams {
    H264(H264Codec),
    H265(H265Codec),
}

enum AudioCodecParams {
    Aac(AacCodec),
    Opus,
}

/// Owned (env-free) form of a track's codec parameters, collected by the
/// catalog watcher and converted to a [`TrackFormat`] at encode time. Mirrors
/// `TrackFormat` but holds owned bytes instead of an `env`-bound `Binary`, so it
/// can be carried across `await` points before a term env exists.
enum TrackParams {
    Video {
        params: VideoTrackParams,
        /// Decoder configuration record (avcC/hvcC); empty when carried in-band.
        description: Vec<u8>,
        codec: VideoCodecParams,
    },
    Audio {
        params: AudioTrackParams,
        codec: AudioCodecParams,
    },
    /// A track whose codec the source does not translate to a Membrane format.
    Unrecognized,
}

/// Subscribe to the broadcast catalog and forward the full advertised track set
/// to `pid` on every update as `{:moq_tracks, [{name, format}]}`. Reports why it
/// stopped on `done` once the catalog closes (broadcast gone) or `pid` dies.
async fn run_catalog_watcher(
    broadcast: moq_net::BroadcastConsumer,
    pid: LocalPid,
    done: oneshot::Sender<String>,
) {
    let reason = match watch_catalog(&broadcast, &pid).await {
        Ok(()) => "broadcast ended".to_string(),
        Err(e) => format!("catalog error: {e}"),
    };
    let _ = done.send(reason);
}

async fn watch_catalog(
    broadcast: &moq_net::BroadcastConsumer,
    pid: &LocalPid,
) -> anyhow::Result<()> {
    let catalog_track = broadcast
        .subscribe_track(&hang::Catalog::default_track())
        .map_err(|e| anyhow::anyhow!("subscribe_track(catalog) failed: {e}"))?;
    let mut catalog = moq_mux::catalog::hang::Consumer::<()>::new(catalog_track);

    while let Some(snapshot) = catalog.next().await? {
        let mut entries = Vec::new();
        for (name, config) in &snapshot.video.renditions {
            entries.push(TrackEntry {
                name: name.clone(),
                params: video_params(config),
            });
        }
        for (name, config) in &snapshot.audio.renditions {
            entries.push(TrackEntry {
                name: name.clone(),
                params: audio_params(config),
            });
        }
        send_tracks(pid, &entries)?;
    }

    Ok(())
}

fn video_params(config: &hang::catalog::VideoConfig) -> TrackParams {
    let codec = match &config.codec {
        hang::catalog::VideoCodec::H264(h) => VideoCodecParams::H264(H264Codec {
            inline: h.inline,
            profile: h.profile,
            constraints: h.constraints,
            level: h.level,
        }),
        hang::catalog::VideoCodec::H265(h) => VideoCodecParams::H265(H265Codec {
            in_band: h.in_band,
            profile_space: h.profile_space,
            profile_idc: h.profile_idc,
            profile_compatibility_flags: h.profile_compatibility_flags.to_vec(),
            tier_flag: h.tier_flag,
            level_idc: h.level_idc,
            constraint_flags: h.constraint_flags.to_vec(),
        }),
        _ => return TrackParams::Unrecognized,
    };

    // `VideoTrackParams` carries concrete values; the catalog leaves these
    // optional, so coerce a missing dimension/framerate to 0 (the Elixir side
    // treats a 0 framerate as "unknown").
    TrackParams::Video {
        params: VideoTrackParams {
            width: config.coded_width.unwrap_or(0),
            height: config.coded_height.unwrap_or(0),
            framerate: config.framerate.unwrap_or(0.0),
        },
        description: config
            .description
            .as_ref()
            .map(|b| b.to_vec())
            .unwrap_or_default(),
        codec,
    }
}

fn audio_params(config: &hang::catalog::AudioConfig) -> TrackParams {
    let codec = match &config.codec {
        hang::catalog::AudioCodec::AAC(aac) => AudioCodecParams::Aac(AacCodec {
            profile: aac.profile,
        }),
        hang::catalog::AudioCodec::Opus => AudioCodecParams::Opus,
        _ => return TrackParams::Unrecognized,
    };

    TrackParams::Audio {
        params: AudioTrackParams {
            sample_rate: config.sample_rate,
            channels: config.channel_count,
        },
        codec,
    }
}

fn send_tracks(pid: &LocalPid, tracks: &[TrackEntry]) -> anyhow::Result<()> {
    let result = OwnedEnv::new().send_and_clear(pid, |env| {
        let list: Vec<Term> = tracks.iter().map(|t| encode_track(env, t)).collect();
        (atoms::moq_tracks(), list).encode(env)
    });
    result.map_err(|_| anyhow::anyhow!("subscriber pid is dead"))
}

fn encode_track<'a>(env: Env<'a>, entry: &TrackEntry) -> Term<'a> {
    (entry.name.as_str(), encode_format(env, &entry.params)).encode(env)
}

/// Build the shared [`TrackFormat`] term from owned params. This is the inverse
/// of the Sink's decode, so both directions speak the identical shape.
fn encode_format<'a>(env: Env<'a>, params: &TrackParams) -> Term<'a> {
    let format = match params {
        TrackParams::Video {
            params,
            description,
            codec,
        } => {
            let description = make_binary(env, description);
            match codec {
                VideoCodecParams::H264(codec) => TrackFormat::H264 {
                    params: params.clone(),
                    description,
                    codec: codec.clone(),
                },
                VideoCodecParams::H265(codec) => TrackFormat::H265 {
                    params: params.clone(),
                    description,
                    codec: codec.clone(),
                },
            }
        }
        TrackParams::Audio { params, codec } => match codec {
            AudioCodecParams::Aac(codec) => TrackFormat::Aac {
                params: params.clone(),
                codec: codec.clone(),
            },
            AudioCodecParams::Opus => TrackFormat::Opus {
                params: params.clone(),
            },
        },
        TrackParams::Unrecognized => TrackFormat::Unrecognized,
    };

    format.encode(env)
}

fn make_binary<'a>(env: Env<'a>, bytes: &[u8]) -> Binary<'a> {
    let mut bin = OwnedBinary::new(bytes.len()).expect("binary allocation should succeed");
    bin.as_mut_slice().copy_from_slice(bytes);
    bin.release(env)
}

fn send_setup_failed(pid: &LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(pid, |env| (atoms::moq_setup_failed(), reason).encode(env));
}

fn send_track_format(pid: &LocalPid, token: Token, params: &TrackParams) {
    let _ = OwnedEnv::new().send_and_clear(pid, |env| {
        (atoms::moq_track_format(), token, encode_format(env, params)).encode(env)
    });
}

fn send_track_ended(pid: &LocalPid, token: Token, reason: String) {
    let _ = OwnedEnv::new().send_and_clear(pid, |env| {
        (atoms::moq_track_ended(), token, reason).encode(env)
    });
}

fn send_disconnected(pid: &LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(pid, |env| (atoms::moq_disconnected(), reason).encode(env));
}
