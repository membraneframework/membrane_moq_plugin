use rustler::{Atom, Binary, LocalPid, NifResult, OwnedEnv, ResourceArc};
use std::sync::{Mutex, OnceLock};
use tokio::sync::mpsc;
use url::Url;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        moq_connected,
        moq_disconnected,
        moq_frame,
    }
}

// ---------------------------------------------------------------------------
// Shared tokio runtime
// ---------------------------------------------------------------------------

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    })
}

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------

pub struct SessionResource {
    origin: moq_lite::OriginProducer,
    shutdown: Mutex<Option<mpsc::UnboundedSender<()>>>,
}

pub struct BroadcastResource {
    broadcast: Mutex<moq_lite::BroadcastProducer>,
    catalog: Mutex<moq_mux::CatalogProducer>,
}

enum TrackCmd {
    Frame(moq_mux::container::Frame),
    Stop,
}

pub struct TrackResource {
    sender: Mutex<Option<mpsc::UnboundedSender<TrackCmd>>>,
    broadcast: ResourceArc<BroadcastResource>,
    track_name: String,
    kind: TrackRole,
}

#[derive(Clone, Copy)]
enum TrackRole {
    Video,
    Audio,
}

pub struct SubscriberResource {
    tx: mpsc::UnboundedSender<()>,
}

// ---------------------------------------------------------------------------
// Session NIFs
// ---------------------------------------------------------------------------

/// Connect to a MoQ relay server and prepare the session.
///
/// Builds the origin synchronously so subsequent NIFs can publish broadcasts
/// immediately. The QUIC handshake completes asynchronously; `:moq_connected`
/// is sent to `pid` once the session is up. `:moq_disconnected` is sent if the
/// session closes (clean or with an error).
#[rustler::nif]
fn setup_session(
    url: String,
    pid: LocalPid,
) -> NifResult<(Atom, ResourceArc<SessionResource>)> {
    let url = Url::parse(&url).map_err(|e| rustler::Error::Term(Box::new(format!("invalid url: {e}"))))?;

    let origin = moq_lite::Origin::produce();
    let consume = origin.consume();

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let config = {
            let mut tls = moq_native::ClientTls::default();
            tls.disable_verify = Some(true);
            let mut config = moq_native::ClientConfig::default();
            config.tls = tls;
            config
        };

        let client = match config.init() {
            Ok(c) => c.with_publish(consume),
            Err(e) => {
                eprintln!("MoQ client init failed: {e}");
                send_atom(&pid, atoms::moq_disconnected());
                return;
            }
        };

        let session = match client.connect(url).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("MoQ connect failed: {e}");
                send_atom(&pid, atoms::moq_disconnected());
                return;
            }
        };

        send_atom(&pid, atoms::moq_connected());

        tokio::select! {
            _ = shutdown_rx.recv() => {}
            result = session.closed() => {
                if let Err(e) = result {
                    eprintln!("MoQ session closed with error: {e}");
                }
                send_atom(&pid, atoms::moq_disconnected());
            }
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SessionResource {
            origin,
            shutdown: Mutex::new(Some(shutdown_tx)),
        }),
    ))
}

/// Tear down the session, signaling the connection task to exit.
///
/// Idempotent: subsequent calls are no-ops.
#[rustler::nif]
fn close_session(session: ResourceArc<SessionResource>) -> Atom {
    if let Some(tx) = session.shutdown.lock().unwrap().take() {
        let _ = tx.send(());
    }
    atoms::ok()
}

// ---------------------------------------------------------------------------
// Broadcast NIFs
// ---------------------------------------------------------------------------

/// Open a new broadcast on this session and create its (hang + MSF) catalog.
///
/// Path uniqueness is enforced by moq-lite — calling twice with the same path
/// returns an error.
#[rustler::nif]
fn open_broadcast(
    session: ResourceArc<SessionResource>,
    path: String,
) -> NifResult<(Atom, ResourceArc<BroadcastResource>)> {
    // moq-lite's create_broadcast spawns a tokio task to track the
    // broadcast lifetime, so we need a runtime context.
    let _guard = runtime().handle().enter();

    let mut bp = session
        .origin
        .create_broadcast(&path)
        .ok_or_else(|| rustler::Error::Term(Box::new(format!("create_broadcast({path}) refused"))))?;

    let catalog = moq_mux::CatalogProducer::new(&mut bp)
        .map_err(|e| rustler::Error::Term(Box::new(format!("CatalogProducer::new failed: {e}"))))?;

    Ok((
        atoms::ok(),
        ResourceArc::new(BroadcastResource {
            broadcast: Mutex::new(bp),
            catalog: Mutex::new(catalog),
        }),
    ))
}

/// Close the broadcast, aborting any in-flight tracks and unannouncing it.
#[rustler::nif]
fn close_broadcast(broadcast_res: ResourceArc<BroadcastResource>) -> Atom {
    let _guard = runtime().handle().enter();
    let _ = broadcast_res.catalog.lock().unwrap().finish();
    let _ = broadcast_res
        .broadcast
        .lock()
        .unwrap()
        .abort(moq_lite::Error::Cancel);
    atoms::ok()
}

// ---------------------------------------------------------------------------
// Track NIFs
// ---------------------------------------------------------------------------

/// Add an H.264 video track to the broadcast.
///
/// `codec_str` is the WebCodecs codec string, e.g. `"avc1.64001f"` or `"avc3.64001f"`.
/// Returns a TrackResource that frame data should be sent to via `send_frame/4`.
#[rustler::nif]
fn add_h264_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
    codec_str: String,
    width: u32,
    height: u32,
    framerate: f64,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec: hang::catalog::VideoCodec = codec_str
        .parse()
        .map_err(|e| rustler::Error::Term(Box::new(format!("invalid h264 codec '{codec_str}': {e}"))))?;
    add_video_track(broadcast_res, track_name, codec, width, height, framerate)
}

/// Add an H.265 video track to the broadcast.
///
/// `codec_str` is the WebCodecs codec string, e.g. `"hvc1.1.6.L93.B0"`.
#[rustler::nif]
fn add_h265_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
    codec_str: String,
    width: u32,
    height: u32,
    framerate: f64,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec: hang::catalog::VideoCodec = codec_str
        .parse()
        .map_err(|e| rustler::Error::Term(Box::new(format!("invalid h265 codec '{codec_str}': {e}"))))?;
    add_video_track(broadcast_res, track_name, codec, width, height, framerate)
}

/// Add an AAC audio track to the broadcast.
///
/// `profile` follows the AAC profile encoding (e.g. 2 for AAC-LC, 5 for HE-AAC).
#[rustler::nif]
fn add_aac_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
    profile: u8,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::AAC(hang::catalog::AAC { profile });
    add_audio_track(broadcast_res, track_name, codec, sample_rate, channels)
}

/// Add an Opus audio track to the broadcast.
#[rustler::nif]
fn add_opus_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::Opus;
    add_audio_track(broadcast_res, track_name, codec, sample_rate, channels)
}

/// Send a frame to a track.
///
/// `timestamp_us` is the presentation timestamp in microseconds.
/// `keyframe` only matters for video tracks — it triggers a new MoQ group so
/// subscribers can join mid-stream. For audio tracks it should always be true
/// (each audio frame stands alone).
#[rustler::nif]
fn send_frame(
    track: ResourceArc<TrackResource>,
    timestamp_us: u64,
    keyframe: bool,
    data: Binary,
) -> Atom {
    let timestamp = match moq_mux::container::Timestamp::from_micros(timestamp_us) {
        Ok(t) => t,
        Err(_) => {
            eprintln!("send_frame: timestamp overflow ({timestamp_us}us)");
            return atoms::error();
        }
    };

    let frame = moq_mux::container::Frame {
        timestamp,
        payload: bytes::Bytes::copy_from_slice(data.as_slice()),
        keyframe,
    };

    let sender_guard = track.sender.lock().unwrap();
    if let Some(tx) = sender_guard.as_ref() {
        let _ = tx.send(TrackCmd::Frame(frame));
    }
    atoms::ok()
}

/// Close a track: stop its data task, finish the moq-lite track, and remove
/// the rendition from the broadcast catalog. Idempotent.
#[rustler::nif]
fn remove_track(track: ResourceArc<TrackResource>) -> Atom {
    if let Some(tx) = track.sender.lock().unwrap().take() {
        let _ = tx.send(TrackCmd::Stop);
    }

    let mut cp = track.broadcast.catalog.lock().unwrap();
    let mut guard = cp.lock();
    match track.kind {
        TrackRole::Video => {
            guard.video.renditions.remove(&track.track_name);
        }
        TrackRole::Audio => {
            guard.audio.renditions.remove(&track.track_name);
        }
    }

    atoms::ok()
}

// ---------------------------------------------------------------------------
// Subscriber NIFs (Source) — TODO
// ---------------------------------------------------------------------------

#[rustler::nif]
fn start_subscriber(
    _url: String,
    _broadcast: String,
    _track: String,
    pid: LocalPid,
) -> (Atom, ResourceArc<SubscriberResource>) {
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let _ = stop_rx.recv().await;
        send_atom(&pid, atoms::moq_disconnected());
    });

    (
        atoms::ok(),
        ResourceArc::new(SubscriberResource { tx: stop_tx }),
    )
}

#[rustler::nif]
fn stop_subscriber(resource: ResourceArc<SubscriberResource>) -> Atom {
    let _ = resource.tx.send(());
    atoms::ok()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn send_atom(pid: &LocalPid, atom: Atom) {
    let mut owned = OwnedEnv::new();
    owned.send_and_clear(pid, |env| atom.to_term(env));
}

fn add_video_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
    codec: hang::catalog::VideoCodec,
    width: u32,
    height: u32,
    framerate: f64,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let track = {
        let mut bp = broadcast_res.broadcast.lock().unwrap();
        bp.create_track(moq_lite::Track {
            name: track_name.clone(),
            priority: 0,
        })
        .map_err(|e| rustler::Error::Term(Box::new(format!("create_track failed: {e}"))))?
    };

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        guard.video.renditions.insert(
            track_name.clone(),
            hang::catalog::VideoConfig {
                codec,
                description: None,
                coded_width: Some(width),
                coded_height: Some(height),
                display_ratio_width: None,
                display_ratio_height: None,
                bitrate: None,
                framerate: Some(framerate),
                optimize_for_latency: Some(true),
                container: hang::catalog::Container::Legacy,
                jitter: None,
            },
        );
    }

    let sender = spawn_track_task(track);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            sender: Mutex::new(Some(sender)),
            broadcast: broadcast_res,
            track_name,
            kind: TrackRole::Video,
        }),
    ))
}

fn add_audio_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
    codec: hang::catalog::AudioCodec,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let track = {
        let mut bp = broadcast_res.broadcast.lock().unwrap();
        bp.create_track(moq_lite::Track {
            name: track_name.clone(),
            priority: 0,
        })
        .map_err(|e| rustler::Error::Term(Box::new(format!("create_track failed: {e}"))))?
    };

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        guard.audio.renditions.insert(
            track_name.clone(),
            hang::catalog::AudioConfig {
                codec,
                sample_rate,
                channel_count: channels,
                bitrate: None,
                description: None,
                container: hang::catalog::Container::Legacy,
                jitter: None,
            },
        );
    }

    let sender = spawn_track_task(track);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            sender: Mutex::new(Some(sender)),
            broadcast: broadcast_res,
            track_name,
            kind: TrackRole::Audio,
        }),
    ))
}

/// Spawn the per-track data task: owns the Legacy-container Producer, drains
/// the mpsc, writes each frame. Exits when the channel closes (track removed)
/// or after a TrackCmd::Stop.
fn spawn_track_task(track: moq_lite::TrackProducer) -> mpsc::UnboundedSender<TrackCmd> {
    let (tx, mut rx) = mpsc::unbounded_channel::<TrackCmd>();
    runtime().spawn(async move {
        let mut producer = moq_mux::ordered::Producer::new(track, moq_mux::hang::Legacy);
        while let Some(cmd) = rx.recv().await {
            match cmd {
                TrackCmd::Frame(frame) => {
                    if let Err(e) = producer.write(frame) {
                        eprintln!("track write failed: {e}");
                        break;
                    }
                }
                TrackCmd::Stop => break,
            }
        }
        let _ = producer.finish();
    });
    tx
}

// ---------------------------------------------------------------------------
// NIF init
// ---------------------------------------------------------------------------

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    rustler::resource!(SessionResource, env);
    rustler::resource!(BroadcastResource, env);
    rustler::resource!(TrackResource, env);
    rustler::resource!(SubscriberResource, env);
    true
}

rustler::init!(
    "Elixir.Membrane.MoQ.Native",
    [
        setup_session,
        close_session,
        open_broadcast,
        close_broadcast,
        add_h264_track,
        add_h265_track,
        add_aac_track,
        add_opus_track,
        send_frame,
        remove_track,
        start_subscriber,
        stop_subscriber
    ],
    load = load
);
