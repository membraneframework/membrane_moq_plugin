use rustler::{Atom, Binary, LocalPid, OwnedEnv, ResourceArc};
use std::sync::OnceLock;
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
// Resource types
// ---------------------------------------------------------------------------

enum PublisherCmd {
    Configure {
        codec: String,
        width: u32,
        height: u32,
        framerate: f64,
    },
    Frame {
        timestamp_us: u64,
        keyframe: bool,
        data: Vec<u8>,
    },
    Stop,
}

pub struct PublisherResource {
    tx: mpsc::UnboundedSender<PublisherCmd>,
}

pub struct SubscriberResource {
    tx: mpsc::UnboundedSender<()>,
}

// ---------------------------------------------------------------------------
// Publisher NIFs (Sink)
// ---------------------------------------------------------------------------

/// Connect to a MoQ relay server and prepare a broadcast.
///
/// Establishes the QUIC session and sends `:moq_connected` to `pid` once ready.
/// Call `configure_publisher/5` afterwards (once codec parameters are known) to
/// publish the catalog and open the video track.
#[rustler::nif]
fn setup_publisher(
    url: String,
    broadcast: String,
    pid: LocalPid,
) -> (Atom, ResourceArc<PublisherResource>) {
    let config = {
        let mut tls = moq_native::ClientTls::default();
        tls.disable_verify = Some(true);
        let mut config = moq_native::ClientConfig::default();
        config.tls = tls;
        config
    };

    let url = Url::parse(&url).expect("failed parsing url");
    let (tx, mut rx) = mpsc::unbounded_channel::<PublisherCmd>();

    runtime().spawn(async move {
        // --- Create origin + broadcast producer ---
        let origin = moq_lite::Origin::produce();
        let mut bp = origin.create_broadcast(&broadcast).unwrap();

        // --- Connect ---
        // with_publish: relay pulls our content (publisher role)
        // with_consume: relay pushes announced broadcasts to us (subscriber role)
        let client = config
            .init()
            .expect("failed creating client")
            .with_publish(origin.consume());

        let session = client.connect(url).await.expect("failed connecting");

        let mut owned = OwnedEnv::new();
        owned.send_and_clear(&pid, |env| atoms::moq_connected().to_term(env));

        // --- Wait for codec configuration ---
        let (codec, width, height, framerate) = loop {
            match rx.recv().await {
                Some(PublisherCmd::Configure {
                    codec,
                    width,
                    height,
                    framerate,
                }) => {
                    break (codec, width, height, framerate);
                }
                Some(PublisherCmd::Stop) | None => return,
                Some(PublisherCmd::Frame { .. }) => {
                    // frames before configure are dropped
                    println!("!!! frames received before Configure cmd");
                }
            }
        };

        // --- Catalog track ---
        let mut catalog_track = bp
            .create_track(hang::Catalog::default_track())
            .expect("failed creating catalog track");

        let video_codec: hang::catalog::VideoCodec =
            codec.parse().expect("failed parsing codec string");

        let mut renditions = std::collections::BTreeMap::new();
        renditions.insert(
            "video".to_string(),
            hang::catalog::VideoConfig {
                codec: video_codec,
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

        let catalog = hang::Catalog {
            video: hang::catalog::Video {
                renditions,
                display: None,
                rotation: None,
                flip: None,
            },
            ..Default::default()
        };

        let catalog_json = catalog.to_string().expect("failed serializing catalog");
        println!("catalog_json {}", catalog_json);

        let mut catalog_group = catalog_track
            .append_group()
            .expect("failed creating catalog group");
        catalog_group
            .write_frame(bytes::Bytes::from(catalog_json))
            .expect("failed writing catalog frame");

        // --- Video track ---
        let video_track = bp
            .create_track(moq_lite::Track {
                name: "video".to_string(),
                priority: 0,
            })
            .expect("failed creating video track");
        let mut producer = hang::container::OrderedProducer::new(video_track);

        // --- Frame loop ---
        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(PublisherCmd::Frame { timestamp_us, keyframe, data }) => {
                        if keyframe {
                            producer.keyframe().expect("failed closing group on keyframe");
                        }
                        let frame = hang::container::Frame {
                            timestamp: hang::container::Timestamp::from_micros(timestamp_us)
                                .expect("timestamp overflow"),
                            payload: hang::container::BufList::from_iter([
                                bytes::Bytes::from(data)
                            ]),
                        };
                        producer.write(frame).expect("failed writing frame");
                    }
                    Some(PublisherCmd::Stop) | None => break,
                    Some(PublisherCmd::Configure { .. }) => {
                        // already configured, ignore
                    }
                },
                result = session.closed() => {
                    result.expect("session error");
                    break;
                }
            }
        }

        producer.finish().expect("failed finishing track");
    });

    (atoms::ok(), ResourceArc::new(PublisherResource { tx }))
}

/// Publish the hang catalog and open the video track.
///
/// Must be called after `setup_publisher/3` has delivered `:moq_connected`.
/// `codec` is a WebCodecs codec string, e.g. `"avc1.64001f"`.
#[rustler::nif]
fn configure_publisher(
    resource: ResourceArc<PublisherResource>,
    codec: String,
    width: u32,
    height: u32,
    framerate: f64,
) -> Atom {
    let _ = resource.tx.send(PublisherCmd::Configure {
        codec,
        width,
        height,
        framerate,
    });
    atoms::ok()
}

/// Send an H.264 frame to the relay.
///
/// `timestamp_us` is the presentation timestamp in microseconds.
/// `keyframe` must be `true` for IDR frames — this closes the current MoQ
/// group and starts a new one, ensuring independent decodability per group.
#[rustler::nif]
fn send_segment(
    resource: ResourceArc<PublisherResource>,
    timestamp_us: u64,
    keyframe: bool,
    data: Binary,
) -> Atom {
    let _ = resource.tx.send(PublisherCmd::Frame {
        timestamp_us,
        keyframe,
        data: data.as_slice().to_vec(),
    });
    atoms::ok()
}

/// Signal the publisher task to stop and close the relay session.
#[rustler::nif]
fn stop_publisher(resource: ResourceArc<PublisherResource>) -> Atom {
    let _ = resource.tx.send(PublisherCmd::Stop);
    atoms::ok()
}

// ---------------------------------------------------------------------------
// Subscriber NIFs (Source) — TODO
// ---------------------------------------------------------------------------

#[rustler::nif]
fn start_subscriber(
    url: String,
    broadcast: String,
    _track: String,
    pid: LocalPid,
) -> (Atom, ResourceArc<SubscriberResource>) {
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let _ = stop_rx.recv().await;
        let mut owned = OwnedEnv::new();
        owned.send_and_clear(&pid, |env| atoms::moq_disconnected().to_term(env));
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
// NIF init
// ---------------------------------------------------------------------------

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    rustler::resource!(PublisherResource, env);
    rustler::resource!(SubscriberResource, env);
    true
}

rustler::init!(
    "Elixir.Membrane.MoQ.Native",
    [
        setup_publisher,
        configure_publisher,
        send_segment,
        stop_publisher,
        start_subscriber,
        stop_subscriber
    ],
    load = load
);
