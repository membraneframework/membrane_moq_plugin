use rustler::{Atom, Binary, LocalPid, OwnedEnv, Reference, ResourceArc};
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
// Shared tokio runtime (created once, reused across all NIF calls)
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
// Resource types (hold a channel sender to control the background task)
// ---------------------------------------------------------------------------

enum PublisherCmd {
    Frame { track_id: u32, data: Vec<u8> },
    AddTrack { track_id: u32 },
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

/// Start a MoQ publisher session in a background tokio task.
///
/// Once the QUIC handshake with the relay completes, sends `:moq_connected`
/// to `pid`. Frames queued via `publish_frame/2` are forwarded to the relay
/// as individual MoQ groups (one frame per group).
#[rustler::nif]
fn start_publisher(
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
        let origin = moq_lite::Origin::produce();
        let mut bp = origin.create_broadcast(broadcast).unwrap();

        let client = config
            .init()
            .expect("failed creating client")
            .with_consume(origin);

        let session = client.connect(url).await.expect("failed connecting client");

        let mut owned = OwnedEnv::new();
        // send `:moq_connected` to `Membrane.MoQ.Sink` to complete setup
        owned.send_and_clear(&pid, |env| atoms::moq_connected().to_term(env));

        let mut tracks: std::collections::HashMap<u32, moq_lite::TrackProducer> =
            std::collections::HashMap::new();

        let catalog_producer = bp
            .create_track(moq_lite::Track::new("catalog"))
            .expect("failed creating catalog track");

        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(PublisherCmd::AddTrack { track_id }) => {
                        let tp = bp.create_track(moq_lite::Track::new(&track_id.to_string()))
                            .expect("failed creating track");
                        tracks.insert(track_id, tp);
                    }
                    Some(PublisherCmd::Frame { track_id, data }) => {
                        if let Some(tp) = tracks.get_mut(&track_id) {
                            let mut gp = tp.append_group().expect("failed appending group");
                            gp.write_frame(bytes::Bytes::from(data)).expect("failed writing frame");
                        }
                    }
                    Some(PublisherCmd::Stop) | None => break,
                },
                result = session.closed() => {
                    result.expect("session error");
                    break;
                }
            }
        }
    });

    (atoms::ok(), ResourceArc::new(PublisherResource { tx }))
}

/// Enqueue a binary payload to be sent as a MoQ frame on the named track.
#[rustler::nif]
fn send_segment(resource: ResourceArc<PublisherResource>, track: Reference, data: Binary) -> Atom {
    let _ = resource.tx.send(PublisherCmd::Frame {
        track_id: track.hash_phash2(),
        data: data.as_slice().to_vec(),
    });
    atoms::ok()
}

/// Register a new track on the broadcast.
#[rustler::nif]
fn add_track(resource: ResourceArc<PublisherResource>, name: Reference, header: Binary) -> Atom {
    println!("add_track called dupa");
    let _ = resource.tx.send(PublisherCmd::AddTrack {
        track_id: name.hash_phash2(),
    });
    atoms::ok()
}

/// Signal the publisher task to finish and close the relay session.
#[rustler::nif]
fn stop_publisher(resource: ResourceArc<PublisherResource>) -> Atom {
    let _ = resource.tx.send(PublisherCmd::Stop);
    atoms::ok()
}

// ---------------------------------------------------------------------------
// Subscriber NIFs (Source)
// ---------------------------------------------------------------------------

/// Start a MoQ subscriber session in a background tokio task.
///
/// Connects to the relay, subscribes to `track` within `broadcast`, and
/// forwards each received frame to `pid` as `{:moq_frame, binary}`.
/// Sends `:moq_disconnected` when the subscription ends or the relay
/// closes the connection.
#[rustler::nif]
fn start_subscriber(
    url: String,
    broadcast: String,
    track: String,
    pid: LocalPid,
) -> (Atom, ResourceArc<SubscriberResource>) {
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        // TODO: Establish a native QUIC/WebTransport connection to `url`.
        //
        //   let quic_session = moq_native::Session::connect(&url).await?;
        //
        //   let (origin_producer, origin_consumer) = moq_lite::Origin::produce();
        //
        //   moq_lite::Client::new()
        //       .with_consume(origin_producer)
        //       .connect(quic_session)
        //       .await
        //       .expect("MoQ handshake failed");
        //
        //   let broadcast_consumer = origin_consumer
        //       .subscribe(&broadcast)
        //       .expect("failed to subscribe to broadcast");
        //   let track_info = moq_lite::Track::new(&track);
        //   let mut track_consumer = broadcast_consumer
        //       .subscribe_track(&track_info)
        //       .expect("failed to subscribe to track");
        //
        //   loop {
        //       tokio::select! {
        //           result = track_consumer.next_group() => {
        //               let Some(mut group) = result else { break };
        //               while let Some(frame) = group.read_frame().await {
        //                   let payload = frame.payload.to_vec();
        //                   let owned = OwnedEnv::new();
        //                   owned.send_and_clear(&pid, |env| {
        //                       (atoms::moq_frame(), payload.as_slice()).encode(env)
        //                   });
        //               }
        //           }
        //           _ = stop_rx.recv() => break,
        //       }
        //   }

        let _ = stop_rx.recv().await;

        let mut owned = OwnedEnv::new();
        owned.send_and_clear(&pid, |env| atoms::moq_disconnected().to_term(env));
    });

    (
        atoms::ok(),
        ResourceArc::new(SubscriberResource { tx: stop_tx }),
    )
}

/// Signal the subscriber task to stop receiving frames and close the session.
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

rustler::init!("Elixir.Membrane.MoQ.Native", load = load);
