use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use hang::moq_net;

use rustler::types::atom::ok;
use rustler::{Atom, Binary, LocalPid, NifResult, Resource, ResourceArc};

use url::Url;

mod broadcast_consumer;
mod broadcast_producer;
mod messages;
mod session;
mod track_format;
mod web_codecs;

use broadcast_producer::{AddTrackError, UpdateTrackError, WriteFrameError};
use messages::Token;
use track_format::{Container, TrackFormat};

macro_rules! nif_error {
    ($fmt:literal $($arg:tt)*) => {
        rustler::Error::Term(Box::new(format!($fmt $($arg)*)))
    };
    ($term:expr) => {
        rustler::Error::Term(Box::new($term))
    };
}

pub(crate) use nif_error;

pub(crate) mod atoms {
    rustler::atoms! {
        // tagged atoms used in messages
        moq_connected,
        moq_broadcast_ready,
        moq_frame,
        moq_catalog,
        moq_track_finished,
        moq_broadcast_closed,
        moq_setup_failed,
        moq_disconnected,
        moq_track_error,

        // atoms for synchronous error reports
        missing_keyframe,
        producer_poisoned,
        consumer_closed,
        opus,
        unrecognized,
        track_already_exists,
        unknown_track,
        kind_mismatch,
    }
}

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime build should succeed")
    })
}

struct SessionResource(session::Session);

#[rustler::resource_impl]
impl Resource for SessionResource {}

struct BroadcastProducerResource(Mutex<broadcast_producer::Producer>);

#[rustler::resource_impl]
impl Resource for BroadcastProducerResource {}

struct BroadcastConsumerResource(broadcast_consumer::Handle);

#[rustler::resource_impl]
impl Resource for BroadcastConsumerResource {}

#[rustler::nif]
fn create_session(
    url: &str,
    pid: LocalPid,
    disable_tls_verify: bool,
) -> NifResult<(Atom, ResourceArc<SessionResource>)> {
    let url = Url::parse(url).map_err(|e| nif_error!("invalid url: {e}"))?;

    let session = session::Session::connect(url, pid, disable_tls_verify);
    Ok((ok(), ResourceArc::new(SessionResource(session))))
}

#[rustler::nif]
fn close_session(session: ResourceArc<SessionResource>) -> Atom {
    session.0.close();
    ok()
}

#[rustler::nif]
fn create_broadcast_producer(
    session: ResourceArc<SessionResource>,
    path: &str,
) -> NifResult<(Atom, ResourceArc<BroadcastProducerResource>)> {
    let producer =
        broadcast_producer::Producer::new(&session.0, path).map_err(|e| nif_error!("{e}"))?;

    Ok((
        ok(),
        ResourceArc::new(BroadcastProducerResource(Mutex::new(producer))),
    ))
}

#[rustler::nif]
fn close_broadcast_producer(producer: ResourceArc<BroadcastProducerResource>) -> Atom {
    let locked = producer.0.lock();
    let poisoned = locked.is_err();
    let mut producer = locked.unwrap_or_else(std::sync::PoisonError::into_inner);

    if !poisoned {
        // Flush frames inside the producers' internal cache.
        // We don't do this for a poisoned resource to avoid touching possibly inconsistent state.
        producer.finish();
    }

    producer.abort();
    ok()
}

#[rustler::nif]
fn add_track(
    producer: ResourceArc<BroadcastProducerResource>,
    track: String,
    format: TrackFormat,
    priority: u8,
    container: Container,
    latency_ns: u64,
) -> NifResult<Atom> {
    let latency = Duration::from_nanos(latency_ns);

    lock_producer(&producer)?
        .add_track(track, format, container, priority, latency)
        .map_err(|e| match e {
            AddTrackError::AlreadyExists => nif_error!(atoms::track_already_exists()),
            other => nif_error!("{other}"),
        })?;

    Ok(ok())
}

#[rustler::nif]
fn update_track(
    producer: ResourceArc<BroadcastProducerResource>,
    track: &str,
    format: TrackFormat,
) -> NifResult<Atom> {
    lock_producer(&producer)?
        .update_track(track, format)
        .map_err(|e| match e {
            UpdateTrackError::UnknownTrack => nif_error!(atoms::unknown_track()),
            UpdateTrackError::KindMismatch => nif_error!(atoms::kind_mismatch()),
        })?;

    Ok(ok())
}

#[rustler::nif]
fn send_frame(
    producer: ResourceArc<BroadcastProducerResource>,
    track: &str,
    timestamp_ns: u64,
    keyframe: bool,
    data: Binary,
) -> NifResult<Atom> {
    let timestamp = moq_net::Timestamp::from_nanos(timestamp_ns)
        .map_err(|e| nif_error!("timestamp conversion failed: {e}"))?;

    let frame = moq_mux::container::Frame {
        timestamp,
        payload: bytes::Bytes::copy_from_slice(data.as_slice()),
        keyframe,
        duration: None,
    };

    match lock_producer(&producer)?.write_frame(track, frame) {
        Ok(()) => Ok(ok()),
        Err(WriteFrameError::MissingKeyframe) => Ok(atoms::missing_keyframe()),
        Err(WriteFrameError::UnknownTrack) => Err(nif_error!(atoms::unknown_track())),
        Err(e) => Err(nif_error!("{e}")),
    }
}

#[rustler::nif]
fn remove_track(producer: ResourceArc<BroadcastProducerResource>, track: &str) -> NifResult<Atom> {
    lock_producer(&producer)?.remove_track(track);
    Ok(ok())
}

#[rustler::nif]
fn create_broadcast_consumer(
    session: ResourceArc<SessionResource>,
    path: String,
    pid: LocalPid,
    latency_ns: u64,
) -> (Atom, ResourceArc<BroadcastConsumerResource>) {
    let latency = Duration::from_nanos(latency_ns);

    let consumer = broadcast_consumer::spawn(&session.0, path, pid, latency);
    (ok(), ResourceArc::new(BroadcastConsumerResource(consumer)))
}

#[rustler::nif]
fn subscribe_track(
    consumer: ResourceArc<BroadcastConsumerResource>,
    track: String,
    token: Token,
    priority: u8,
) -> NifResult<Atom> {
    consumer
        .0
        .subscribe(track, token, priority)
        .map_err(|_closed| nif_error!(atoms::consumer_closed()))?;

    Ok(ok())
}

#[rustler::nif]
fn unsubscribe_track(consumer: ResourceArc<BroadcastConsumerResource>, token: Token) -> Atom {
    consumer.0.unsubscribe(token);
    ok()
}

#[rustler::nif]
fn close_broadcast_consumer(consumer: ResourceArc<BroadcastConsumerResource>) -> Atom {
    consumer.0.close();
    ok()
}

fn lock_producer(
    resource: &BroadcastProducerResource,
) -> NifResult<MutexGuard<'_, broadcast_producer::Producer>> {
    resource
        .0
        .lock()
        .map_err(|_poison| nif_error!(atoms::producer_poisoned()))
}

rustler::init!("Elixir.ExMoQ.Native");
