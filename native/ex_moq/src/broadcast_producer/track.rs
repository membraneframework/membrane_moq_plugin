use hang::moq_net;

use rustler::{Atom, Binary, NifResult, ResourceArc};

use super::BroadcastProducerResource;
use crate::{
    atoms,
    track_format::{Container, ResolvedConfig, TrackFormat},
};

type WireProducer = moq_mux::container::Producer<moq_mux::catalog::hang::Container>;

enum Rendition {
    Video(moq_mux::catalog::VideoTrack),
    Audio(moq_mux::catalog::AudioTrack),
}

struct KindMismatch;

impl Rendition {
    fn new(catalog: &moq_mux::catalog::Producer, name: &str, config: ResolvedConfig) -> Self {
        match config {
            ResolvedConfig::Video(config) => {
                let mut handle = catalog.reserve().video(name);
                handle.set(config);
                Self::Video(handle)
            }
            ResolvedConfig::Audio(config) => {
                let mut handle = catalog.reserve().audio(name);
                handle.set(config);
                Self::Audio(handle)
            }
        }
    }

    fn set(&mut self, config: ResolvedConfig) -> Result<(), KindMismatch> {
        match (self, config) {
            (Self::Video(handle), ResolvedConfig::Video(config)) => handle.set(config),
            (Self::Audio(handle), ResolvedConfig::Audio(config)) => handle.set(config),
            (_, _) => return Err(KindMismatch),
        }
        Ok(())
    }
}

pub(super) struct LiveTrack {
    pub(super) producer: WireProducer,
    rendition: Rendition,
    container: hang::catalog::Container,
}

#[rustler::nif]
pub(crate) fn add_track(
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    track: String,
    format: TrackFormat,
    priority: u8,
    container: Container,
    latency_ns: u64,
) -> NifResult<Atom> {
    let catalog_container = hang::catalog::Container::from(container);
    let wire_container = (&catalog_container)
        .try_into()
        .map_err(|e| crate::nif_error!("container init failed: {e}"))?;

    let resolved = ResolvedConfig::new(format, catalog_container.clone())?;

    let mut inner = broadcast_res
        .0
        .lock()
        .map_err(|_poison| crate::nif_error!(atoms::moq_producer_poisoned()))?;

    if inner.tracks.contains_key(&track) {
        return Err(crate::nif_error!(atoms::moq_track_already_exists()));
    }

    let track_producer = inner
        .broadcast
        .create_track(
            track.as_str(),
            moq_net::track::Info::default().with_priority(priority),
        )
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?;

    let rendition = Rendition::new(&inner.catalog, &track, resolved);

    let latency = std::time::Duration::from_nanos(latency_ns);
    let producer = inner
        .catalog
        .media_producer(track_producer, wire_container)
        .map_err(|e| crate::nif_error!("media_producer failed: {e}"))?
        .with_latency(latency);

    inner.tracks.insert(
        track,
        LiveTrack {
            producer,
            rendition,
            container: catalog_container,
        },
    );

    Ok(atoms::ok())
}

#[rustler::nif]
pub(crate) fn update_track(
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    track: &str,
    format: TrackFormat,
) -> NifResult<Atom> {
    let mut inner = broadcast_res
        .0
        .lock()
        .map_err(|_poison| crate::nif_error!(atoms::moq_producer_poisoned()))?;

    let Some(live) = inner.tracks.get_mut(track) else {
        return Err(crate::nif_error!(atoms::moq_unknown_track()));
    };

    let resolved = ResolvedConfig::new(format, live.container.clone())?;

    live.rendition.set(resolved).map_err(|_kind_mismatch| {
        crate::nif_error!("cannot change a track's media kind in place")
    })?;

    Ok(atoms::ok())
}

#[rustler::nif]
pub(crate) fn send_frame(
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    track: &str,
    timestamp_ns: u64,
    keyframe: bool,
    data: Binary,
) -> NifResult<Atom> {
    let timestamp = moq_net::Timestamp::from_nanos(timestamp_ns)
        .map_err(|e| crate::nif_error!("timestamp conversion failed: {e}"))?;

    let frame = moq_mux::container::Frame {
        timestamp,
        payload: bytes::Bytes::copy_from_slice(data.as_slice()),
        keyframe,
        duration: None,
    };

    let mut inner = broadcast_res
        .0
        .lock()
        .map_err(|_poison| crate::nif_error!(atoms::moq_producer_poisoned()))?;

    let Some(live) = inner.tracks.get_mut(track) else {
        return Err(crate::nif_error!(atoms::moq_unknown_track()));
    };

    match live.producer.write(frame) {
        Ok(()) => Ok(atoms::ok()),
        Err(moq_mux::Error::MissingKeyframe(moq_mux::container::MissingKeyframe)) => {
            Ok(atoms::moq_missing_keyframe())
        }
        Err(e) => Err(crate::nif_error!("writing frame failed: {e}")),
    }
}

#[rustler::nif]
pub(crate) fn remove_track(
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    track: &str,
) -> Atom {
    let (mut inner, poisoned) = match broadcast_res.0.lock() {
        Ok(guard) => (guard, false),
        Err(poison) => (poison.into_inner(), true),
    };

    let Some(mut live) = inner.tracks.remove(track) else {
        return atoms::ok();
    };

    if !poisoned {
        let _ = live.producer.finish();
    }
    let _ = inner.broadcast.remove_track(track);

    atoms::ok()
}
