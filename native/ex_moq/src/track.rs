use hang::moq_net;

use rustler::{Atom, Binary, NifResult, Resource, ResourceArc};
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use crate::{
    atoms,
    broadcast_producer::BroadcastProducerResource,
    lock_ignoring_poison, runtime,
    track_format::{Container, ResolvedConfig, TrackFormat},
};

pub(crate) type WireProducer = moq_mux::container::Producer<moq_mux::catalog::hang::Container>;

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

struct LiveTrack {
    producer: Arc<Mutex<WireProducer>>,
    rendition: Rendition,
    broadcast: moq_net::broadcast::Producer,
}

pub(crate) struct TrackResource {
    live: Mutex<Option<LiveTrack>>,
    name: String,
    container: hang::catalog::Container,
}

impl Resource for TrackResource {}

impl TrackResource {
    fn teardown(&self) {
        let Some(live) = lock_ignoring_poison(&self.live).take() else {
            return;
        };

        let LiveTrack {
            producer,
            rendition,
            mut broadcast,
        } = live;

        let _ = lock_ignoring_poison(&producer).finish();
        let _ = broadcast.remove_track(&self.name);
        // Retires the catalog rendition.
        drop(rendition);
    }
}

impl Drop for TrackResource {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = runtime().handle().enter();
            self.teardown();
        }));
    }
}

#[rustler::nif]
pub(crate) fn add_track(
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    track: String,
    format: TrackFormat,
    priority: u8,
    container: Container,
    latency_ns: u64,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let catalog_container = hang::catalog::Container::from(container);
    let wire = (&catalog_container)
        .try_into()
        .map_err(|e| crate::nif_error!("container init failed: {e}"))?;

    let resolved = ResolvedConfig::new(format, catalog_container.clone())?;

    let mut inner = broadcast_res.inner.lock().map_err(|_poison| {
        crate::nif_error!("broadcast producer poisoned by panic in an earlier NIF call")
    })?;

    let track_producer = inner
        .broadcast
        .create_track(
            track,
            moq_net::track::Info::default().with_priority(priority),
        )
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?;

    let name = track_producer.name().to_string();

    let rendition = Rendition::new(&inner.catalog, &name, resolved);

    let latency = std::time::Duration::from_nanos(latency_ns);
    let producer = Arc::new(Mutex::new(
        inner
            .catalog
            .media_producer(track_producer, wire)
            .map_err(|e| crate::nif_error!("media_producer failed: {e}"))?
            .with_latency(latency),
    ));

    inner.tracks.retain(|weak| weak.strong_count() > 0);
    inner.tracks.push(Arc::downgrade(&producer));
    let broadcast = inner.broadcast.clone();
    drop(inner);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            live: Mutex::new(Some(LiveTrack {
                producer,
                rendition,
                broadcast,
            })),
            name,
            container: catalog_container,
        }),
    ))
}

#[rustler::nif]
pub(crate) fn update_track(
    track_res: ResourceArc<TrackResource>,
    format: TrackFormat,
) -> NifResult<Atom> {
    let _guard = runtime().handle().enter();

    let resolved = ResolvedConfig::new(format, track_res.container.clone())?;

    let mut live = track_res
        .live
        .lock()
        .map_err(|_poison| crate::nif_error!("track poisoned by panic in an earlier NIF call"))?;

    let Some(live) = live.as_mut() else {
        return Err(crate::nif_error!(
            "track {:?} was removed; a stale resource cannot update the catalog",
            track_res.name
        ));
    };

    live.rendition.set(resolved).map_err(|_kind_mismatch| {
        crate::nif_error!(
            "track {:?}: cannot change a track's media kind in place",
            track_res.name
        )
    })?;

    Ok(atoms::ok())
}

#[rustler::nif]
pub(crate) fn send_frame(
    track_res: ResourceArc<TrackResource>,
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

    let _guard = runtime().handle().enter();

    let live = track_res.live.lock().map_err(|_poiosn| {
        crate::nif_error!("track resource poisoned by panic in an earlier NIF call")
    })?;

    let Some(live) = live.as_ref() else {
        return Err(crate::nif_error!("track {:?} was removed", track_res.name));
    };

    let result = live
        .producer
        .lock()
        .map_err(|_poison| {
            crate::nif_error!("track producer poisoned by panic in an earlier nif call")
        })?
        .write(frame);

    match result {
        Ok(()) => Ok(atoms::ok()),
        Err(moq_mux::Error::MissingKeyframe(moq_mux::container::MissingKeyframe)) => {
            Ok(atoms::moq_missing_keyframe())
        }
        Err(e) => Err(crate::nif_error!(
            "writing frame for track {0} failed: {1}",
            track_res.name,
            e
        )),
    }
}

#[rustler::nif]
pub(crate) fn remove_track(track_res: ResourceArc<TrackResource>) -> Atom {
    let _guard = runtime().handle().enter();
    track_res.teardown();
    atoms::ok()
}
