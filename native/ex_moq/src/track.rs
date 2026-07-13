use hang::moq_net;

use rustler::{Atom, Binary, NifResult, Resource, ResourceArc};
use std::sync::{Arc, Mutex, Weak};

use crate::{
    atoms,
    broadcast_producer::{BroadcastProducerResource, ProducerInner},
    lock_ignoring_poison, runtime,
    track_format::{ContainerKind, ResolvedConfig, TrackFormat, TrackKind},
};

/// Producer over the runtime-dispatched container enum,
/// so one resource type covers every wire format the catalog can describe.
pub(crate) type WireProducer = moq_mux::container::Producer<moq_mux::catalog::hang::Container>;

pub(crate) struct TrackResource {
    producer: Arc<Mutex<WireProducer>>,
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    name: String,
    kind: TrackKind,
    settings: TrackSettings,
}

/// Wire/delivery parameters a track keeps for its lifetime; a replacement
/// rendition created on a mid-stream format change inherits them.
#[derive(Clone)]
struct TrackSettings {
    // Used to generate new, unique track names if format changes mid-stream.
    // For the first rendition (before any stream format change), suffix == name
    suffix: String,
    priority: u8,
    container: ContainerKind,
    latency: std::time::Duration,
}

impl Resource for TrackResource {}

impl TrackResource {
    fn teardown(&self) {
        let mut inner = lock_ignoring_poison(&self.broadcast_res.inner);

        let _ = lock_ignoring_poison(&self.producer).finish();

        let owns_name = inner
            .tracks
            .get(&self.name)
            .is_some_and(|weak| Weak::ptr_eq(weak, &Arc::downgrade(&self.producer)));

        if !owns_name {
            return;
        }

        inner.tracks.remove(&self.name);
        let _ = inner.broadcast.remove_track(&self.name);

        let mut guard = inner.catalog.lock();
        match self.kind {
            TrackKind::Video => {
                guard.video.renditions.remove(&self.name);
            }
            TrackKind::Audio => {
                guard.audio.renditions.remove(&self.name);
            }
        }
    }
}

impl Drop for TrackResource {
    fn drop(&mut self) {
        let _guard = runtime().handle().enter();
        self.teardown();
    }
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn add_track(
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    track: String,
    format: TrackFormat,
    priority: u8,
    container: ContainerKind,
    latency_ns: u64,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let resolved = format.resolve()?;

    let mut inner = lock_ignoring_poison(&broadcast_res.inner);

    let tp = inner
        .broadcast
        .create_track(moq_net::Track {
            name: track,
            priority,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?;

    let settings = TrackSettings {
        suffix: tp.name.clone(),
        priority,
        container,
        latency: std::time::Duration::from_nanos(latency_ns),
    };
    let resource = init_track(
        tp,
        resolved,
        &mut inner,
        broadcast_res.clone(),
        settings,
        None,
    )?;

    Ok((atoms::ok(), ResourceArc::new(resource)))
}

/// Replace a live track with one carrying a new format, published on a brand-new moq track.
/// Returns a new track resource through which all subsequent frames must be sent.
#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn replace_track(
    old_track_res: ResourceArc<TrackResource>,
    format: TrackFormat,
) -> NifResult<(Atom, ResourceArc<TrackResource>, String)> {
    let _guard = runtime().handle().enter();

    let resolved = format.resolve()?;

    if resolved.kind() != old_track_res.kind {
        return Err(crate::nif_error!(
            "cannot change a track's media kind in place"
        ));
    }

    let broadcast_res = old_track_res.broadcast_res.clone();
    // The new rendition keeps the replaced track's settings.
    let settings = old_track_res.settings.clone();

    let mut inner = lock_ignoring_poison(&broadcast_res.inner);

    let name = inner.broadcast.unique_name(&settings.suffix);
    let tp = inner
        .broadcast
        .create_track(moq_net::Track {
            name,
            priority: settings.priority,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?;

    let resource = init_track(
        tp,
        resolved,
        &mut inner,
        broadcast_res.clone(),
        settings,
        Some(&old_track_res.name),
    )?;

    let _ = lock_ignoring_poison(&old_track_res.producer).finish();

    let name = resource.name.clone();
    Ok((atoms::ok(), ResourceArc::new(resource), name))
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn send_frame(
    track_res: ResourceArc<TrackResource>,
    timestamp_ns: u64,
    keyframe: bool,
    data: Binary,
) -> NifResult<Atom> {
    let timestamp = moq_mux::container::Timestamp::from_nanos(timestamp_ns)
        .map_err(|e| crate::nif_error!("timestamp conversion failed: {e}"))?;
    let frame = moq_mux::container::Frame {
        timestamp,
        payload: bytes::Bytes::copy_from_slice(data.as_slice()),
        keyframe,
        duration: None,
    };

    let _guard = runtime().handle().enter();
    match lock_ignoring_poison(&track_res.producer).write(frame) {
        Ok(()) => Ok(atoms::ok()),
        Err(moq_mux::Error::MissingKeyframe(moq_mux::container::MissingKeyframe)) => {
            Ok(atoms::missing_keyframe())
        }
        Err(e) => Err(crate::nif_error!("writing frame failed: {e}")),
    }
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn remove_track(track_res: ResourceArc<TrackResource>) -> Atom {
    let _guard = runtime().handle().enter();
    track_res.teardown();
    atoms::ok()
}

fn init_track(
    tp: moq_net::TrackProducer,
    mut resolved: ResolvedConfig,
    inner: &mut ProducerInner,
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    settings: TrackSettings,
    replaces: Option<&str>,
) -> NifResult<TrackResource> {
    let catalog_container = settings.container.to_catalog();
    let wire = moq_mux::catalog::hang::Container::try_from(&catalog_container)
        .map_err(|e| crate::nif_error!("container init failed: {e}"))?;
    resolved.set_container(catalog_container);

    let name = tp.name.clone();
    let kind = resolved.kind();

    {
        let mut guard = inner.catalog.lock();
        match resolved {
            ResolvedConfig::Video(config) => {
                if let Some(old_name) = replaces {
                    guard.video.renditions.remove(old_name);
                }
                guard.video.renditions.insert(name.clone(), config);
            }
            ResolvedConfig::Audio(config) => {
                if let Some(old_name) = replaces {
                    guard.audio.renditions.remove(old_name);
                }
                guard.audio.renditions.insert(name.clone(), config);
            }
        }
    }

    let producer = Arc::new(Mutex::new(
        moq_mux::container::Producer::new(tp, wire).with_latency(settings.latency),
    ));

    if let Some(old_name) = replaces {
        inner.tracks.remove(old_name);
    }
    inner.tracks.insert(name.clone(), Arc::downgrade(&producer));

    Ok(TrackResource {
        producer,
        broadcast_res,
        name,
        kind,
        settings,
    })
}
