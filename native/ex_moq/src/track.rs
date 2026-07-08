use hang::moq_net;

use rustler::{Atom, Binary, NifResult, Resource, ResourceArc};
use std::sync::Mutex;

use crate::{
    atoms,
    broadcast_producer::BroadcastProducerResource,
    runtime,
    track_format::{ContainerKind, ResolvedConfig, TrackFormat, TrackKind},
};

/// Producer over the runtime-dispatched container enum,
/// so one resource type covers every wire format the catalog can describe.
type WireProducer = moq_mux::container::Producer<moq_mux::catalog::hang::Container>;

pub(crate) struct TrackResource {
    producer: Mutex<WireProducer>,
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    name: String,
    // Used to generate new, unique track names if format changes mid-stream.
    // For the first rendition (before any stream format change), suffix == name
    suffix: String,
    kind: TrackKind,
    priority: u8,
    container: ContainerKind,
    latency: std::time::Duration,
}

impl Resource for TrackResource {}

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

    let tp = broadcast_res
        .broadcast
        .lock()
        .unwrap()
        .create_track(moq_net::Track {
            name: track,
            priority,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?;

    let suffix = tp.name.clone();
    let resource = init_track(
        tp,
        resolved,
        broadcast_res,
        suffix,
        priority,
        container,
        std::time::Duration::from_nanos(latency_ns),
        None,
    )?;

    Ok((atoms::ok(), ResourceArc::new(resource)))
}

/// Replace a live track with one carrying a new format, published on a brand-new moq track.
/// Returns a new track resource through which all subsequent frames must be sent.
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
    let suffix = old_track_res.suffix.clone();
    let priority = old_track_res.priority;

    let tp = {
        let mut broadcast = broadcast_res.broadcast.lock().unwrap();
        let name = broadcast.unique_name(&suffix);
        broadcast
            .create_track(moq_net::Track { name, priority })
            .map_err(|e| crate::nif_error!("create_track failed: {e}"))?
    };

    // The new rendition keeps the replaced track's container and latency.
    let resource = init_track(
        tp,
        resolved,
        broadcast_res,
        suffix,
        priority,
        old_track_res.container,
        old_track_res.latency,
        Some(&old_track_res.name),
    )?;

    let _ = old_track_res.producer.lock().unwrap().finish();

    let name = resource.name.clone();
    Ok((atoms::ok(), ResourceArc::new(resource), name))
}

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
    track_res
        .producer
        .lock()
        .unwrap()
        .write(frame)
        .map_err(|e| crate::nif_error!("writing frame failed: {e}"))?;
    Ok(atoms::ok())
}

#[rustler::nif]
pub(crate) fn remove_track(track_res: ResourceArc<TrackResource>) -> Atom {
    let _guard = runtime().handle().enter();
    let _ = track_res.producer.lock().unwrap().finish();

    let mut cp = track_res.broadcast_res.catalog.lock().unwrap();
    let mut guard = cp.lock();
    match track_res.kind {
        TrackKind::Video => {
            guard.video.renditions.remove(&track_res.name);
        }
        TrackKind::Audio => {
            guard.audio.renditions.remove(&track_res.name);
        }
    }

    atoms::ok()
}

fn init_track(
    tp: moq_net::TrackProducer,
    mut resolved: ResolvedConfig,
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    suffix: String,
    priority: u8,
    container: ContainerKind,
    latency: std::time::Duration,
    replaces: Option<&str>,
) -> NifResult<TrackResource> {
    let catalog_container = container.to_catalog()?;
    let wire = moq_mux::catalog::hang::Container::try_from(&catalog_container)
        .map_err(|e| crate::nif_error!("container init failed: {e}"))?;
    resolved.set_container(catalog_container);

    let name = tp.name.clone();
    let kind = resolved.kind();

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
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

    let producer = moq_mux::container::Producer::new(tp, wire).with_latency(latency);

    Ok(TrackResource {
        producer: Mutex::new(producer),
        broadcast_res,
        name,
        suffix,
        kind,
        priority,
        container,
        latency,
    })
}
