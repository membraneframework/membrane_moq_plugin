use hang::moq_net;

use rustler::{Atom, Binary, NifResult, Resource, ResourceArc};
use std::sync::Mutex;

use crate::{
    atoms,
    broadcast_producer::BroadcastProducerResource,
    runtime,
    track_format::{ResolvedConfig, TrackFormat, TrackKind},
};

type LegacyProducer = moq_mux::container::Producer<moq_mux::container::legacy::Wire>;

pub(crate) struct TrackResource {
    producer: Mutex<LegacyProducer>,
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    name: String,
    // Used to generate new, unique track names if format changes mid-stream.
    // For the first rendition (before any stream format change), suffix == name
    suffix: String,
    kind: TrackKind,
}

impl Resource for TrackResource {}

#[rustler::nif]
pub(crate) fn add_track(
    broadcast_res: ResourceArc<BroadcastProducerResource>,
    track: String,
    format: TrackFormat,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let resolved = format.resolve()?;

    let tp = broadcast_res
        .broadcast
        .lock()
        .unwrap()
        .create_track(moq_net::Track {
            name: track,
            priority: 0,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?;
    let name = tp.name.clone();
    let kind = resolved.kind();

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        match resolved {
            ResolvedConfig::Video(config) => {
                guard.video.renditions.insert(name.clone(), config);
            }
            ResolvedConfig::Audio(config) => {
                guard.audio.renditions.insert(name.clone(), config);
            }
        }
    }

    let producer = moq_mux::container::Producer::new(tp, moq_mux::container::legacy::Wire);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            producer: Mutex::new(producer),
            suffix: name.clone(),
            broadcast_res,
            name,
            kind,
        }),
    ))
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
    let kind = resolved.kind();

    if kind != old_track_res.kind {
        return Err(crate::nif_error!(
            "cannot change a track's media kind in place"
        ));
    }

    let broadcast_res = old_track_res.broadcast_res.clone();
    let suffix = old_track_res.suffix.clone();

    let tp = broadcast_res
        .broadcast
        .lock()
        .unwrap()
        .unique_track(&suffix)
        .map_err(|e| crate::nif_error!("unique_track failed: {e}"))?;

    let name = tp.name.clone();

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        match resolved {
            ResolvedConfig::Video(config) => {
                guard.video.renditions.remove(&old_track_res.name);
                guard.video.renditions.insert(name.clone(), config);
            }
            ResolvedConfig::Audio(config) => {
                guard.audio.renditions.remove(&old_track_res.name);
                guard.audio.renditions.insert(name.clone(), config);
            }
        }
    }

    let _ = old_track_res.producer.lock().unwrap().finish();

    let producer = moq_mux::container::Producer::new(tp, moq_mux::container::legacy::Wire);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            producer: Mutex::new(producer),
            broadcast_res,
            name: name.clone(),
            suffix,
            kind,
        }),
        name,
    ))
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
