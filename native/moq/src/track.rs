use rustler::{Atom, Binary, NifResult, Resource, ResourceArc};
use tokio::sync::mpsc;

use crate::{
    atoms,
    broadcast::BroadcastResource,
    nif_types::{H264Codec, H265Codec, VideoTrackParams},
    runtime,
};

pub(crate) enum TrackCmd {
    Frame(moq_mux::container::Frame),
    Stop,
}

pub struct TrackResource {
    sender: mpsc::UnboundedSender<TrackCmd>,
    broadcast_res: ResourceArc<BroadcastResource>,
    name: String,
    kind: TrackRole,
}

impl Resource for TrackResource {}

#[derive(Clone, Copy)]
pub(crate) enum TrackRole {
    Video,
    Audio,
}

#[rustler::nif]
pub fn add_h264_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    video_params: VideoTrackParams,
    dcr: Binary,
    codec: H264Codec,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let video_codec = hang::catalog::VideoCodec::H264(hang::catalog::H264 {
        inline: codec.inline,
        profile: codec.profile,
        constraints: codec.constraints,
        level: codec.level,
    });

    let desc = if dcr.is_empty() {
        None
    } else {
        Some(bytes::Bytes::copy_from_slice(dcr.as_slice()))
    };
    add_video_track(
        broadcast_res,
        track,
        video_codec,
        video_params.width,
        video_params.height,
        video_params.framerate,
        desc,
    )
}

/// Add an H.265 video track to the broadcast.
#[rustler::nif]
pub fn add_h265_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    video_params: VideoTrackParams,
    dcr: Binary,
    codec: H265Codec,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let profile_compatibility_flags: [u8; 4] = codec
        .profile_compatibility_flags
        .try_into()
        .map_err(|_| crate::nif_error!("profile_compatibility_flags must be exactly 4 bytes"))?;

    let constraint_flags: [u8; 6] = codec
        .constraint_flags
        .try_into()
        .map_err(|_| crate::nif_error!("constraint_flags must be exactly 6 bytes"))?;

    let video_codec = hang::catalog::VideoCodec::H265(hang::catalog::H265 {
        in_band: codec.in_band,
        profile_space: codec.profile_space,
        profile_idc: codec.profile_idc,
        profile_compatibility_flags,
        tier_flag: codec.tier_flag,
        level_idc: codec.level_idc,
        constraint_flags,
    });

    let desc = if dcr.is_empty() {
        None
    } else {
        Some(bytes::Bytes::copy_from_slice(dcr.as_slice()))
    };

    add_video_track(
        broadcast_res,
        track,
        video_codec,
        video_params.width,
        video_params.height,
        video_params.framerate,
        desc,
    )
}

#[rustler::nif]
pub fn add_aac_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    profile: u8,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::AAC(hang::catalog::AAC { profile });
    add_audio_track(broadcast_res, track, codec, sample_rate, channels)
}

#[rustler::nif]
pub fn add_opus_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::Opus;
    add_audio_track(broadcast_res, track, codec, sample_rate, channels)
}

#[rustler::nif]
pub fn send_frame(
    track_res: ResourceArc<TrackResource>,
    timestamp_us: u64,
    keyframe: bool,
    data: Binary,
) -> NifResult<Atom> {
    let timestamp = moq_mux::container::Timestamp::from_micros(timestamp_us)
        .map_err(|e| crate::nif_error!("timestamp conversion failed: {e}"))?;
    let frame = moq_mux::container::Frame {
        timestamp,
        payload: bytes::Bytes::copy_from_slice(data.as_slice()),
        keyframe,
    };

    let _ = track_res.sender.send(TrackCmd::Frame(frame));
    Ok(atoms::ok())
}

/// Close a track: stop its data task, finish the moq-lite track, and remove
/// the rendition from the broadcast catalog. Idempotent.
#[rustler::nif]
pub fn remove_track(track_res: ResourceArc<TrackResource>) -> Atom {
    let _ = track_res.sender.send(TrackCmd::Stop);

    let mut cp = track_res.broadcast_res.catalog.lock().unwrap();
    let mut guard = cp.lock();
    match track_res.kind {
        TrackRole::Video => {
            guard.video.renditions.remove(&track_res.name);
        }
        TrackRole::Audio => {
            guard.audio.renditions.remove(&track_res.name);
        }
    }

    atoms::ok()
}

fn add_video_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    codec: hang::catalog::VideoCodec,
    width: u32,
    height: u32,
    framerate: f64,
    description: Option<bytes::Bytes>,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let track_res = {
        let mut bp = broadcast_res.broadcast.lock().unwrap();
        bp.create_track(hang::moq_lite::Track {
            name: track.clone(),
            priority: 0,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?
    };

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        guard.video.renditions.insert(
            track.clone(),
            hang::catalog::VideoConfig {
                codec,
                description,
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

    let sender = spawn_track_task(track_res);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            sender,
            broadcast_res,
            name: track,
            kind: TrackRole::Video,
        }),
    ))
}

fn add_audio_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    codec: hang::catalog::AudioCodec,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let track_res = {
        let mut bp = broadcast_res.broadcast.lock().unwrap();
        bp.create_track(hang::moq_lite::Track {
            name: track.clone(),
            priority: 0,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?
    };

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        guard.audio.renditions.insert(
            track.clone(),
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

    let sender = spawn_track_task(track_res);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            sender,
            broadcast_res,
            name: track,
            kind: TrackRole::Audio,
        }),
    ))
}

fn spawn_track_task(track: hang::moq_lite::TrackProducer) -> mpsc::UnboundedSender<TrackCmd> {
    let (tx, mut rx) = mpsc::unbounded_channel::<TrackCmd>();
    runtime().spawn(async move {
        let mut producer =
            moq_mux::container::Producer::new(track, moq_mux::container::Hang::Legacy);
        while let Some(cmd) = rx.recv().await {
            match cmd {
                TrackCmd::Frame(frame) => {
                    if let Err(e) = producer.write(frame) {
                        eprintln!("track write failed: {e}"); // TODO: bypasses Membrane.Logger
                                                              // TODO: what happens then this receiver dies? we break, call producer.finish(), the task finishes,
                                                              // TODO: do all subsequent calls to `tx` fail?
                                                              // TODO: should a call to `producer.write` fail this entire task?
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
