use rustler::{Atom, Binary, NifResult, ResourceArc};
use std::sync::Mutex;
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
    pub(crate) sender: Mutex<Option<mpsc::UnboundedSender<TrackCmd>>>, // TODO: do we need the mutex here? `TrackResource` is always wrapped in ResourceArc anyway
    pub(crate) broadcast_res: ResourceArc<BroadcastResource>,
    pub(crate) name: String,
    pub(crate) kind: TrackRole,
}

impl rustler::Resource for TrackResource {}

#[derive(Clone, Copy)]
pub(crate) enum TrackRole {
    Video,
    Audio,
}

#[rustler::nif]
pub fn add_h264_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
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
        track_name,
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
    track_name: String,
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
        track_name,
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
    track_name: String,
    profile: u8,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::AAC(hang::catalog::AAC { profile });
    add_audio_track(broadcast_res, track_name, codec, sample_rate, channels)
}

#[rustler::nif]
pub fn add_opus_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track_name: String,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::Opus;
    add_audio_track(broadcast_res, track_name, codec, sample_rate, channels)
}

#[rustler::nif]
pub fn send_frame(
    track: ResourceArc<TrackResource>,
    timestamp_us: u64,
    keyframe: bool,
    data: Binary,
) -> Atom {
    let timestamp = match moq_mux::container::Timestamp::from_micros(timestamp_us) {
        Ok(t) => t,
        Err(_) => {
            // TODO: logs skip Membrane.Logger
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
pub fn remove_track(track_res: ResourceArc<TrackResource>) -> Atom {
    if let Some(tx) = track_res.sender.lock().unwrap().take() {
        let _ = tx.send(TrackCmd::Stop);
    }

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
    track_name: String,
    codec: hang::catalog::VideoCodec,
    width: u32,
    height: u32,
    framerate: f64,
    description: Option<bytes::Bytes>,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let track = {
        let mut bp = broadcast_res.broadcast.lock().unwrap();
        bp.create_track(moq_lite::Track {
            name: track_name.clone(),
            priority: 0,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?
    };

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        guard.video.renditions.insert(
            track_name.clone(),
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

    let sender = spawn_track_task(track);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            sender: Mutex::new(Some(sender)),
            broadcast_res,
            name: track_name,
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
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?
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
            broadcast_res,
            name: track_name,
            kind: TrackRole::Audio,
        }),
    ))
}

fn spawn_track_task(track: moq_lite::TrackProducer) -> mpsc::UnboundedSender<TrackCmd> {
    let (tx, mut rx) = mpsc::unbounded_channel::<TrackCmd>();
    runtime().spawn(async move {
        let mut producer = moq_mux::ordered::Producer::new(track, moq_mux::hang::Legacy);
        while let Some(cmd) = rx.recv().await {
            match cmd {
                TrackCmd::Frame(frame) => {
                    if let Err(e) = producer.write(frame) {
                        eprintln!("track write failed: {e}"); // TODO: bypasses Membrane.Logger
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
