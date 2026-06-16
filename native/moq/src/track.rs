use bytes::Bytes;
use rustler::{Atom, Binary, Encoder, LocalPid, NifResult, OwnedEnv, Resource, ResourceArc};
use tokio::sync::mpsc;

use crate::{
    atoms,
    broadcast::BroadcastResource,
    nif_types::{H264Codec, H265Codec, VideoTrackParams},
    runtime,
};

enum TrackCmd {
    Frame(moq_mux::container::Frame),
    Stop,
}

pub(crate) struct TrackResource {
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
pub(crate) fn add_h264_track(
    pid: LocalPid,
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    video_params: VideoTrackParams,
    dcr: Binary,
    codec: H264Codec,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::VideoCodec::H264(hang::catalog::H264 {
        inline: codec.inline,
        profile: codec.profile,
        constraints: codec.constraints,
        level: codec.level,
    });

    let description = if dcr.is_empty() {
        None
    } else {
        Some(bytes::Bytes::copy_from_slice(dcr.as_slice()))
    };

    let config = create_video_config(
        codec,
        video_params.width,
        video_params.height,
        video_params.framerate,
        description,
    );

    add_video_track(pid, broadcast_res, track, config)
}

#[rustler::nif]
pub(crate) fn add_h265_track(
    pid: LocalPid,
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

    let codec = hang::catalog::VideoCodec::H265(hang::catalog::H265 {
        in_band: codec.in_band,
        profile_space: codec.profile_space,
        profile_idc: codec.profile_idc,
        profile_compatibility_flags,
        tier_flag: codec.tier_flag,
        level_idc: codec.level_idc,
        constraint_flags,
    });

    let description = if dcr.is_empty() {
        None
    } else {
        Some(bytes::Bytes::copy_from_slice(dcr.as_slice()))
    };

    add_video_track(
        pid,
        broadcast_res,
        track,
        create_video_config(
            codec,
            video_params.width,
            video_params.height,
            video_params.framerate,
            description,
        ),
    )
}

#[rustler::nif]
pub(crate) fn add_aac_track(
    pid: LocalPid,
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    profile: u8,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::AAC(hang::catalog::AAC { profile });

    add_audio_track(
        pid,
        broadcast_res,
        track,
        create_audio_config(codec, sample_rate, channels),
    )
}

#[rustler::nif]
pub(crate) fn add_opus_track(
    pid: LocalPid,
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    sample_rate: u32,
    channels: u32,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let codec = hang::catalog::AudioCodec::Opus;

    let config = create_audio_config(codec, sample_rate, channels);

    add_audio_track(pid, broadcast_res, track, config)
}

#[rustler::nif]
pub(crate) fn send_frame(
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

    track_res
        .sender
        .send(TrackCmd::Frame(frame))
        .map_err(|e| crate::nif_error!("sending frame to track task failed: {e}"))?;
    Ok(atoms::ok())
}

#[rustler::nif]
pub(crate) fn remove_track(track_res: ResourceArc<TrackResource>) -> Atom {
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

#[allow(clippy::too_many_arguments)]
fn add_video_track(
    pid: LocalPid,
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    config: hang::catalog::VideoConfig,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let tp = {
        let mut bp = broadcast_res.broadcast.lock().unwrap();
        bp.create_track(hang::moq_net::Track {
            name: track.clone(),
            priority: 0,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?
    };

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        guard.video.renditions.insert(track.clone(), config);
    }

    let sender = spawn_track_task(pid, tp);

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
    pid: LocalPid,
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    config: hang::catalog::AudioConfig,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    let _guard = runtime().handle().enter();

    let tp = {
        let mut bp = broadcast_res.broadcast.lock().unwrap();
        bp.create_track(hang::moq_net::Track {
            name: track.clone(),
            priority: 0,
        })
        .map_err(|e| crate::nif_error!("create_track failed: {e}"))?
    };

    {
        let mut cp = broadcast_res.catalog.lock().unwrap();
        let mut guard = cp.lock();
        guard.audio.renditions.insert(track.clone(), config);
    }

    let sender = spawn_track_task(pid, tp);

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

fn spawn_track_task(
    pid: LocalPid,
    track: hang::moq_net::TrackProducer,
) -> mpsc::UnboundedSender<TrackCmd> {
    let (tx, mut rx) = mpsc::unbounded_channel::<TrackCmd>();
    runtime().spawn(async move {
        let mut producer =
            moq_mux::container::Producer::new(track, moq_mux::container::legacy::Wire);
        while let Some(cmd) = rx.recv().await {
            match cmd {
                TrackCmd::Frame(frame) => {
                    if producer.write(frame).is_err() {
                        OwnedEnv::new()
                            .send_and_clear(&pid, |env| {
                                (atoms::moq_write_failed(), producer.track().name.clone())
                                    .encode(env)
                            })
                            .expect("sending message to parent should succeed")
                    }
                }
                TrackCmd::Stop => break,
            }
        }
        let _ = producer.finish();
    });
    tx
}

fn create_video_config(
    codec: hang::catalog::VideoCodec,
    width: u32,
    height: u32,
    framerate: f64,
    description: Option<Bytes>,
) -> hang::catalog::VideoConfig {
    let mut config = hang::catalog::VideoConfig::new(codec);
    config.description = description;
    config.coded_width = Some(width);
    config.coded_height = Some(height);
    config.framerate = Some(framerate);
    config.optimize_for_latency = Some(true);
    config.container = hang::catalog::Container::Legacy;
    config
}
fn create_audio_config(
    codec: hang::catalog::AudioCodec,
    sample_rate: u32,
    channel_count: u32,
) -> hang::catalog::AudioConfig {
    hang::catalog::AudioConfig::new(codec, sample_rate, channel_count)
}
