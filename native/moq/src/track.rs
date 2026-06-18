use hang::moq_net;

use bytes::Bytes;
use rustler::{Atom, Binary, NifResult, NifTaggedEnum, Resource, ResourceArc};
use std::sync::Mutex;

use crate::{
    atoms,
    broadcast::BroadcastResource,
    nif_types::{H264Codec, H265Codec, VideoTrackParams},
    runtime,
};

type LegacyProducer = moq_mux::container::Producer<moq_mux::container::legacy::Wire>;

pub(crate) struct TrackResource {
    producer: Mutex<LegacyProducer>,
    broadcast_res: ResourceArc<BroadcastResource>,
    name: String,
    // Used to generate new, unique track names if format changes mid-stream.
    // For the first rendition (before any stream format change), suffix == name
    suffix: String,
    kind: TrackKind,
}

impl Resource for TrackResource {}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum TrackKind {
    Video,
    Audio,
}

#[derive(NifTaggedEnum)]
pub(crate) enum TrackFormat<'a> {
    H264 {
        params: VideoTrackParams,
        dcr: Binary<'a>,
        codec: H264Codec,
    },
    H265 {
        params: VideoTrackParams,
        dcr: Binary<'a>,
        codec: H265Codec,
    },
    Aac {
        profile: u8,
        sample_rate: u32,
        channels: u32,
    },
    Opus {
        sample_rate: u32,
        channels: u32,
    },
}

enum ResolvedConfig {
    Video(hang::catalog::VideoConfig),
    Audio(hang::catalog::AudioConfig),
}

impl ResolvedConfig {
    fn kind(&self) -> TrackKind {
        match self {
            ResolvedConfig::Video(_) => TrackKind::Video,
            ResolvedConfig::Audio(_) => TrackKind::Audio,
        }
    }
}

impl TrackFormat<'_> {
    fn resolve(self) -> NifResult<ResolvedConfig> {
        let config = match self {
            TrackFormat::H264 { params, dcr, codec } => {
                ResolvedConfig::Video(h264_video_config(params, dcr.as_slice(), codec))
            }
            TrackFormat::H265 { params, dcr, codec } => {
                ResolvedConfig::Video(h265_video_config(params, dcr.as_slice(), codec)?)
            }
            TrackFormat::Aac {
                profile,
                sample_rate,
                channels,
            } => ResolvedConfig::Audio(aac_audio_config(profile, sample_rate, channels)),
            TrackFormat::Opus {
                sample_rate,
                channels,
            } => ResolvedConfig::Audio(opus_audio_config(sample_rate, channels)),
        };
        Ok(config)
    }
}

#[rustler::nif]
pub(crate) fn add_track(
    broadcast_res: ResourceArc<BroadcastResource>,
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

fn h264_video_config(
    video_params: VideoTrackParams,
    dcr: &[u8],
    codec: H264Codec,
) -> hang::catalog::VideoConfig {
    let codec = hang::catalog::VideoCodec::H264(hang::catalog::H264 {
        inline: codec.inline,
        profile: codec.profile,
        constraints: codec.constraints,
        level: codec.level,
    });

    let description = (!dcr.is_empty()).then(|| Bytes::copy_from_slice(dcr));

    create_video_config(
        codec,
        video_params.width,
        video_params.height,
        video_params.framerate,
        description,
    )
}

fn h265_video_config(
    video_params: VideoTrackParams,
    dcr: &[u8],
    codec: H265Codec,
) -> NifResult<hang::catalog::VideoConfig> {
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

    let description = (!dcr.is_empty()).then(|| Bytes::copy_from_slice(dcr));

    Ok(create_video_config(
        codec,
        video_params.width,
        video_params.height,
        video_params.framerate,
        description,
    ))
}

fn aac_audio_config(profile: u8, sample_rate: u32, channels: u32) -> hang::catalog::AudioConfig {
    let codec = hang::catalog::AudioCodec::AAC(hang::catalog::AAC { profile });
    create_audio_config(codec, sample_rate, channels)
}

fn opus_audio_config(sample_rate: u32, channels: u32) -> hang::catalog::AudioConfig {
    create_audio_config(hang::catalog::AudioCodec::Opus, sample_rate, channels)
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
