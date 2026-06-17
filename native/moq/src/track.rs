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
    kind: TrackRole,
}

impl Resource for TrackResource {}

#[derive(Clone, Copy)]
pub(crate) enum TrackRole {
    Video,
    Audio,
}

/// One codec's full track format, decoded straight from an Elixir tagged tuple.
///
/// `NifTaggedEnum` maps each variant to `{:variant_name, %{field => value}}`, with
/// names snake_cased. So the Elixir side sends e.g.
///
///   {:h264, %{params: %VideoTrackParams{...}, dcr: <<...>>, codec: %H264Codec{...}}}
///   {:opus, %{sample_rate: 48_000, channels: 2}}
///
/// Nested `VideoTrackParams` / `H264Codec` / `H265Codec` are themselves `NifStruct`s,
/// so they decode from the map values automatically. `dcr` borrows the caller's
/// binary for the duration of the call (hence the `'a` lifetime).
///
/// This collapses the per-codec `add_*_track` NIFs into a single `add_track`, and
/// lets `update_track` reuse the exact same shape.
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

/// A decoded format resolved to its catalog config, tagged by media role.
enum ResolvedConfig {
    Video(hang::catalog::VideoConfig),
    Audio(hang::catalog::AudioConfig),
}

impl TrackFormat<'_> {
    /// Build the hang catalog config for this format. Shared by `add_track`
    /// (creates a new track) and `update_track` (republishes in place).
    fn resolve(self) -> NifResult<ResolvedConfig> {
        let config = match self {
            TrackFormat::H264 {
                params,
                dcr,
                codec,
            } => ResolvedConfig::Video(h264_video_config(params, dcr.as_slice(), codec)),
            TrackFormat::H265 {
                params,
                dcr,
                codec,
            } => ResolvedConfig::Video(h265_video_config(params, dcr.as_slice(), codec)?),
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

/// Add a track of any supported codec, dispatching on the Elixir-decoded
/// `TrackFormat` enum. Replaces the four per-codec `add_*_track` NIFs.
#[rustler::nif]
pub(crate) fn add_track(
    broadcast_res: ResourceArc<BroadcastResource>,
    track: String,
    format: TrackFormat,
) -> NifResult<(Atom, ResourceArc<TrackResource>)> {
    match format.resolve()? {
        ResolvedConfig::Video(config) => add_video_track(broadcast_res, track, config),
        ResolvedConfig::Audio(config) => add_audio_track(broadcast_res, track, config),
    }
}

/// Change a live track's format in place: keep the existing producer (and its
/// monotonic group sequence) and just republish the catalog rendition under the
/// same name. The same `TrackFormat` enum used by `add_track` is reused here.
#[rustler::nif]
pub(crate) fn update_track(
    track_res: ResourceArc<TrackResource>,
    format: TrackFormat,
) -> NifResult<Atom> {
    let _guard = runtime().handle().enter();

    let mut cp = track_res.broadcast_res.catalog.lock().unwrap();
    let mut guard = cp.lock();

    match (track_res.kind, format.resolve()?) {
        (TrackRole::Video, ResolvedConfig::Video(config)) => {
            guard.video.renditions.insert(track_res.name.clone(), config);
        }
        (TrackRole::Audio, ResolvedConfig::Audio(config)) => {
            guard.audio.renditions.insert(track_res.name.clone(), config);
        }
        // Switching media role (e.g. audio track -> video format) isn't a format
        // change of the same track; that needs a new track.
        _ => return Err(crate::nif_error!("cannot change a track's media role in place")),
    }

    Ok(atoms::ok())
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

    let producer = moq_mux::container::Producer::new(tp, moq_mux::container::legacy::Wire);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            producer: Mutex::new(producer),
            broadcast_res,
            name: track,
            kind: TrackRole::Video,
        }),
    ))
}

fn add_audio_track(
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

    let producer = moq_mux::container::Producer::new(tp, moq_mux::container::legacy::Wire);

    Ok((
        atoms::ok(),
        ResourceArc::new(TrackResource {
            producer: Mutex::new(producer),
            broadcast_res,
            name: track,
            kind: TrackRole::Audio,
        }),
    ))
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
