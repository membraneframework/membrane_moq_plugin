use bytes::Bytes;
use rustler::{
    Binary, Encoder, Env, NewBinary, NifResult, NifStruct, NifTaggedEnum, NifUnitEnum, Term,
};

#[derive(NifUnitEnum, Clone, Copy, PartialEq)]
pub(crate) enum ContainerKind {
    Legacy,
    Loc,
}

impl ContainerKind {
    pub(crate) const fn to_catalog(self) -> hang::catalog::Container {
        match self {
            Self::Legacy => hang::catalog::Container::Legacy,
            Self::Loc => hang::catalog::Container::Loc,
        }
    }
}

/// A consumed rendition's wire container as it crosses the NIF boundary,
/// so a subscription can echo the catalog's choice back.
#[derive(NifTaggedEnum)]
pub(crate) enum WireContainer<'a> {
    Legacy,
    Loc,
    Cmaf { init: Binary<'a> },
}

impl WireContainer<'_> {
    /// Build the runtime-dispatched container the frame parser needs.
    pub(crate) fn resolve(&self) -> Result<moq_mux::catalog::hang::Container, moq_mux::Error> {
        Ok(match self {
            Self::Legacy => moq_mux::catalog::hang::Container::Legacy,
            Self::Loc => moq_mux::catalog::hang::Container::Loc,
            Self::Cmaf { init } => moq_mux::catalog::hang::Container::Cmaf(
                moq_mux::container::fmp4::Wire::from_init(&Bytes::copy_from_slice(
                    init.as_slice(),
                ))?,
            ),
        })
    }
}

pub(crate) fn encode_container<'a>(
    env: Env<'a>,
    container: &hang::catalog::Container,
) -> Term<'a> {
    let container = match container {
        hang::catalog::Container::Legacy => WireContainer::Legacy,
        hang::catalog::Container::Loc => WireContainer::Loc,
        hang::catalog::Container::Cmaf { init, .. } => {
            let mut binary = NewBinary::new(env, init.len());
            binary.as_mut_slice().copy_from_slice(init);
            WireContainer::Cmaf {
                init: binary.into(),
            }
        }
    };
    container.encode(env)
}

#[derive(NifStruct, Clone, PartialEq)]
#[module = "ExMoQ.Native.VideoTrackParams"]
pub(crate) struct VideoTrackParams {
    /// Investigate if Rustler could accept `Option<std::num::NonZero<T>>` for these
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
    pub(crate) framerate: Option<f64>,
}

#[derive(NifStruct, Clone, PartialEq)]
#[module = "ExMoQ.Native.AudioTrackParams"]
pub(crate) struct AudioTrackParams {
    pub(crate) sample_rate: u32,
    pub(crate) channels: u32,
}

#[derive(NifStruct, Clone, PartialEq)]
#[module = "ExMoQ.Native.H264Codec"]
pub(crate) struct H264Codec {
    pub(crate) inline: bool,
    pub(crate) profile: u8,
    pub(crate) constraints: u8,
    pub(crate) level: u8,
}

#[derive(NifStruct, Clone, PartialEq)]
#[module = "ExMoQ.Native.H265Codec"]
pub(crate) struct H265Codec {
    pub(crate) in_band: bool,
    pub(crate) profile_space: u8,
    pub(crate) profile_idc: u8,
    pub(crate) profile_compatibility_flags: Vec<u8>,
    pub(crate) tier_flag: bool,
    pub(crate) level_idc: u8,
    pub(crate) constraint_flags: Vec<u8>,
}

#[derive(NifStruct, Clone, PartialEq)]
#[module = "ExMoQ.Native.AACCodec"]
pub(crate) struct AacCodec {
    pub(crate) profile: u8,
}

#[derive(NifTaggedEnum)]
pub(crate) enum TrackFormat<'a> {
    H264 {
        params: VideoTrackParams,
        description: Binary<'a>,
        codec: H264Codec,
    },
    H265 {
        params: VideoTrackParams,
        description: Binary<'a>,
        codec: H265Codec,
    },
    Aac {
        params: AudioTrackParams,
        codec: AacCodec,
    },
    Opus {
        params: AudioTrackParams,
    },
    Unrecognized,
}

pub(crate) enum ResolvedConfig {
    Video(hang::catalog::VideoConfig),
    Audio(hang::catalog::AudioConfig),
}

impl ResolvedConfig {
    pub(crate) fn set_container(&mut self, container: hang::catalog::Container) {
        match self {
            Self::Video(config) => config.container = container,
            Self::Audio(config) => config.container = container,
        }
    }
}

impl TrackFormat<'_> {
    pub(crate) fn resolve(self) -> NifResult<ResolvedConfig> {
        let config = match self {
            TrackFormat::H264 {
                params,
                description,
                codec,
            } => ResolvedConfig::Video(h264_video_config(params, description.as_slice(), codec)),
            TrackFormat::H265 {
                params,
                description,
                codec,
            } => ResolvedConfig::Video(h265_video_config(params, description.as_slice(), codec)?),
            TrackFormat::Aac { params, codec } => ResolvedConfig::Audio(aac_audio_config(
                codec.profile,
                params.sample_rate,
                params.channels,
            )),
            TrackFormat::Opus { params } => {
                ResolvedConfig::Audio(opus_audio_config(params.sample_rate, params.channels))
            }
            TrackFormat::Unrecognized => {
                return Err(crate::nif_error!(
                    "cannot publish an unrecognized track format"
                ))
            }
        };
        Ok(config)
    }
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

fn create_video_config(
    codec: hang::catalog::VideoCodec,
    width: Option<u32>,
    height: Option<u32>,
    framerate: Option<f64>,
    description: Option<Bytes>,
) -> hang::catalog::VideoConfig {
    let mut config = hang::catalog::VideoConfig::new(codec);
    config.description = description;
    config.coded_width = width;
    config.coded_height = height;
    config.framerate = framerate;
    config.optimize_for_latency = Some(true);
    config
}

fn create_audio_config(
    codec: hang::catalog::AudioCodec,
    sample_rate: u32,
    channel_count: u32,
) -> hang::catalog::AudioConfig {
    hang::catalog::AudioConfig::new(codec, sample_rate, channel_count)
}

#[derive(Clone, PartialEq)]
pub(crate) enum VideoCodecParams {
    H264(H264Codec),
    H265(H265Codec),
}

#[derive(Clone, PartialEq)]
pub(crate) enum AudioCodecParams {
    Aac(AacCodec),
    Opus,
}

#[derive(Clone, PartialEq)]
pub(crate) enum TrackParams {
    Video {
        params: VideoTrackParams,
        /// Decoder configuration record (avcC/hvcC).
        /// empty when carried in-band.
        description: Vec<u8>,
        codec: VideoCodecParams,
    },
    Audio {
        params: AudioTrackParams,
        codec: AudioCodecParams,
    },
    Unrecognized,
}

pub(crate) fn video_params(config: &hang::catalog::VideoConfig) -> TrackParams {
    let codec = match &config.codec {
        hang::catalog::VideoCodec::H264(h) => VideoCodecParams::H264(H264Codec {
            inline: h.inline,
            profile: h.profile,
            constraints: h.constraints,
            level: h.level,
        }),
        hang::catalog::VideoCodec::H265(h) => VideoCodecParams::H265(H265Codec {
            in_band: h.in_band,
            profile_space: h.profile_space,
            profile_idc: h.profile_idc,
            profile_compatibility_flags: h.profile_compatibility_flags.to_vec(),
            tier_flag: h.tier_flag,
            level_idc: h.level_idc,
            constraint_flags: h.constraint_flags.to_vec(),
        }),
        _ => return TrackParams::Unrecognized,
    };

    TrackParams::Video {
        params: VideoTrackParams {
            width: config.coded_width,
            height: config.coded_height,
            framerate: config.framerate,
        },
        description: config
            .description
            .as_ref()
            .map(|b| b.to_vec())
            .unwrap_or_default(),
        codec,
    }
}

pub(crate) const fn audio_params(config: &hang::catalog::AudioConfig) -> TrackParams {
    let codec = match &config.codec {
        hang::catalog::AudioCodec::AAC(aac) => AudioCodecParams::Aac(AacCodec {
            profile: aac.profile,
        }),
        hang::catalog::AudioCodec::Opus => AudioCodecParams::Opus,
        _ => return TrackParams::Unrecognized,
    };

    TrackParams::Audio {
        params: AudioTrackParams {
            sample_rate: config.sample_rate,
            channels: config.channel_count,
        },
        codec,
    }
}

pub(crate) fn encode_format<'a>(env: Env<'a>, params: &TrackParams) -> Term<'a> {
    let format = match params {
        TrackParams::Video {
            params,
            description,
            codec,
        } => {
            let mut dcr_binary = NewBinary::new(env, description.len());
            dcr_binary.as_mut_slice().copy_from_slice(description);

            match codec {
                VideoCodecParams::H264(codec) => TrackFormat::H264 {
                    params: params.clone(),
                    description: dcr_binary.into(),
                    codec: codec.clone(),
                },
                VideoCodecParams::H265(codec) => TrackFormat::H265 {
                    params: params.clone(),
                    description: dcr_binary.into(),
                    codec: codec.clone(),
                },
            }
        }
        TrackParams::Audio { params, codec } => match codec {
            AudioCodecParams::Aac(codec) => TrackFormat::Aac {
                params: params.clone(),
                codec: codec.clone(),
            },
            AudioCodecParams::Opus => TrackFormat::Opus {
                params: params.clone(),
            },
        },
        TrackParams::Unrecognized => TrackFormat::Unrecognized,
    };

    format.encode(env)
}
