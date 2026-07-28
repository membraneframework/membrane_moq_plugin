use bytes::Bytes;
use rustler::{Binary, Env, NewBinary, NifResult, NifStruct, NifTaggedEnum, NifUnitEnum};

#[derive(NifUnitEnum, Clone, Copy)]
pub(crate) enum Container {
    Legacy,
    Loc,
}

impl From<Container> for hang::catalog::Container {
    fn from(container: Container) -> Self {
        match container {
            Container::Legacy => Self::Legacy,
            Container::Loc => Self::Loc,
        }
    }
}

pub(crate) struct UnrecognizedContainer;

impl TryFrom<&hang::catalog::Container> for Container {
    type Error = UnrecognizedContainer;

    fn try_from(container: &hang::catalog::Container) -> Result<Self, Self::Error> {
        match container {
            hang::catalog::Container::Legacy => Ok(Self::Legacy),
            hang::catalog::Container::Loc => Ok(Self::Loc),
            _ => Err(UnrecognizedContainer),
        }
    }
}

#[derive(NifStruct, Clone)]
#[module = "ExMoQ.Native.VideoTrackParams"]
pub(crate) struct VideoTrackParams {
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
    pub(crate) framerate: Option<f64>,
}

#[derive(NifStruct, Clone)]
#[module = "ExMoQ.Native.AudioTrackParams"]
pub(crate) struct AudioTrackParams {
    pub(crate) sample_rate: u32,
    pub(crate) channels: u32,
}

#[derive(NifStruct, Clone)]
#[module = "ExMoQ.Native.H264Codec"]
pub(crate) struct H264Codec {
    pub(crate) inline: bool,
    pub(crate) profile: u8,
    pub(crate) constraints: u8,
    pub(crate) level: u8,
}

#[derive(NifStruct, Clone)]
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

#[derive(NifStruct, Clone)]
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

impl<'a> TrackFormat<'a> {
    pub(crate) fn from_video(env: Env<'a>, config: &hang::catalog::VideoConfig) -> Self {
        // Decoder configuration record (avcC/hvcC), empty when carried in-band.
        let dcr = config.description.as_deref().unwrap_or_default();
        let mut description = NewBinary::new(env, dcr.len());
        description.as_mut_slice().copy_from_slice(dcr);

        let params = VideoTrackParams {
            width: config.coded_width,
            height: config.coded_height,
            framerate: config.framerate,
        };

        match &config.codec {
            hang::catalog::VideoCodec::H264(h) => Self::H264 {
                params,
                description: description.into(),
                codec: H264Codec {
                    inline: h.inline,
                    profile: h.profile,
                    constraints: h.constraints,
                    level: h.level,
                },
            },
            hang::catalog::VideoCodec::H265(h) => Self::H265 {
                params,
                description: description.into(),
                codec: H265Codec {
                    in_band: h.in_band,
                    profile_space: h.profile_space,
                    profile_idc: h.profile_idc,
                    profile_compatibility_flags: h.profile_compatibility_flags.to_vec(),
                    tier_flag: h.tier_flag,
                    level_idc: h.level_idc,
                    constraint_flags: h.constraint_flags.to_vec(),
                },
            },
            _ => Self::Unrecognized,
        }
    }

    pub(crate) fn from_audio(config: &hang::catalog::AudioConfig) -> Self {
        let params = AudioTrackParams {
            sample_rate: config.sample_rate,
            channels: config.channel_count,
        };

        match &config.codec {
            hang::catalog::AudioCodec::AAC(aac) => Self::Aac {
                params,
                codec: AacCodec {
                    profile: aac.profile,
                },
            },
            hang::catalog::AudioCodec::Opus => Self::Opus { params },
            _ => Self::Unrecognized,
        }
    }
}

pub(crate) enum ResolvedConfig {
    Video(hang::catalog::VideoConfig),
    Audio(hang::catalog::AudioConfig),
}

impl ResolvedConfig {
    pub(crate) fn new(
        format: TrackFormat<'_>,
        container: hang::catalog::Container,
    ) -> NifResult<Self> {
        let config = match format {
            TrackFormat::H264 {
                params,
                description,
                codec,
            } => Self::Video(h264_video_config(
                &params,
                description.as_slice(),
                &codec,
                container,
            )),
            TrackFormat::H265 {
                params,
                description,
                codec,
            } => Self::Video(h265_video_config(
                &params,
                description.as_slice(),
                codec,
                container,
            )?),
            TrackFormat::Aac { params, codec } => Self::Audio(aac_audio_config(
                codec.profile,
                params.sample_rate,
                params.channels,
                container,
            )),
            TrackFormat::Opus { params } => Self::Audio(opus_audio_config(
                params.sample_rate,
                params.channels,
                container,
            )),
            TrackFormat::Unrecognized => {
                return Err(crate::nif_error!(
                    "cannot publish an unrecognized track format"
                ));
            }
        };
        Ok(config)
    }
}

fn h264_video_config(
    video_params: &VideoTrackParams,
    dcr: &[u8],
    codec: &H264Codec,
    container: hang::catalog::Container,
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
        container,
    )
}

fn h265_video_config(
    video_params: &VideoTrackParams,
    dcr: &[u8],
    codec: H265Codec,
    container: hang::catalog::Container,
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
        container,
    ))
}

fn aac_audio_config(
    profile: u8,
    sample_rate: u32,
    channels: u32,
    container: hang::catalog::Container,
) -> hang::catalog::AudioConfig {
    let codec = hang::catalog::AudioCodec::AAC(hang::catalog::AAC { profile });
    let mut config = hang::catalog::AudioConfig::new(codec, sample_rate, channels);
    config.container = container;

    // WebCodecs-convention decoders treat a description-less mp4a.40.x rendition as ADTS;
    // MoQ requires raw AAC frames, so we include the AudioSpecificConfig in the catalog.
    config.description = Some(
        moq_mux::codec::aac::Config {
            profile,
            sample_rate,
            channel_count: channels,
        }
        .encode(),
    );

    config
}

fn opus_audio_config(
    sample_rate: u32,
    channels: u32,
    container: hang::catalog::Container,
) -> hang::catalog::AudioConfig {
    let mut config =
        hang::catalog::AudioConfig::new(hang::catalog::AudioCodec::Opus, sample_rate, channels);
    config.container = container;
    config
}

fn create_video_config(
    codec: hang::catalog::VideoCodec,
    width: Option<u32>,
    height: Option<u32>,
    framerate: Option<f64>,
    description: Option<Bytes>,
    container: hang::catalog::Container,
) -> hang::catalog::VideoConfig {
    let mut config = hang::catalog::VideoConfig::new(codec);
    config.container = container;
    config.description = description;
    config.coded_width = width;
    config.coded_height = height;
    config.framerate = framerate;
    config.optimize_for_latency = Some(true);
    config
}
