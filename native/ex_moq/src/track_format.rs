use bytes::Bytes;
use rustler::{Binary, Env, NewBinary, NifResult, NifStruct, NifTaggedEnum, NifUnitEnum};

pub(crate) struct UnrecognizedContainer;

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

impl From<&hang::catalog::VideoConfig> for VideoTrackParams {
    fn from(config: &hang::catalog::VideoConfig) -> Self {
        Self {
            width: config.coded_width,
            height: config.coded_height,
            framerate: config.framerate,
        }
    }
}

#[derive(NifStruct, Clone)]
#[module = "ExMoQ.Native.AudioTrackParams"]
pub(crate) struct AudioTrackParams {
    pub(crate) sample_rate: u32,
    pub(crate) channels: u32,
}

impl From<&hang::catalog::AudioConfig> for AudioTrackParams {
    fn from(config: &hang::catalog::AudioConfig) -> Self {
        Self {
            sample_rate: config.sample_rate,
            channels: config.channel_count,
        }
    }
}

#[derive(NifStruct, Clone)]
#[module = "ExMoQ.Native.H264Codec"]
pub(crate) struct H264Codec {
    pub(crate) inline: bool,
    pub(crate) profile: u8,
    pub(crate) constraints: u8,
    pub(crate) level: u8,
}

impl From<&hang::catalog::H264> for H264Codec {
    fn from(codec: &hang::catalog::H264) -> Self {
        Self {
            inline: codec.inline,
            profile: codec.profile,
            constraints: codec.constraints,
            level: codec.level,
        }
    }
}

impl From<&H264Codec> for hang::catalog::H264 {
    fn from(codec: &H264Codec) -> Self {
        Self {
            inline: codec.inline,
            profile: codec.profile,
            constraints: codec.constraints,
            level: codec.level,
        }
    }
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

impl From<&hang::catalog::H265> for H265Codec {
    fn from(codec: &hang::catalog::H265) -> Self {
        Self {
            in_band: codec.in_band,
            profile_space: codec.profile_space,
            profile_idc: codec.profile_idc,
            profile_compatibility_flags: codec.profile_compatibility_flags.to_vec(),
            tier_flag: codec.tier_flag,
            level_idc: codec.level_idc,
            constraint_flags: codec.constraint_flags.to_vec(),
        }
    }
}

impl TryFrom<H265Codec> for hang::catalog::H265 {
    type Error = rustler::Error;

    fn try_from(codec: H265Codec) -> NifResult<Self> {
        let profile_compatibility_flags: [u8; 4] =
            codec.profile_compatibility_flags.try_into().map_err(|_| {
                crate::nif_error!("profile_compatibility_flags must be exactly 4 bytes")
            })?;

        let constraint_flags: [u8; 6] = codec
            .constraint_flags
            .try_into()
            .map_err(|_| crate::nif_error!("constraint_flags must be exactly 6 bytes"))?;

        Ok(Self {
            in_band: codec.in_band,
            profile_space: codec.profile_space,
            profile_idc: codec.profile_idc,
            profile_compatibility_flags,
            tier_flag: codec.tier_flag,
            level_idc: codec.level_idc,
            constraint_flags,
        })
    }
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
        let dcr = config.description.as_deref().unwrap_or_default();
        let mut description = NewBinary::new(env, dcr.len());
        description.as_mut_slice().copy_from_slice(dcr);
        let description = description.into();
        let params = config.into();

        match &config.codec {
            hang::catalog::VideoCodec::H264(codec) => Self::H264 {
                params,
                description,
                codec: codec.into(),
            },
            hang::catalog::VideoCodec::H265(codec) => Self::H265 {
                params,
                description,
                codec: codec.into(),
            },
            _ => Self::Unrecognized,
        }
    }

    pub(crate) fn from_audio(config: &hang::catalog::AudioConfig) -> Self {
        let params = config.into();

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

pub(crate) struct VideoFormat(hang::catalog::VideoConfig);

impl VideoFormat {
    pub(crate) fn with_container(
        self,
        container: hang::catalog::Container,
    ) -> hang::catalog::VideoConfig {
        let mut config = self.0;
        config.container = container;
        config
    }
}

pub(crate) struct AudioFormat(hang::catalog::AudioConfig);

impl AudioFormat {
    pub(crate) fn with_container(
        self,
        container: hang::catalog::Container,
    ) -> hang::catalog::AudioConfig {
        let mut config = self.0;
        config.container = container;
        config
    }
}

pub(crate) enum TrackConfig {
    Video(VideoFormat),
    Audio(AudioFormat),
}

impl TryFrom<TrackFormat<'_>> for TrackConfig {
    type Error = rustler::Error;

    fn try_from(format: TrackFormat<'_>) -> NifResult<Self> {
        let config = match format {
            TrackFormat::H264 {
                params,
                description,
                codec,
            } => Self::Video(VideoFormat(video_config(
                hang::catalog::VideoCodec::H264((&codec).into()),
                &params,
                description.as_slice(),
            ))),
            TrackFormat::H265 {
                params,
                description,
                codec,
            } => Self::Video(VideoFormat(video_config(
                hang::catalog::VideoCodec::H265(codec.try_into()?),
                &params,
                description.as_slice(),
            ))),
            TrackFormat::Aac { params, codec } => Self::Audio(AudioFormat(aac_audio_config(
                codec.profile,
                params.sample_rate,
                params.channels,
            ))),
            TrackFormat::Opus { params } => {
                Self::Audio(AudioFormat(hang::catalog::AudioConfig::new(
                    hang::catalog::AudioCodec::Opus,
                    params.sample_rate,
                    params.channels,
                )))
            }
            TrackFormat::Unrecognized => {
                return Err(crate::nif_error!(
                    "cannot publish an unrecognized track format"
                ));
            }
        };
        Ok(config)
    }
}

fn video_config(
    codec: hang::catalog::VideoCodec,
    params: &VideoTrackParams,
    dcr: &[u8],
) -> hang::catalog::VideoConfig {
    let mut config = hang::catalog::VideoConfig::new(codec);
    config.description = (!dcr.is_empty()).then(|| Bytes::copy_from_slice(dcr));
    config.coded_width = params.width;
    config.coded_height = params.height;
    config.framerate = params.framerate;
    config.optimize_for_latency = Some(true);
    config
}

fn aac_audio_config(profile: u8, sample_rate: u32, channels: u32) -> hang::catalog::AudioConfig {
    let codec = hang::catalog::AudioCodec::AAC(hang::catalog::AAC { profile });
    let mut config = hang::catalog::AudioConfig::new(codec, sample_rate, channels);

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
