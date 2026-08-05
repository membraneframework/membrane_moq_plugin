use rustler::{NifResult, NifStruct};

#[derive(NifStruct, Clone)]
#[module = "ExMoQ.Native.WebCodecs.VideoTrackParams"]
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
#[module = "ExMoQ.Native.WebCodecs.AudioTrackParams"]
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
#[module = "ExMoQ.Native.WebCodecs.H264Codec"]
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
#[module = "ExMoQ.Native.WebCodecs.H265Codec"]
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
#[module = "ExMoQ.Native.WebCodecs.AACCodec"]
pub(crate) struct AacCodec {
    pub(crate) profile: u8,
}
