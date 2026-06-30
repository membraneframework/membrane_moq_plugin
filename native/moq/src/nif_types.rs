use rustler::{Binary, NifStruct, NifTaggedEnum};

#[derive(NifStruct, Clone)]
#[module = "Membrane.MoQ.Native.VideoTrackParams"]
pub(crate) struct VideoTrackParams {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) framerate: f64,
}

#[derive(NifStruct, Clone)]
#[module = "Membrane.MoQ.Native.AudioTrackParams"]
pub(crate) struct AudioTrackParams {
    pub(crate) sample_rate: u32,
    pub(crate) channels: u32,
}

#[derive(NifStruct, Clone)]
#[module = "Membrane.MoQ.Native.H264Codec"]
pub(crate) struct H264Codec {
    pub(crate) inline: bool,
    pub(crate) profile: u8,
    pub(crate) constraints: u8,
    pub(crate) level: u8,
}

#[derive(NifStruct, Clone)]
#[module = "Membrane.MoQ.Native.H265Codec"]
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
#[module = "Membrane.MoQ.Native.AACCodec"]
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
