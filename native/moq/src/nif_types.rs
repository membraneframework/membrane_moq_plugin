use rustler::NifStruct;

#[derive(NifStruct)]
#[module = "Membrane.MoQ.Native.VideoTrackParams"]
pub(crate) struct VideoTrackParams {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) framerate: f64,
}

#[derive(NifStruct)]
#[module = "Membrane.MoQ.Native.H264Codec"]
pub(crate) struct H264Codec {
    pub(crate) inline: bool,
    pub(crate) profile: u8,
    pub(crate) constraints: u8,
    pub(crate) level: u8,
}

#[derive(NifStruct)]
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
