use rustler::NifStruct;

#[derive(NifStruct)]
#[module = "Membrane.MoQ.Native.VideoTrackParams"]
pub struct VideoTrackParams {
    pub width: u32,
    pub height: u32,
    pub framerate: f64,
}

#[derive(NifStruct)]
#[module = "Membrane.MoQ.Native.H264Codec"]
pub struct H264Codec {
    pub inline: bool,
    pub profile: u8,
    pub constraints: u8,
    pub level: u8,
}

#[derive(NifStruct)]
#[module = "Membrane.MoQ.Native.H265Codec"]
pub struct H265Codec {
    pub in_band: bool,
    pub profile_space: u8,
    pub profile_idc: u8,
    pub profile_compatibility_flags: Vec<u8>,
    pub tier_flag: bool,
    pub level_idc: u8,
    pub constraint_flags: Vec<u8>,
}
