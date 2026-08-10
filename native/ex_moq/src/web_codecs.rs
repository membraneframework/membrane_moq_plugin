use rustler::{Decoder, Encoder, Env, NifResult, NifStruct, Term};

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.VideoTrackParams"]
pub(crate) struct VideoTrackParams {
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
    pub(crate) framerate: Option<f64>,
}

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.AudioTrackParams"]
pub(crate) struct AudioTrackParams {
    pub(crate) sample_rate: u32,
    pub(crate) channels: u32,
}

pub(crate) struct H264Codec(pub(crate) hang::catalog::H264);

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.H264Codec"]
struct H264CodecWire {
    in_band: bool,
    profile: u8,
    constraints: u8,
    level: u8,
}

impl Encoder for H264Codec {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        H264CodecWire {
            in_band: self.0.inline,
            profile: self.0.profile,
            constraints: self.0.constraints,
            level: self.0.level,
        }
        .encode(env)
    }
}

impl<'a> Decoder<'a> for H264Codec {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let wire: H264CodecWire = term.decode()?;

        Ok(Self(hang::catalog::H264 {
            inline: wire.in_band,
            profile: wire.profile,
            constraints: wire.constraints,
            level: wire.level,
        }))
    }
}

pub(crate) struct H265Codec(pub(crate) hang::catalog::H265);

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.H265Codec"]
struct H265CodecWire {
    in_band: bool,
    profile_space: u8,
    profile_idc: u8,
    profile_compatibility_flags: Vec<u8>,
    tier_flag: bool,
    level_idc: u8,
    constraint_flags: Vec<u8>,
}

impl Encoder for H265Codec {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        H265CodecWire {
            in_band: self.0.in_band,
            profile_space: self.0.profile_space,
            profile_idc: self.0.profile_idc,
            profile_compatibility_flags: self.0.profile_compatibility_flags.to_vec(),
            tier_flag: self.0.tier_flag,
            level_idc: self.0.level_idc,
            constraint_flags: self.0.constraint_flags.to_vec(),
        }
        .encode(env)
    }
}

impl<'a> Decoder<'a> for H265Codec {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let wire: H265CodecWire = term.decode()?;

        let profile_compatibility_flags: [u8; 4] =
            wire.profile_compatibility_flags.try_into().map_err(|_| {
                crate::nif_error!("profile_compatibility_flags must be exactly 4 bytes")
            })?;

        let constraint_flags: [u8; 6] = wire
            .constraint_flags
            .try_into()
            .map_err(|_| crate::nif_error!("constraint_flags must be exactly 6 bytes"))?;

        Ok(Self(hang::catalog::H265 {
            in_band: wire.in_band,
            profile_space: wire.profile_space,
            profile_idc: wire.profile_idc,
            profile_compatibility_flags,
            tier_flag: wire.tier_flag,
            level_idc: wire.level_idc,
            constraint_flags,
        }))
    }
}

pub(crate) struct AacCodec(pub(crate) hang::catalog::AAC);

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.AACCodec"]
struct AacCodecWire {
    profile: u8,
}

impl Encoder for AacCodec {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        AacCodecWire {
            profile: self.0.profile,
        }
        .encode(env)
    }
}

impl<'a> Decoder<'a> for AacCodec {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let wire: AacCodecWire = term.decode()?;

        Ok(Self(hang::catalog::AAC {
            profile: wire.profile,
        }))
    }
}
pub(crate) struct OpusCodec;

impl Encoder for OpusCodec {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        crate::atoms::opus().encode(env)
    }
}

impl<'a> Decoder<'a> for OpusCodec {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let atom: rustler::Atom = term.decode()?;

        if atom != crate::atoms::opus() {
            return Err(rustler::Error::BadArg);
        }

        Ok(Self)
    }
}
