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
struct H264CodecTerm {
    in_band: bool,
    profile: u8,
    constraints: u8,
    level: u8,
}

impl Encoder for H264Codec {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        H264CodecTerm {
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
        let codec: H264CodecTerm = term.decode()?;

        Ok(Self(hang::catalog::H264 {
            inline: codec.in_band,
            profile: codec.profile,
            constraints: codec.constraints,
            level: codec.level,
        }))
    }
}

pub(crate) struct H265Codec(pub(crate) hang::catalog::H265);

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.H265Codec"]
struct H265CodecTerm {
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
        H265CodecTerm {
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
        let codec: H265CodecTerm = term.decode()?;

        let profile_compatibility_flags: [u8; 4] =
            codec.profile_compatibility_flags.try_into().map_err(|_| {
                crate::nif_error!("profile_compatibility_flags must be exactly 4 bytes")
            })?;

        let constraint_flags: [u8; 6] = codec
            .constraint_flags
            .try_into()
            .map_err(|_| crate::nif_error!("constraint_flags must be exactly 6 bytes"))?;

        Ok(Self(hang::catalog::H265 {
            in_band: codec.in_band,
            profile_space: codec.profile_space,
            profile_idc: codec.profile_idc,
            profile_compatibility_flags,
            tier_flag: codec.tier_flag,
            level_idc: codec.level_idc,
            constraint_flags,
        }))
    }
}

pub(crate) struct AacCodec(pub(crate) hang::catalog::AAC);

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.AACCodec"]
struct AacCodecTerm {
    profile: u8,
}

impl Encoder for AacCodec {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        AacCodecTerm {
            profile: self.0.profile,
        }
        .encode(env)
    }
}

impl<'a> Decoder<'a> for AacCodec {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let codec: AacCodecTerm = term.decode()?;

        Ok(Self(hang::catalog::AAC {
            profile: codec.profile,
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
