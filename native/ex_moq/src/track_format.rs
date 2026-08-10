use bytes::Bytes;
use rustler::{
    Binary, Decoder, Encoder, Env, NewBinary, NifResult, NifStruct, NifUnitEnum, NifUntaggedEnum,
    Term,
};

use crate::web_codecs;

pub(crate) type WireContainer = moq_mux::catalog::hang::Container;
pub(crate) type CatalogContainer = hang::catalog::Container;

#[derive(NifUnitEnum, Clone, Copy)]
pub(crate) enum Container {
    Legacy,
    Loc,
}

impl From<Container> for CatalogContainer {
    fn from(container: Container) -> Self {
        match container {
            Container::Legacy => Self::Legacy,
            Container::Loc => Self::Loc,
        }
    }
}

impl From<Container> for WireContainer {
    fn from(container: Container) -> Self {
        match container {
            Container::Legacy => Self::Legacy,
            Container::Loc => Self::Loc,
        }
    }
}

pub(crate) struct Description(pub(crate) Bytes);

impl Encoder for Description {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let mut binary = NewBinary::new(env, self.0.len());
        binary.as_mut_slice().copy_from_slice(&self.0);
        Binary::from(binary).encode(env)
    }
}

impl<'a> Decoder<'a> for Description {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let binary: Binary = term.decode()?;
        Ok(Self(Bytes::copy_from_slice(binary.as_slice())))
    }
}

#[derive(NifUntaggedEnum)]
pub(crate) enum VideoCodec {
    H264(web_codecs::H264Codec),
    H265(web_codecs::H265Codec),
}

#[derive(NifUntaggedEnum)]
pub(crate) enum AudioCodec {
    Aac(web_codecs::AacCodec),
    Opus(web_codecs::OpusCodec),
}

pub(crate) struct UnrecognizedFormat;

impl Encoder for UnrecognizedFormat {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        crate::atoms::unrecognized().encode(env)
    }
}

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.VideoTrackFormat"]
pub(crate) struct VideoTrackFormat {
    pub(crate) params: web_codecs::VideoTrackParams,
    pub(crate) description: Description,
    pub(crate) codec: VideoCodec,
}

impl TryFrom<&hang::catalog::VideoConfig> for VideoTrackFormat {
    type Error = UnrecognizedFormat;

    fn try_from(config: &hang::catalog::VideoConfig) -> Result<Self, Self::Error> {
        let codec = match &config.codec {
            hang::catalog::VideoCodec::H264(codec) => {
                VideoCodec::H264(web_codecs::H264Codec(codec.clone()))
            }
            hang::catalog::VideoCodec::H265(codec) => {
                VideoCodec::H265(web_codecs::H265Codec(codec.clone()))
            }
            _ => return Err(UnrecognizedFormat),
        };

        Ok(Self {
            params: web_codecs::VideoTrackParams {
                width: config.coded_width,
                height: config.coded_height,
                framerate: config.framerate,
            },
            description: Description(config.description.clone().unwrap_or_default()),
            codec,
        })
    }
}

pub(crate) fn video_config(
    format: VideoTrackFormat,
    container: CatalogContainer,
) -> hang::catalog::VideoConfig {
    let codec = match format.codec {
        VideoCodec::H264(codec) => hang::catalog::VideoCodec::H264(codec.0),
        VideoCodec::H265(codec) => hang::catalog::VideoCodec::H265(codec.0),
    };

    let mut config = hang::catalog::VideoConfig::new(codec);
    config.description = (!format.description.0.is_empty()).then_some(format.description.0);
    config.coded_width = format.params.width;
    config.coded_height = format.params.height;
    config.framerate = format.params.framerate;
    config.optimize_for_latency = Some(true);
    config.container = container;
    config
}

#[derive(NifStruct)]
#[module = "ExMoQ.Native.WebCodecs.AudioTrackFormat"]
pub(crate) struct AudioTrackFormat {
    pub(crate) params: web_codecs::AudioTrackParams,
    pub(crate) codec: AudioCodec,
}

impl TryFrom<&hang::catalog::AudioConfig> for AudioTrackFormat {
    type Error = UnrecognizedFormat;

    fn try_from(config: &hang::catalog::AudioConfig) -> Result<Self, Self::Error> {
        let codec = match &config.codec {
            hang::catalog::AudioCodec::AAC(codec) => {
                AudioCodec::Aac(web_codecs::AacCodec(codec.clone()))
            }
            hang::catalog::AudioCodec::Opus => AudioCodec::Opus(web_codecs::OpusCodec),
            _ => return Err(UnrecognizedFormat),
        };

        Ok(Self {
            params: web_codecs::AudioTrackParams {
                sample_rate: config.sample_rate,
                channels: config.channel_count,
            },
            codec,
        })
    }
}

pub(crate) fn audio_config(
    format: AudioTrackFormat,
    container: CatalogContainer,
) -> hang::catalog::AudioConfig {
    let mut config = match format.codec {
        AudioCodec::Aac(codec) => {
            let profile = codec.0.profile;
            let mut config = hang::catalog::AudioConfig::new(
                codec.0,
                format.params.sample_rate,
                format.params.channels,
            );

            // WebCodecs-convention decoders treat a description-less mp4a.40.x rendition as ADTS;
            // MoQ requires raw AAC frames, so we include the AudioSpecificConfig in the catalog.
            config.description = Some(
                moq_mux::codec::aac::Config {
                    profile,
                    sample_rate: format.params.sample_rate,
                    channel_count: format.params.channels,
                }
                .encode(),
            );

            config
        }
        AudioCodec::Opus(_codec) => hang::catalog::AudioConfig::new(
            hang::catalog::AudioCodec::Opus,
            format.params.sample_rate,
            format.params.channels,
        ),
    };

    config.container = container;
    config
}

#[derive(NifUntaggedEnum)]
pub(crate) enum TrackFormat {
    Video(VideoTrackFormat),
    Audio(AudioTrackFormat),
}
