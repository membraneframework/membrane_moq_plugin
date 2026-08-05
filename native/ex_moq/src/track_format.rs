use bytes::Bytes;
use rustler::{Binary, Env, NewBinary, NifResult, NifTaggedEnum, NifUnitEnum};

use crate::web_codecs::{AacCodec, AudioTrackParams, H264Codec, H265Codec, VideoTrackParams};

pub(crate) type WireContainer = moq_mux::catalog::hang::Container;
pub(crate) type CatalogContainer = hang::catalog::Container;

pub(crate) struct UnrecognizedContainer;

#[derive(NifUnitEnum, Clone, Copy)]
pub(crate) enum Container {
    Legacy,
    Loc,
}

impl TryFrom<&CatalogContainer> for Container {
    type Error = UnrecognizedContainer;

    fn try_from(container: &CatalogContainer) -> Result<Self, Self::Error> {
        match container {
            CatalogContainer::Legacy => Ok(Self::Legacy),
            CatalogContainer::Loc => Ok(Self::Loc),
            _ => Err(UnrecognizedContainer),
        }
    }
}

pub(crate) struct ContainerPair {
    pub(crate) wire: WireContainer,
    pub(crate) catalog: CatalogContainer,
}

impl From<Container> for ContainerPair {
    fn from(container: Container) -> Self {
        match container {
            Container::Legacy => Self {
                wire: WireContainer::Legacy,
                catalog: CatalogContainer::Legacy,
            },
            Container::Loc => Self {
                wire: WireContainer::Loc,
                catalog: CatalogContainer::Loc,
            },
        }
    }
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

pub(crate) struct PartialVideoConfig(hang::catalog::VideoConfig);

impl PartialVideoConfig {
    pub(crate) fn with_container(
        self,
        container: hang::catalog::Container,
    ) -> hang::catalog::VideoConfig {
        let mut config = self.0;
        config.container = container;
        config
    }
}

pub(crate) struct PartialAudioConfig(hang::catalog::AudioConfig);

impl PartialAudioConfig {
    pub(crate) fn with_container(
        self,
        container: hang::catalog::Container,
    ) -> hang::catalog::AudioConfig {
        let mut config = self.0;
        config.container = container;
        config
    }
}

// hang::catalog::*Config types contain both
// track format (may change throughout track's lifetime)
// and container (can't change),
// while ex_moq tries to distinguish them.
//
// Unfortunately, the catalog API happily accepts a config
// with a container different from the old one,
// so we need to ensure it doesn't happen manually.
//
// These wrapper types are supposed to help with this,
// only giving back a hang config when a container is supplied,
// which the format update NIF omits deliberately.
pub(crate) enum PartialTrackConfig {
    Video(PartialVideoConfig),
    Audio(PartialAudioConfig),
}

impl TryFrom<TrackFormat<'_>> for PartialTrackConfig {
    type Error = rustler::Error;

    fn try_from(format: TrackFormat<'_>) -> NifResult<Self> {
        let config = match format {
            TrackFormat::H264 {
                params,
                description,
                codec,
            } => Self::Video(PartialVideoConfig(video_config(
                hang::catalog::VideoCodec::H264((&codec).into()),
                &params,
                description.as_slice(),
            ))),
            TrackFormat::H265 {
                params,
                description,
                codec,
            } => Self::Video(PartialVideoConfig(video_config(
                hang::catalog::VideoCodec::H265(codec.try_into()?),
                &params,
                description.as_slice(),
            ))),
            TrackFormat::Aac { params, codec } => Self::Audio(PartialAudioConfig(
                aac_audio_config(codec.profile, params.sample_rate, params.channels),
            )),
            TrackFormat::Opus { params } => {
                Self::Audio(PartialAudioConfig(hang::catalog::AudioConfig::new(
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
