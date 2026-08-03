use std::time::Duration;

use hang::moq_net;

use super::Producer;
use crate::track_format::ResolvedConfig;

type WireProducer = moq_mux::container::Producer<moq_mux::catalog::hang::Container>;

enum Rendition {
    Video(moq_mux::catalog::VideoTrack),
    Audio(moq_mux::catalog::AudioTrack),
}

struct KindMismatch;

impl Rendition {
    fn new(catalog: &moq_mux::catalog::Producer, name: &str, config: ResolvedConfig) -> Self {
        match config {
            ResolvedConfig::Video(config) => {
                let mut handle = catalog.reserve().video(name);
                handle.set(config);
                Self::Video(handle)
            }
            ResolvedConfig::Audio(config) => {
                let mut handle = catalog.reserve().audio(name);
                handle.set(config);
                Self::Audio(handle)
            }
        }
    }

    fn set(&mut self, config: ResolvedConfig) -> Result<(), KindMismatch> {
        match (self, config) {
            (Self::Video(handle), ResolvedConfig::Video(config)) => handle.set(config),
            (Self::Audio(handle), ResolvedConfig::Audio(config)) => handle.set(config),
            (_, _) => return Err(KindMismatch),
        }
        Ok(())
    }
}

pub(super) struct LiveTrack {
    pub(super) producer: WireProducer,
    rendition: Rendition,
    container: hang::catalog::Container,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AddTrackError {
    #[error("track already exists")]
    AlreadyExists,
    #[error("container init failed: {0}")]
    ContainerInit(moq_mux::Error),
    #[error("create_track failed: {0}")]
    CreateTrack(moq_net::Error),
    #[error("media_producer failed: {0}")]
    MediaProducer(moq_mux::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpdateTrackError {
    #[error("unknown track")]
    UnknownTrack,
    #[error("cannot change a track's media kind in place")]
    KindMismatch,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum WriteFrameError {
    #[error("unknown track")]
    UnknownTrack,
    #[error("missing keyframe")]
    MissingKeyframe,
    #[error("writing frame failed: {0}")]
    Write(moq_mux::Error),
}

impl Producer {
    pub(crate) fn add_track(
        &mut self,
        track: String,
        config: ResolvedConfig,
        container: hang::catalog::Container,
        priority: u8,
        latency: Duration,
    ) -> Result<(), AddTrackError> {
        let wire_container = (&container)
            .try_into()
            .map_err(AddTrackError::ContainerInit)?;

        if self.tracks.contains_key(&track) {
            return Err(AddTrackError::AlreadyExists);
        }

        let track_producer = self
            .broadcast
            .create_track(
                track.as_str(),
                moq_net::track::Info::default().with_priority(priority),
            )
            .map_err(AddTrackError::CreateTrack)?;

        let rendition = Rendition::new(
            &self.catalog,
            &track,
            config.with_container(container.clone()),
        );

        let producer = self
            .catalog
            .media_producer(track_producer, wire_container)
            .map_err(AddTrackError::MediaProducer)?
            .with_latency(latency);

        self.tracks.insert(
            track,
            LiveTrack {
                producer,
                rendition,
                container,
            },
        );

        Ok(())
    }

    pub(crate) fn update_track(
        &mut self,
        track: &str,
        config: ResolvedConfig,
    ) -> Result<(), UpdateTrackError> {
        let Some(live) = self.tracks.get_mut(track) else {
            return Err(UpdateTrackError::UnknownTrack);
        };

        live.rendition
            .set(config.with_container(live.container.clone()))
            .map_err(|_kind_mismatch| UpdateTrackError::KindMismatch)
    }

    pub(crate) fn write_frame(
        &mut self,
        track: &str,
        frame: moq_mux::container::Frame,
    ) -> Result<(), WriteFrameError> {
        let Some(live) = self.tracks.get_mut(track) else {
            return Err(WriteFrameError::UnknownTrack);
        };

        match live.producer.write(frame) {
            Ok(()) => Ok(()),
            Err(moq_mux::Error::MissingKeyframe(moq_mux::container::MissingKeyframe)) => {
                Err(WriteFrameError::MissingKeyframe)
            }
            Err(e) => Err(WriteFrameError::Write(e)),
        }
    }

    pub(crate) fn remove_track(&mut self, track: &str, finish: bool) {
        let Some(mut live) = self.tracks.remove(track) else {
            return;
        };

        if finish {
            let _ = live.producer.finish();
        }
        let _ = self.broadcast.remove_track(track);
    }
}
