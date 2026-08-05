use hang::moq_net;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::time::Duration;

use crate::track_format::{ContainerPair, PartialTrackConfig, WireContainer};
use crate::{runtime, session::Session};

struct KindMismatch;

enum Rendition {
    Video(moq_mux::catalog::VideoTrack),
    Audio(moq_mux::catalog::AudioTrack),
}

impl Rendition {
    fn new(
        catalog: &moq_mux::catalog::Producer,
        name: &str,
        config: PartialTrackConfig,
        container: hang::catalog::Container,
    ) -> Self {
        match config {
            PartialTrackConfig::Video(partial) => {
                let mut handle = catalog.reserve().init(name);
                handle.set(partial.with_container(container));
                Self::Video(handle)
            }
            PartialTrackConfig::Audio(partial) => {
                let mut handle = catalog.reserve().init(name);
                handle.set(partial.with_container(container));
                Self::Audio(handle)
            }
        }
    }

    fn set(&mut self, config: PartialTrackConfig) -> Result<(), KindMismatch> {
        match (self, config) {
            (Self::Video(handle), PartialTrackConfig::Video(partial)) => {
                handle.update(|config| *config = partial.with_container(config.container.clone()));
            }
            (Self::Audio(handle), PartialTrackConfig::Audio(partial)) => {
                handle.update(|config| *config = partial.with_container(config.container.clone()));
            }
            (_, _) => return Err(KindMismatch),
        }
        Ok(())
    }
}

type WireProducer = moq_mux::container::Producer<WireContainer>;

struct LiveTrack {
    producer: WireProducer,
    rendition: Rendition,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CreateError {
    #[error("create_broadcast({path}) failed: {source}")]
    Broadcast {
        path: String,
        source: moq_net::Error,
    },
    #[error("CatalogProducer::new failed: {0}")]
    Catalog(moq_net::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AddTrackError {
    #[error("track already exists")]
    AlreadyExists,
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

pub(crate) struct Producer {
    broadcast: moq_net::broadcast::Producer,
    catalog: moq_mux::catalog::Producer,
    tracks: HashMap<String, LiveTrack>,
}

impl Producer {
    pub(crate) fn new(session: &Session, path: &str) -> Result<Self, CreateError> {
        let mut broadcast = {
            // from moq_net::model::Producer::create_broadcast:
            // must be called with a runtime available
            let _guard = runtime().handle().enter();

            session
                .publish
                .create_broadcast(path, moq_net::broadcast::Route::new().with_announce(true))
                .map_err(|source| CreateError::Broadcast {
                    path: path.to_owned(),
                    source,
                })?
        };

        let catalog =
            moq_mux::catalog::Producer::new(&mut broadcast).map_err(CreateError::Catalog)?;

        Ok(Self {
            broadcast,
            catalog,
            tracks: HashMap::new(),
        })
    }

    pub(crate) fn add_track(
        &mut self,
        track: String,
        config: PartialTrackConfig,
        containers: ContainerPair,
        priority: u8,
        latency: Duration,
    ) -> Result<(), AddTrackError> {
        let entry = match self.tracks.entry(track) {
            Entry::Occupied(_) => return Err(AddTrackError::AlreadyExists),
            Entry::Vacant(entry) => entry,
        };

        let live = Self::create_track(
            &mut self.broadcast,
            &self.catalog,
            entry.key(),
            config,
            containers,
            priority,
            latency,
        )?;

        entry.insert(live);

        Ok(())
    }

    pub(crate) fn update_track(
        &mut self,
        track: &str,
        config: PartialTrackConfig,
    ) -> Result<(), UpdateTrackError> {
        self.tracks
            .get_mut(track)
            .ok_or(UpdateTrackError::UnknownTrack)?
            .rendition
            .set(config)
            .map_err(|_kind_mismatch| UpdateTrackError::KindMismatch)
    }

    pub(crate) fn write_frame(
        &mut self,
        track: &str,
        frame: moq_mux::container::Frame,
    ) -> Result<(), WriteFrameError> {
        self.tracks
            .get_mut(track)
            .ok_or(WriteFrameError::UnknownTrack)?
            .producer
            .write(frame)
            .map_err(|e| match e {
                moq_mux::Error::MissingKeyframe(moq_mux::container::MissingKeyframe) => {
                    WriteFrameError::MissingKeyframe
                }
                e => WriteFrameError::Write(e),
            })
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

    pub(crate) fn finish(&mut self) {
        for live in self.tracks.values_mut() {
            let _ = live.producer.finish();
        }
        let _ = self.catalog.finish();
    }

    pub(crate) fn abort(&mut self) {
        self.tracks.clear();
        let _ = self.broadcast.clone().abort(moq_net::Error::Cancel);
    }

    fn create_track(
        broadcast: &mut moq_net::broadcast::Producer,
        catalog: &moq_mux::catalog::Producer,
        track: &str,
        config: PartialTrackConfig,
        containers: ContainerPair,
        priority: u8,
        latency: Duration,
    ) -> Result<LiveTrack, AddTrackError> {
        let track_producer = broadcast
            .create_track(
                track,
                moq_net::track::Info::default().with_priority(priority),
            )
            .map_err(AddTrackError::CreateTrack)?;

        let producer = match catalog.media_producer(track_producer, containers.wire) {
            Ok(producer) => producer.with_latency(latency),
            Err(e) => {
                let _ = broadcast.remove_track(track);
                return Err(AddTrackError::MediaProducer(e));
            }
        };

        let rendition = Rendition::new(catalog, track, config, containers.catalog);

        Ok(LiveTrack {
            producer,
            rendition,
        })
    }
}
