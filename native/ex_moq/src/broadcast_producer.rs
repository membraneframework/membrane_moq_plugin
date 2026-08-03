mod track;

use hang::moq_net;

use std::collections::HashMap;

use crate::{runtime, session::Session};
use track::LiveTrack;

pub(crate) use track::{AddTrackError, UpdateTrackError, WriteFrameError};

pub(crate) struct Producer {
    broadcast: moq_net::broadcast::Producer,
    catalog: moq_mux::catalog::Producer,
    tracks: HashMap<String, LiveTrack>,
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

    pub(crate) fn finish(&mut self) {
        self.tracks.values_mut().for_each(|live| {
            let _ = live.producer.finish();
        });
        let _ = self.catalog.finish();
    }

    pub(crate) fn abort(&mut self) {
        self.tracks.clear();
        let _ = self.broadcast.clone().abort(moq_net::Error::Cancel);
    }
}
