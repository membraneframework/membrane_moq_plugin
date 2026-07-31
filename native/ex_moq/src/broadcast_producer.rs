mod track;

use hang::moq_net;

use rustler::{Atom, NifResult, Resource, ResourceArc};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::{atoms, runtime, session::SessionResource};
use track::LiveTrack;

struct ProducerInner {
    broadcast: moq_net::broadcast::Producer,
    catalog: moq_mux::catalog::Producer,
    tracks: HashMap<String, LiveTrack>,
}

pub(crate) struct BroadcastProducerResource(Mutex<ProducerInner>);

impl Resource for BroadcastProducerResource {}

#[rustler::nif]
pub(crate) fn create_broadcast_producer(
    session: ResourceArc<SessionResource>,
    path: &str,
) -> NifResult<(Atom, ResourceArc<BroadcastProducerResource>)> {
    let mut broadcast_producer = {
        // from moq_net::model::Producer::create_broadcast:
        // must be called with a runtime available
        let _guard = runtime().handle().enter();

        session
            .publish
            .create_broadcast(path, moq_net::broadcast::Route::new().with_announce(true))
            .map_err(|e| crate::nif_error!("create_broadcast({path}) failed: {e}"))?
    };

    let catalog_producer = moq_mux::catalog::Producer::new(&mut broadcast_producer)
        .map_err(|e| crate::nif_error!("CatalogProducer::new failed: {e}"))?;

    Ok((
        atoms::ok(),
        ResourceArc::new(BroadcastProducerResource(Mutex::new(ProducerInner {
            broadcast: broadcast_producer,
            catalog: catalog_producer,
            tracks: HashMap::new(),
        }))),
    ))
}

#[rustler::nif]
pub(crate) fn close_broadcast_producer(producer: ResourceArc<BroadcastProducerResource>) -> Atom {
    let (mut inner, poisoned) = match producer.0.lock() {
        Ok(guard) => (guard, false),
        Err(poison) => (poison.into_inner(), true),
    };

    if !poisoned {
        inner.tracks.values_mut().for_each(|live| {
            let _ = live.producer.finish();
        });
        let _ = inner.catalog.finish();
    }

    inner.tracks.clear();
    let _ = inner.broadcast.clone().abort(moq_net::Error::Cancel);
    atoms::ok()
}
