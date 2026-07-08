use hang::moq_net;

use rustler::{Atom, NifResult, Resource, ResourceArc};
use std::collections::HashMap;
use std::sync::{Mutex, Weak};

use crate::{atoms, runtime, session::SessionResource, track::WireProducer};

pub(crate) struct BroadcastProducerResource {
    pub(crate) broadcast: Mutex<moq_net::BroadcastProducer>,
    pub(crate) catalog: Mutex<moq_mux::catalog::Producer>,
    pub(crate) tracks: Mutex<HashMap<String, Weak<Mutex<WireProducer>>>>,
}

impl Resource for BroadcastProducerResource {}

#[rustler::nif]
pub(crate) fn create_broadcast_producer(
    session: ResourceArc<SessionResource>,
    path: String,
) -> NifResult<(Atom, ResourceArc<BroadcastProducerResource>)> {
    let _guard = runtime().handle().enter();

    let mut bp: moq_net::BroadcastProducer = session
        .publish
        .create_broadcast(&path)
        .ok_or_else(|| crate::nif_error!("create_broadcast({path}) refused"))?;

    let catalog = moq_mux::catalog::Producer::new(&mut bp)
        .map_err(|e| crate::nif_error!("CatalogProducer::new failed: {e}"))?;

    Ok((
        atoms::ok(),
        ResourceArc::new(BroadcastProducerResource {
            broadcast: Mutex::new(bp),
            catalog: Mutex::new(catalog),
            tracks: Mutex::new(HashMap::new()),
        }),
    ))
}

#[rustler::nif]
pub(crate) fn close_broadcast_producer(producer: ResourceArc<BroadcastProducerResource>) -> Atom {
    let _guard = runtime().handle().enter();

    producer
        .tracks
        .lock()
        .unwrap()
        .values()
        .filter_map(Weak::upgrade)
        .for_each(|track| {
            let _ = track.lock().unwrap().finish();
        });

    let _ = producer.catalog.lock().unwrap().finish();
    let _ = producer
        .broadcast
        .lock()
        .unwrap()
        .abort(moq_net::Error::Cancel);
    atoms::ok()
}
