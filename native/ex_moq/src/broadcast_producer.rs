use hang::moq_net;

use rustler::{Atom, NifResult, Resource, ResourceArc};
use std::sync::{Mutex, Weak};

use crate::{atoms, lock_ignoring_poison, runtime, session::SessionResource, track::WireProducer};

pub(crate) struct ProducerInner {
    pub(crate) broadcast: moq_net::broadcast::Producer,
    pub(crate) catalog: moq_mux::catalog::Producer,
    /// Weak handles to the live wire producers,
    /// so closing the broadcast can finish them without keeping them alive.
    pub(crate) tracks: Vec<Weak<Mutex<WireProducer>>>,
}

pub(crate) struct BroadcastProducerResource {
    pub(crate) inner: Mutex<ProducerInner>,
}

impl Resource for BroadcastProducerResource {}

#[rustler::nif]
pub(crate) fn create_broadcast_producer(
    session: ResourceArc<SessionResource>,
    path: &str,
) -> NifResult<(Atom, ResourceArc<BroadcastProducerResource>)> {
    let _guard = runtime().handle().enter();

    let mut broadcast_producer = session
        .publish
        .create_broadcast(path, moq_net::broadcast::Route::new().with_announce(true))
        .map_err(|e| crate::nif_error!("create_broadcast({path}) failed: {e}"))?;

    let catalog_producer = moq_mux::catalog::Producer::new(&mut broadcast_producer)
        .map_err(|e| crate::nif_error!("CatalogProducer::new failed: {e}"))?;

    Ok((
        atoms::ok(),
        ResourceArc::new(BroadcastProducerResource {
            inner: Mutex::new(ProducerInner {
                broadcast: broadcast_producer,
                catalog: catalog_producer,
                tracks: Vec::new(),
            }),
        }),
    ))
}

#[rustler::nif]
pub(crate) fn close_broadcast_producer(producer: ResourceArc<BroadcastProducerResource>) -> Atom {
    let _guard = runtime().handle().enter();

    let mut inner = lock_ignoring_poison(&producer.inner);

    inner
        .tracks
        .drain(..)
        .filter_map(|weak| weak.upgrade())
        .for_each(|track| {
            let _ = lock_ignoring_poison(&track).finish();
        });

    let _ = inner.catalog.finish();
    let _ = inner.broadcast.clone().abort(moq_net::Error::Cancel);
    atoms::ok()
}
