use hang::moq_net;

use rustler::{Atom, NifResult, Resource, ResourceArc};
use std::collections::HashMap;
use std::sync::{Mutex, Weak};

use crate::{atoms, lock_ignoring_poison, runtime, session::SessionResource, track::WireProducer};

pub(crate) struct ProducerInner {
    pub(crate) broadcast: moq_net::BroadcastProducer,
    pub(crate) catalog: moq_mux::catalog::Producer,
    pub(crate) tracks: HashMap<String, Weak<Mutex<WireProducer>>>,
}

pub(crate) struct BroadcastProducerResource {
    pub(crate) inner: Mutex<ProducerInner>,
}

impl Resource for BroadcastProducerResource {}

#[allow(clippy::needless_pass_by_value)]
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
            inner: Mutex::new(ProducerInner {
                broadcast: bp,
                catalog,
                tracks: HashMap::new(),
            }),
        }),
    ))
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn close_broadcast_producer(producer: ResourceArc<BroadcastProducerResource>) -> Atom {
    let _guard = runtime().handle().enter();

    let mut inner = lock_ignoring_poison(&producer.inner);

    inner
        .tracks
        .values()
        .filter_map(Weak::upgrade)
        .for_each(|track| {
            let _ = lock_ignoring_poison(&track).finish();
        });

    let _ = inner.catalog.finish();
    let _ = inner.broadcast.abort(moq_net::Error::Cancel);
    atoms::ok()
}
