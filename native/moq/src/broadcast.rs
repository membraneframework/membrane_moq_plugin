use hang::moq_net;

use rustler::{Atom, NifResult, Resource, ResourceArc};
use std::sync::Mutex;

use crate::{atoms, runtime, session::SessionResource};

pub(crate) struct BroadcastResource {
    pub(crate) broadcast: Mutex<moq_net::BroadcastProducer>,
    pub(crate) catalog: Mutex<moq_mux::catalog::Producer>,
}

impl Resource for BroadcastResource {}

#[rustler::nif]
pub(crate) fn open_broadcast(
    session: ResourceArc<SessionResource>,
    path: String,
) -> NifResult<(Atom, ResourceArc<BroadcastResource>)> {
    let _guard = runtime().handle().enter();

    let mut bp: moq_net::BroadcastProducer = session
        .origin
        .create_broadcast(&path)
        .ok_or_else(|| crate::nif_error!("create_broadcast({path}) refused"))?;

    let catalog = moq_mux::catalog::Producer::new(&mut bp)
        .map_err(|e| crate::nif_error!("CatalogProducer::new failed: {e}"))?;

    Ok((
        atoms::ok(),
        ResourceArc::new(BroadcastResource {
            broadcast: Mutex::new(bp),
            catalog: Mutex::new(catalog),
        }),
    ))
}

#[rustler::nif]
pub(crate) fn close_broadcast(broadcast_res: ResourceArc<BroadcastResource>) -> Atom {
    let _guard = runtime().handle().enter();
    let _ = broadcast_res.catalog.lock().unwrap().finish();
    let _ = broadcast_res
        .broadcast
        .lock()
        .unwrap()
        .abort(moq_net::Error::Cancel);
    atoms::ok()
}
