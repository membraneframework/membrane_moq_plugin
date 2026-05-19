use rustler::{Atom, NifResult, ResourceArc};
use std::sync::Mutex;

use crate::{atoms, runtime, session::SessionResource};

pub struct BroadcastResource {
    pub(crate) broadcast: Mutex<moq_lite::BroadcastProducer>,
    pub(crate) catalog: Mutex<moq_mux::CatalogProducer>,
}

impl rustler::Resource for BroadcastResource {}

/// Open a new broadcast on this session and create its (hang + MSF) catalog.
///
/// Path uniqueness is enforced by moq-lite — calling twice with the same path
/// returns an error.
#[rustler::nif]
pub fn open_broadcast(
    session: ResourceArc<SessionResource>,
    path: String,
) -> NifResult<(Atom, ResourceArc<BroadcastResource>)> {
    // moq-lite's create_broadcast spawns a tokio task to track the
    // broadcast lifetime, so we need a runtime context.
    let _guard = runtime().handle().enter();

    let mut bp = session.origin.create_broadcast(&path).ok_or_else(|| {
        crate::nif_error!("create_broadcast({path}) refused")
    })?;

    let catalog = moq_mux::CatalogProducer::new(&mut bp)
        .map_err(|e| crate::nif_error!("CatalogProducer::new failed: {e}"))?;

    Ok((
        atoms::ok(),
        ResourceArc::new(BroadcastResource {
            broadcast: Mutex::new(bp),
            catalog: Mutex::new(catalog),
        }),
    ))
}

/// Close the broadcast, aborting any in-flight tracks and unannouncing it.
#[rustler::nif]
pub fn close_broadcast(broadcast_res: ResourceArc<BroadcastResource>) -> Atom {
    let _guard = runtime().handle().enter();
    let _ = broadcast_res.catalog.lock().unwrap().finish();
    let _ = broadcast_res
        .broadcast
        .lock()
        .unwrap()
        .abort(moq_lite::Error::Cancel);
    atoms::ok()
}
