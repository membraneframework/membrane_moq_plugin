use rustler::{Atom, LocalPid, ResourceArc};
use tokio::sync::mpsc;

use crate::{atoms, runtime};

pub(crate) struct SubscriberResource {
    pub(crate) tx: mpsc::UnboundedSender<()>,
}

impl rustler::Resource for SubscriberResource {}

#[rustler::nif]
pub(crate) fn start_subscriber(
    _url: String,
    _broadcast: String,
    _track: String,
    _pid: LocalPid,
) -> (Atom, ResourceArc<SubscriberResource>) {
    let (stop_tx, mut stop_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let _ = stop_rx.recv().await;
    });

    (
        atoms::ok(),
        ResourceArc::new(SubscriberResource { tx: stop_tx }),
    )
}

#[rustler::nif]
pub(crate) fn stop_subscriber(resource: ResourceArc<SubscriberResource>) -> Atom {
    let _ = resource.tx.send(());
    atoms::ok()
}
