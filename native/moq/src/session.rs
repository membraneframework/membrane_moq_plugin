use rustler::{Atom, LocalPid, NifResult, ResourceArc};
use std::sync::Mutex;
use tokio::sync::mpsc;
use url::Url;

use crate::{atoms, runtime, send_atom};

pub struct SessionResource {
    pub(crate) origin: moq_lite::OriginProducer,
    pub(crate) shutdown: Mutex<Option<mpsc::UnboundedSender<()>>>,
}

impl rustler::Resource for SessionResource {}

/// Connect to a MoQ relay server and prepare the session.
///
/// Builds the origin synchronously so subsequent NIFs can publish broadcasts
/// immediately. The QUIC handshake completes asynchronously; `:moq_connected`
/// is sent to `pid` once the session is up. `:moq_disconnected` is sent if the
/// session closes (clean or with an error).
#[rustler::nif]
pub fn setup_session(
    url: String,
    pid: LocalPid,
    disable_tls_verify: bool,
) -> NifResult<(Atom, ResourceArc<SessionResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;

    // TODO: moq-gst uses moq_lite::Origin::random().produce() instead. Figure out why
    let origin = moq_lite::Origin::produce();
    // TODO: origin creates the following OriginConsumer, which is then _moved_ inside the runtime and bound with the client starting the session
    // Add some description how this works, and why it's enough for the session to just ~exist~ in this thread.
    let consume = origin.consume();

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();
    eprintln!("{disable_tls_verify}");

    runtime().spawn(async move {
        let config = {
            let mut config = moq_native::ClientConfig::default();
            if disable_tls_verify {
                config.tls.disable_verify = Some(true);
            }
            config
        };

        let client = match config.init() {
            Ok(c) => c.with_publish(consume),
            Err(e) => {
                eprintln!("MoQ client init failed: {e}");
                send_atom(pid, atoms::moq_disconnected());
                return;
            }
        };

        let session = match client.connect(url).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("MoQ connect failed: {e}");
                send_atom(pid, atoms::moq_disconnected());
                return;
            }
        };

        send_atom(pid, atoms::moq_connected());

        tokio::select! {
            _ = shutdown_rx.recv() => {}
            result = session.closed() => {
                if let Err(e) = result {
                    eprintln!("MoQ session closed with error: {e}");
                }
                send_atom(pid, atoms::moq_disconnected());
            }
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SessionResource {
            origin,
            shutdown: Mutex::new(Some(shutdown_tx)),
        }),
    ))
}

/// Tear down the session, signaling the connection task to exit.
///
/// Idempotent: subsequent calls are no-ops.
#[rustler::nif]
pub fn close_session(session: ResourceArc<SessionResource>) -> Atom {
    // TODO: lock().unwrap().take() looks like a sequence that can fail in multiple ways, document it better?
    if let Some(tx) = session.shutdown.lock().unwrap().take() {
        let _ = tx.send(());
    }
    atoms::ok()
}
