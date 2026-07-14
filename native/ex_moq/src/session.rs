use hang::moq_net;

use moq_native::ClientConfig;
use rustler::{Atom, LocalPid, NifResult, OwnedEnv, ResourceArc};
use std::sync::Mutex;
use tokio::sync::mpsc;
use url::Url;

use crate::{atoms, messages, runtime};

pub(crate) struct SessionResource {
    pub(crate) publish: moq_net::OriginProducer,
    pub(crate) consume: Mutex<moq_net::OriginConsumer>,
    shutdown: mpsc::UnboundedSender<()>,
}

impl rustler::Resource for SessionResource {}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn create_session(
    url: String,
    pid: LocalPid,
    disable_tls_verify: bool,
) -> NifResult<(Atom, ResourceArc<SessionResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;

    let publish = moq_net::Origin::random().produce();
    let published = publish.consume();

    let consumed = moq_net::Origin::random().produce();
    let consume = consumed.consume();

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let session = tokio::select! {
            session = connect(url, published, consumed, disable_tls_verify) => session,
            _ = shutdown_rx.recv() => return,
        };
        let mut env = OwnedEnv::new();
        match session {
            Ok(session) => {
                messages::send_connected(&mut env, pid);

                tokio::select! {
                    _ = shutdown_rx.recv() => {} // session closed gracefully by parent
                    result = session.closed() => {
                        let reason = match result {
                            Ok(()) => "MoQ session closed gracefully".to_string(),
                            Err(e) => e.to_string(),
                        };
                        messages::send_disconnected(&mut env, pid, reason);
                    }
                }
            }
            Err(e) => messages::send_setup_failed(&mut env, pid, e),
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SessionResource {
            publish,
            consume: Mutex::new(consume),
            shutdown: shutdown_tx,
        }),
    ))
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn close_session(session: ResourceArc<SessionResource>) -> Atom {
    let _ = session.shutdown.send(());
    atoms::ok()
}

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn connect(
    url: Url,
    published: moq_net::OriginConsumer,
    consumed: moq_net::OriginProducer,
    disable_tls_verify: bool,
) -> Result<moq_native::moq_net::Session, String> {
    let mut config = ClientConfig::default();
    config.tls.disable_verify = Some(disable_tls_verify);

    let client = config.init().map_err(|e| e.to_string())?;
    let client = client.with_publish(published).with_consume(consumed);

    match tokio::time::timeout(CONNECT_TIMEOUT, client.connect(url)).await {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(_elapsed) => Err(format!(
            "connecting to the relay timed out after {CONNECT_TIMEOUT:?}"
        )),
    }
}
