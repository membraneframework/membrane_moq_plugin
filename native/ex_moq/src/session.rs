use hang::moq_net;

use moq_native::ClientConfig;
use rustler::{Atom, LocalPid, NifResult, OwnedEnv, ResourceArc};
use std::sync::Mutex;
use tokio::task::AbortHandle;
use url::Url;

use crate::{atoms, messages, runtime};

pub(crate) struct SessionResource {
    pub(crate) publish: moq_net::OriginProducer,
    pub(crate) consume: Mutex<moq_net::OriginConsumer>,
    /// The task owns the session, so aborting it drops the connection
    /// and guarantees no further messages after a graceful close.
    abort: AbortHandle,
}

impl rustler::Resource for SessionResource {}

impl Drop for SessionResource {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

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

    let task = runtime().spawn(async move {
        let mut env = OwnedEnv::new();
        match connect(url, published, consumed, disable_tls_verify).await {
            Ok(session) => {
                messages::send_connected(&mut env, pid);

                let reason = match session.closed().await {
                    Ok(()) => "MoQ session closed gracefully".to_string(),
                    Err(e) => e.to_string(),
                };
                messages::send_disconnected(&mut env, pid, reason);
            }
            Err(e) => messages::send_setup_failed(&mut env, pid, e),
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SessionResource {
            publish,
            consume: Mutex::new(consume),
            abort: task.abort_handle(),
        }),
    ))
}

#[allow(clippy::needless_pass_by_value)]
#[rustler::nif]
pub(crate) fn close_session(session: ResourceArc<SessionResource>) -> Atom {
    session.abort.abort();
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
