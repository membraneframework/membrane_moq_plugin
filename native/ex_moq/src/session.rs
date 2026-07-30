use hang::moq_net;

use moq_native::ClientConfig;
use rustler::{Atom, LocalPid, NifResult, OwnedEnv, ResourceArc};
use tokio::task::AbortHandle;
use url::Url;

use crate::{atoms, messages, runtime};

pub(crate) struct SessionResource {
    pub(crate) publish: moq_net::origin::Producer,
    pub(crate) consume: moq_net::origin::Consumer,
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

#[rustler::nif]
pub(crate) fn create_session(
    url: &str,
    pid: LocalPid,
    disable_tls_verify: bool,
) -> NifResult<(Atom, ResourceArc<SessionResource>)> {
    let url = Url::parse(url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;

    let outgoing = moq_net::Origin::random().produce();
    let outgoing_consumer = outgoing.consume();

    let incoming = moq_net::Origin::random().produce();
    let incoming_consumer = incoming.consume();

    let task = runtime().spawn(async move {
        let mut env = OwnedEnv::new();
        match connect(url, outgoing_consumer, incoming, disable_tls_verify).await {
            Ok(session) => {
                messages::send_connected(&mut env, pid);

                let reason = session.closed().await.to_string();
                messages::send_disconnected(&mut env, pid, reason);
            }
            Err(e) => messages::send_setup_failed(&mut env, pid, e),
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SessionResource {
            publish: outgoing,
            consume: incoming_consumer,
            abort: task.abort_handle(),
        }),
    ))
}

#[rustler::nif]
pub(crate) fn close_session(session: ResourceArc<SessionResource>) -> Atom {
    session.abort.abort();
    atoms::ok()
}

async fn connect(
    url: Url,
    publish: moq_net::origin::Consumer,
    subscribe: moq_net::origin::Producer,
    disable_tls_verify: bool,
) -> Result<moq_native::moq_net::Session, String> {
    let mut config = ClientConfig::default();
    config.tls.disable_verify = Some(disable_tls_verify);

    let client = config.init().map_err(|e| e.to_string())?;
    let client = client.with_publisher(publish).with_subscriber(subscribe);

    client.connect(url).await.map_err(|e| e.to_string())
}
