use hang::moq_net::{
    self,
    origin::{Consumer, Producer},
};

use moq_native::ClientConfig;
use rustler::{LocalPid, OwnedEnv};
use tokio::task::AbortHandle;
use url::Url;

use crate::{messages, runtime};

pub(crate) struct Session {
    pub(crate) publish: Producer,
    pub(crate) consume: Consumer,
    abort: AbortHandle,
}

impl Session {
    pub(crate) fn connect(url: Url, pid: LocalPid, disable_tls_verify: bool) -> Self {
        let outgoing = moq_net::Origin::random().produce();
        let outgoing_consumer = outgoing.consume();

        let incoming = moq_net::Origin::random().produce();
        let incoming_consumer = incoming.consume();

        let task = runtime().spawn(run_session(
            url,
            pid,
            outgoing_consumer,
            incoming,
            disable_tls_verify,
        ));

        Self {
            publish: outgoing,
            consume: incoming_consumer,
            abort: task.abort_handle(),
        }
    }

    pub(crate) fn close(&self) {
        self.abort.abort();
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

async fn run_session(
    url: Url,
    pid: LocalPid,
    outgoing_consumer: Consumer,
    incoming: Producer,
    disable_tls_verify: bool,
) {
    let mut env = OwnedEnv::new();
    match connect(url, outgoing_consumer, incoming, disable_tls_verify).await {
        Ok(session) => {
            messages::send_connected(&mut env, pid);

            let reason = session.closed().await;
            messages::send_disconnected(&mut env, pid, reason.to_string());
        }
        Err(e) => messages::send_setup_failed(&mut env, pid, e.to_string()),
    }
}

async fn connect(
    url: Url,
    publish: Consumer,
    subscribe: Producer,
    disable_tls_verify: bool,
) -> Result<moq_native::moq_net::Session, moq_native::Error> {
    let mut config = ClientConfig::default();
    config.tls.disable_verify = Some(disable_tls_verify);

    config
        .init()?
        .with_publisher(publish)
        .with_subscriber(subscribe)
        .connect(url)
        .await
}
