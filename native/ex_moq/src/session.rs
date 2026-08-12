use hang::moq_net::{self, origin};

use moq_native::ClientConfig;
use rustler::{LocalPid, OwnedEnv};
use tokio::task::AbortHandle;
use url::Url;

use crate::{messages, runtime};

pub(crate) struct Handle {
    pub(crate) publish: origin::Producer,
    pub(crate) subscribe: origin::Consumer,
    abort: AbortHandle,
}

impl Handle {
    pub(crate) fn close(&self) {
        self.abort.abort();
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub(crate) fn create(url: Url, pid: LocalPid, disable_tls_verify: bool) -> Handle {
    let publish = moq_net::Origin::random().produce();
    let subscribe = moq_net::Origin::random().produce();

    let publish_consumer = publish.consume();
    let subscribe_consumer = subscribe.consume();

    let task = runtime().spawn(run_session(
        url,
        pid,
        publish_consumer,
        subscribe,
        disable_tls_verify,
    ));

    Handle {
        publish,
        subscribe: subscribe_consumer,
        abort: task.abort_handle(),
    }
}

async fn run_session(
    url: Url,
    pid: LocalPid,
    publish: origin::Consumer,
    subscribe: origin::Producer,
    disable_tls_verify: bool,
) -> Result<(), messages::PidDead> {
    let mut env = OwnedEnv::new();
    match connect(url, publish, subscribe, disable_tls_verify).await {
        Ok(session) => {
            messages::send_connected(&mut env, pid)?;

            let reason = session.closed().await;
            messages::send_disconnected(&mut env, pid, reason.to_string())
        }
        Err(e) => messages::send_setup_failed(&mut env, pid, e.to_string()),
    }
}

async fn connect(
    url: Url,
    publish: origin::Consumer,
    subscribe: origin::Producer,
    disable_tls_verify: bool,
) -> Result<moq_net::Session, moq_native::Error> {
    let mut config = ClientConfig::default();
    config.tls.disable_verify = Some(disable_tls_verify);

    config
        .init()?
        .with_publisher(publish)
        .with_subscriber(subscribe)
        .connect(url)
        .await
}
