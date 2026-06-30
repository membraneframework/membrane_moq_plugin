use hang::moq_net;

use moq_native::ClientConfig;
use moq_net::OriginConsumer;
use rustler::{Atom, Encoder, LocalPid, NifResult, OwnedEnv, ResourceArc};
use tokio::sync::mpsc;
use url::Url;

use crate::{atoms, runtime};

pub(crate) struct SessionResource {
    pub(crate) origin: moq_net::OriginProducer,
    shutdown: mpsc::UnboundedSender<()>,
}

impl rustler::Resource for SessionResource {}

#[rustler::nif]
pub(crate) fn setup_session(
    url: String,
    pid: LocalPid,
    disable_tls_verify: bool,
) -> NifResult<(Atom, ResourceArc<SessionResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;

    let origin = moq_net::Origin::random().produce();
    let consume = origin.consume();

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let config = client_config(disable_tls_verify);

        let session = create_session(url, consume, config).await;
        match session {
            Ok(session) => {
                OwnedEnv::new()
                    .send_and_clear(&pid, |env| atoms::moq_connected().to_term(env))
                    .expect("sending message to parent should succeed");

                tokio::select! {
                    _ = shutdown_rx.recv() => {} // session closed gracefully by parent
                    result = session.closed() => handle_session_closed(result, pid)
                }
            }
            Err(e) => OwnedEnv::new()
                .send_and_clear(&pid, |env| {
                    (atoms::moq_setup_failed(), e.to_string()).encode(env)
                })
                .expect("sending message to parent should succeed"),
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SessionResource {
            origin,
            shutdown: shutdown_tx,
        }),
    ))
}

#[rustler::nif]
pub(crate) fn close_session(session: ResourceArc<SessionResource>) -> Atom {
    let _ = session.shutdown.send(());
    atoms::ok()
}

pub(crate) fn client_config(disable_tls_verify: bool) -> ClientConfig {
    let mut config = ClientConfig::default();
    config.tls.disable_verify = Some(disable_tls_verify);
    config
}

async fn create_session(
    url: Url,
    consumer: OriginConsumer,
    config: ClientConfig,
) -> Result<moq_native::moq_net::Session, moq_native::Error> {
    let client = config.init()?;
    client.with_publish(consumer).connect(url).await
}

fn handle_session_closed(result: Result<(), moq_net::Error>, pid: LocalPid) {
    let message = result.map_or_else(
        |e| (atoms::moq_disconnected(), e.to_string()),
        |()| {
            (
                atoms::moq_disconnected(),
                "MoQ session closed gracefully".to_string(),
            )
        },
    );
    OwnedEnv::new()
        .send_and_clear(&pid, |env| message.encode(env))
        .expect("sending message to parent should succeed");
}
