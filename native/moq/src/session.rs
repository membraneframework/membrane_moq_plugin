use hang::moq_lite::OriginConsumer;
use moq_native::ClientConfig;
use rustler::{Atom, Encoder, LocalPid, NifResult, OwnedEnv, ResourceArc};
use tokio::sync::mpsc;
use url::Url;

use crate::{atoms, runtime};

pub(crate) struct SessionResource {
    pub(crate) origin: hang::moq_lite::OriginProducer,
    shutdown: mpsc::UnboundedSender<()>,
}

impl rustler::Resource for SessionResource {}

/// Connect to a MoQ relay server and prepare the session.
///
/// Builds the origin synchronously so subsequent NIFs can publish broadcasts
/// immediately. The QUIC handshake completes asynchronously
/// - `:moq_connected` is sent to `pid` once the session is up.
/// - `{:moq_setup_failed, reason :: String.t()}` is sent if establishing the connection fails.
/// - `{:moq_disconnected, reason :: String.t()}` is sent if the session closes.
#[rustler::nif]
pub(crate) fn setup_session(
    url: String,
    pid: LocalPid,
    disable_tls_verify: bool,
) -> NifResult<(Atom, ResourceArc<SessionResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;

    let origin = hang::moq_lite::Origin::random().produce();
    let consume = origin.consume();

    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let config = {
            let mut config = moq_native::ClientConfig::default();
            config.tls.disable_verify = Some(disable_tls_verify);
            config
        };

        let session = create_session(url, consume, config).await;
        match session {
            Ok(session) => {
                OwnedEnv::new()
                    .send_and_clear(&pid, |env| atoms::moq_connected().to_term(env))
                    .expect("sending message to parent should succeed");

                tokio::select! {
                    _ = shutdown_rx.recv() => {}
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

async fn create_session(
    url: Url,
    consumer: OriginConsumer,
    config: ClientConfig,
) -> anyhow::Result<moq_native::moq_lite::Session> {
    let client = config.init()?;
    client.with_publish(consumer).connect(url).await
}

fn handle_session_closed(result: Result<(), hang::moq_lite::Error>, pid: LocalPid) {
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
