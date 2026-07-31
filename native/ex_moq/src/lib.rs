use std::sync::OnceLock;

mod broadcast_consumer;
mod broadcast_producer;
mod messages;
mod session;
mod track_format;

use broadcast_consumer::BroadcastConsumerResource;
use broadcast_producer::BroadcastProducerResource;
use session::SessionResource;

macro_rules! nif_error {
    ($fmt:literal $($arg:tt)*) => {
        rustler::Error::Term(Box::new(format!($fmt $($arg)*)))
    };
    ($term:expr) => {
        rustler::Error::Term(Box::new($term))
    };
}
pub(crate) use nif_error;

pub(crate) mod atoms {
    rustler::atoms! {
        ok,
        error,
        moq_missing_keyframe,
        moq_producer_poisoned,
        moq_track_already_exists,
        moq_unknown_track,
        moq_connected,
        moq_setup_failed,
        moq_disconnected,
        moq_frame,
        moq_catalog,
        moq_track_finished,
        moq_track_error,
        moq_broadcast_ready,
        moq_broadcast_closed,
    }
}

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime build should succeed")
    })
}

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    [
        env.register::<SessionResource>(),
        env.register::<BroadcastProducerResource>(),
        env.register::<BroadcastConsumerResource>(),
    ]
    .iter()
    .all(std::result::Result::is_ok)
}

rustler::init!("Elixir.ExMoQ.Native", load = load);
