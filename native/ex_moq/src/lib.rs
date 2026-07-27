use std::sync::OnceLock;

mod broadcast_consumer;
mod broadcast_producer;
mod messages;
mod session;
mod track;
mod track_format;

use broadcast_consumer::BroadcastConsumerResource;
use broadcast_producer::BroadcastProducerResource;
use session::SessionResource;
use track::TrackResource;

macro_rules! nif_error {
    ($($arg:tt)*) => {
        rustler::Error::Term(Box::new(format!($($arg)*)))
    };
}
pub(crate) use nif_error;

pub(crate) mod atoms {
    rustler::atoms! {
        ok,
        error,
        moq_missing_keyframe,
        moq_connected,
        moq_setup_failed,
        moq_disconnected,
        moq_frame,
        moq_catalog,
        moq_track_ended,
        moq_track_error,
        moq_broadcast_ready,
        moq_broadcast_closed,
    }
}

// `.lock().unwrap()` on a poisoned mutex panics, poisoning other mutexes locked during the call.
// We use this locking utility instead to isolate e.g. a poison from a track resource's mutex
// not to cause panic in other NIF calls.
pub(crate) fn lock_ignoring_poison<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        env.register::<TrackResource>(),
        env.register::<BroadcastConsumerResource>(),
    ]
    .iter()
    .all(std::result::Result::is_ok)
}

rustler::init!("Elixir.ExMoQ.Native", load = load);
