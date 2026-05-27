use std::sync::OnceLock;

mod broadcast;
mod nif_types;
mod session;
mod track;

use broadcast::BroadcastResource;
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
        moq_connected,
        moq_setup_failed,
        moq_disconnected,
        moq_write_failed,
        moq_frame,
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
        env.register::<BroadcastResource>(),
        env.register::<TrackResource>(),
    ]
    .iter()
    .all(std::result::Result::is_ok)
}

rustler::init!("Elixir.Membrane.MoQ.Native", load = load);
