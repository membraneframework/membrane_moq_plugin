use rustler::{Atom, LocalPid, OwnedEnv};
use std::sync::OnceLock;

mod broadcast;
mod nif_types;
mod session;
mod subscriber;
mod track;

use broadcast::BroadcastResource;
use session::SessionResource;
use subscriber::SubscriberResource;
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
        moq_disconnected,
        moq_frame,
    }
}

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    })
}

pub(crate) fn send_atom(pid: &LocalPid, atom: Atom) {
    let mut owned = OwnedEnv::new();
    owned
        .send_and_clear(pid, |env| atom.to_term(env))
        .expect("failed to send atom");
}

fn load(env: rustler::Env, _info: rustler::Term) -> bool {
    [
        env.register::<SessionResource>(),
        env.register::<BroadcastResource>(),
        env.register::<TrackResource>(),
        env.register::<SubscriberResource>(),
    ]
    .iter()
    .all(|r| r.is_ok())
}

rustler::init!("Elixir.Membrane.MoQ.Native", load = load);
