use rustler::{Binary, Encoder, LocalPid, NewBinary, OwnedEnv, Term};

use crate::atoms;
use crate::track_format::{Container, TrackFormat};

pub(crate) struct PidDead;

/// Caller-chosen per-subscription tag, echoed back in the track's messages
/// so the Elixir side can route them to the originating subscription.
pub(crate) type Token = i64;

pub(crate) fn send_connected(env: &mut OwnedEnv, pid: LocalPid) {
    let _ = env.send_and_clear(&pid, |env| atoms::moq_connected().to_term(env));
}

pub(crate) fn send_setup_failed(env: &mut OwnedEnv, pid: LocalPid, reason: String) {
    let _ = env.send_and_clear(&pid, |env| (atoms::moq_setup_failed(), reason).encode(env));
}

pub(crate) fn send_disconnected(env: &mut OwnedEnv, pid: LocalPid, reason: String) {
    let _ = env.send_and_clear(&pid, |env| (atoms::moq_disconnected(), reason).encode(env));
}

pub(crate) fn send_broadcast_ready(env: &mut OwnedEnv, pid: LocalPid, path: &str) {
    let _ = env.send_and_clear(&pid, |env| (atoms::moq_broadcast_ready(), path).encode(env));
}

pub(crate) fn send_broadcast_closed(env: &mut OwnedEnv, pid: LocalPid, path: &str, reason: String) {
    let _ = env.send_and_clear(&pid, |env| {
        (atoms::moq_broadcast_closed(), path, reason).encode(env)
    });
}

/// Sends the full catalog snapshot as a list of
/// `{name, {format, container}}` pairs, one per advertised rendition.
pub(crate) fn send_catalog(
    env: &mut OwnedEnv,
    pid: LocalPid,
    path: &str,
    catalog: &moq_mux::catalog::hang::Catalog,
) -> Result<(), PidDead> {
    env.send_and_clear(&pid, |env| {
        let videos = catalog.video.renditions.iter().map(|(name, config)| {
            (
                name,
                (
                    TrackFormat::from_video(env, config),
                    Container::try_from(&config.container).ok(),
                ),
            )
                .encode(env)
        });
        let audios = catalog.audio.renditions.iter().map(|(name, config)| {
            (
                name,
                (
                    TrackFormat::from_audio(config),
                    Container::try_from(&config.container).ok(),
                ),
            )
                .encode(env)
        });
        let renditions: Vec<Term> = videos.chain(audios).collect();

        (atoms::moq_catalog(), path, renditions).encode(env)
    })
    .map_err(|_| PidDead)
}

pub(crate) fn send_frame(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
    frame: moq_mux::container::Frame,
) -> Result<(), PidDead> {
    let moq_mux::container::Frame {
        payload,
        timestamp,
        keyframe,
        duration: _,
    } = frame;
    env.send_and_clear(&pid, |env| {
        let mut payload_binary = NewBinary::new(env, payload.len());
        payload_binary.as_mut_slice().copy_from_slice(&payload);

        (
            atoms::moq_frame(),
            token,
            Into::<Binary>::into(payload_binary),
            timestamp.as_nanos() as u64,
            keyframe,
        )
            .encode(env)
    })
    .map_err(|_| PidDead)
}

pub(crate) fn send_track_finished(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
) -> Result<(), PidDead> {
    env.send_and_clear(&pid, |env| (atoms::moq_track_finished(), token).encode(env))
        .map_err(|_| PidDead)
}

pub(crate) fn send_track_error(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
    reason: String,
) -> Result<(), PidDead> {
    env.send_and_clear(&pid, |env| {
        (atoms::moq_track_error(), token, reason).encode(env)
    })
    .map_err(|_| PidDead)
}
