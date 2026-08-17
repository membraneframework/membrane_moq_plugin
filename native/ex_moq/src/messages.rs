use rustler::{Binary, Encoder, LocalPid, NewBinary, OwnedEnv, Term};

use crate::atoms;
use crate::broadcast_consumer::CloseReason;
use crate::track_format::{AudioTrackFormat, VideoTrackFormat};

pub(crate) struct PidDead;

/// Caller-chosen per-subscription tag, echoed back in the track's messages
/// so the Elixir side can route them to the originating subscription.
pub(crate) type Token = i64;

pub(crate) fn send_connected(env: &mut OwnedEnv, pid: LocalPid) -> Result<(), PidDead> {
    send(env, pid, atoms::moq_connected())
}

pub(crate) fn send_setup_failed(
    env: &mut OwnedEnv,
    pid: LocalPid,
    reason: String,
) -> Result<(), PidDead> {
    send(env, pid, (atoms::moq_setup_failed(), reason))
}

pub(crate) fn send_disconnected(
    env: &mut OwnedEnv,
    pid: LocalPid,
    reason: String,
) -> Result<(), PidDead> {
    send(env, pid, (atoms::moq_disconnected(), reason))
}

pub(crate) fn send_broadcast_ready(
    env: &mut OwnedEnv,
    pid: LocalPid,
    path: &str,
) -> Result<(), PidDead> {
    send(env, pid, (atoms::moq_broadcast_ready(), path))
}

pub(crate) fn send_broadcast_closed(
    env: &mut OwnedEnv,
    pid: LocalPid,
    path: &str,
    reason: CloseReason,
) -> Result<(), PidDead> {
    send(env, pid, (atoms::moq_broadcast_closed(), path, reason))
}

pub(crate) fn send_track_finished(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
) -> Result<(), PidDead> {
    send(env, pid, (atoms::moq_track_finished(), token))
}

pub(crate) fn send_track_error(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
    reason: String,
) -> Result<(), PidDead> {
    send(env, pid, (atoms::moq_track_error(), token, reason))
}

fn send(env: &mut OwnedEnv, pid: LocalPid, msg: impl Encoder) -> Result<(), PidDead> {
    env.send_and_clear(&pid, |env| msg.encode(env))
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
            Binary::from(payload_binary),
            timestamp.as_nanos() as u64,
            keyframe,
        )
            .encode(env)
    })
    .map_err(|_| PidDead)
}

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
                match VideoTrackFormat::try_from(config) {
                    Ok(format) => format.encode(env),
                    Err(unrecognized) => unrecognized.encode(env),
                },
            )
                .encode(env)
        });

        let audios = catalog.audio.renditions.iter().map(|(name, config)| {
            (
                name,
                match AudioTrackFormat::try_from(config) {
                    Ok(format) => format.encode(env),
                    Err(unrecognized) => unrecognized.encode(env),
                },
            )
                .encode(env)
        });

        let renditions: Vec<Term> = videos.chain(audios).collect();

        (atoms::moq_catalog(), path, renditions).encode(env)
    })
    .map_err(|_| PidDead)
}
