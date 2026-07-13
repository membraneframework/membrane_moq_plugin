use rustler::{Binary, Encoder, LocalPid, NewBinary, OwnedEnv};

use crate::atoms;
use crate::track_format::{encode_format, TrackParams};

pub(crate) struct PidDead;

/// Opaque per-subscription tag echoed back in a track's messages
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

pub(crate) fn send_track_added(
    env: &mut OwnedEnv,
    pid: LocalPid,
    path: &str,
    name: &str,
    params: &TrackParams,
) -> Result<(), PidDead> {
    env.send_and_clear(&pid, |env| {
        (
            atoms::moq_track_added(),
            path,
            name,
            encode_format(env, params),
        )
            .encode(env)
    })
    .map_err(|_| PidDead)
}

pub(crate) fn send_track_removed(
    env: &mut OwnedEnv,
    pid: LocalPid,
    path: &str,
    name: &str,
) -> Result<(), PidDead> {
    env.send_and_clear(&pid, |env| {
        (atoms::moq_track_removed(), path, name).encode(env)
    })
    .map_err(|_| PidDead)
}

pub(crate) fn send_track_format(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
    params: &TrackParams,
) {
    let _ = env.send_and_clear(&pid, |env| {
        (atoms::moq_track_format(), token, encode_format(env, params)).encode(env)
    });
}

pub(crate) fn send_frame(
    env: &mut OwnedEnv,
    pid: LocalPid,
    token: Token,
    payload: &[u8],
    timestamp_ns: u64,
    keyframe: bool,
) -> Result<(), PidDead> {
    env.send_and_clear(&pid, |env| {
        let mut payload_binary = NewBinary::new(env, payload.len());
        payload_binary.as_mut_slice().copy_from_slice(payload);

        (
            atoms::moq_frame(),
            token,
            Into::<Binary>::into(payload_binary),
            timestamp_ns,
            keyframe,
        )
            .encode(env)
    })
    .map_err(|_| PidDead)
}

pub(crate) fn send_track_ended(env: &mut OwnedEnv, pid: LocalPid, token: Token, reason: String) {
    let _ = env.send_and_clear(&pid, |env| {
        (atoms::moq_track_ended(), token, reason).encode(env)
    });
}

/// Unlike `:moq_track_ended`, this signals a subscription that died on our side
/// while its track may well still be advertised in the catalog.
pub(crate) fn send_track_error(env: &mut OwnedEnv, pid: LocalPid, token: Token, reason: String) {
    let _ = env.send_and_clear(&pid, |env| {
        (atoms::moq_track_error(), token, reason).encode(env)
    });
}
