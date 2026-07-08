use rustler::{Encoder, LocalPid, OwnedEnv};

use crate::atoms;
use crate::track_format::{encode_format, make_binary, TrackParams};

pub(crate) struct PidDead;

/// Opaque per-subscription tag echoed back in a track's messages
/// so the Elixir side can route them to the originating subscription.
pub(crate) type Token = i64;

pub(crate) fn send_connected(pid: LocalPid) {
    let _ = OwnedEnv::new().send_and_clear(&pid, |env| atoms::moq_connected().to_term(env));
}

pub(crate) fn send_setup_failed(pid: LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(&pid, |env| (atoms::moq_setup_failed(), reason).encode(env));
}

pub(crate) fn send_disconnected(pid: LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(&pid, |env| (atoms::moq_disconnected(), reason).encode(env));
}

pub(crate) fn send_broadcast_ready(pid: LocalPid, path: &str) {
    let _ = OwnedEnv::new()
        .send_and_clear(&pid, |env| (atoms::moq_broadcast_ready(), path).encode(env));
}

pub(crate) fn send_broadcast_closed(pid: LocalPid, path: &str, reason: String) {
    let _ = OwnedEnv::new().send_and_clear(&pid, |env| {
        (atoms::moq_broadcast_closed(), path, reason).encode(env)
    });
}

pub(crate) fn send_track_added(
    pid: LocalPid,
    path: &str,
    name: &str,
    params: &TrackParams,
) -> Result<(), PidDead> {
    OwnedEnv::new()
        .send_and_clear(&pid, |env| {
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

pub(crate) fn send_track_removed(pid: LocalPid, path: &str, name: &str) -> Result<(), PidDead> {
    OwnedEnv::new()
        .send_and_clear(&pid, |env| {
            (atoms::moq_track_removed(), path, name).encode(env)
        })
        .map_err(|_| PidDead)
}

pub(crate) fn send_track_format(pid: LocalPid, token: Token, params: &TrackParams) {
    let _ = OwnedEnv::new().send_and_clear(&pid, |env| {
        (atoms::moq_track_format(), token, encode_format(env, params)).encode(env)
    });
}

pub(crate) fn send_frame(
    pid: LocalPid,
    token: Token,
    payload: &[u8],
    timestamp_ns: u64,
    keyframe: bool,
) -> Result<(), PidDead> {
    OwnedEnv::new()
        .send_and_clear(&pid, |env| {
            (
                atoms::moq_frame(),
                token,
                make_binary(env, payload),
                timestamp_ns,
                keyframe,
            )
                .encode(env)
        })
        .map_err(|_| PidDead)
}

pub(crate) fn send_track_ended(pid: LocalPid, token: Token, reason: String) {
    let _ = OwnedEnv::new().send_and_clear(&pid, |env| {
        (atoms::moq_track_ended(), token, reason).encode(env)
    });
}
