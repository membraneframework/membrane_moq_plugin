//! The message contract: every term the native layer sends to its owning
//! Elixir process goes through here, so the wire protocol lives in one place.
//!
//! Lifecycle messages (`:moq_connected`, `:moq_setup_failed`,
//! `:moq_disconnected`) are shared by the publisher and subscriber; the
//! data messages (`:moq_tracks`, `:moq_track_format`, `:moq_frame`,
//! `:moq_track_ended`) are emitted by the subscriber. Sends are best effort: a
//! dead recipient is reported as [`PidDead`] for the data messages whose loops
//! must stop, and ignored for the fire-and-forget lifecycle ones.

use rustler::{Encoder, LocalPid, OwnedEnv};

use crate::atoms;
use crate::track_format::{encode_format, make_binary, TrackEntry, TrackParams};

/// The recipient process is gone, so no further message can be delivered.
pub(crate) struct PidDead;

/// Opaque per-subscription tag echoed back in a track's messages so the Elixir
/// side can route them to the originating pad.
pub(crate) type Token = i64;

pub(crate) fn send_connected(pid: &LocalPid) {
    let _ = OwnedEnv::new().send_and_clear(pid, |env| atoms::moq_connected().to_term(env));
}

pub(crate) fn send_setup_failed(pid: &LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(pid, |env| (atoms::moq_setup_failed(), reason).encode(env));
}

pub(crate) fn send_disconnected(pid: &LocalPid, reason: String) {
    let _ =
        OwnedEnv::new().send_and_clear(pid, |env| (atoms::moq_disconnected(), reason).encode(env));
}

/// `{:moq_tracks, [{name, format}]}` — the full advertised track set.
pub(crate) fn send_tracks(pid: &LocalPid, tracks: &[TrackEntry]) -> Result<(), PidDead> {
    OwnedEnv::new()
        .send_and_clear(pid, |env| {
            let list: Vec<_> = tracks
                .iter()
                .map(|t| (t.name.as_str(), encode_format(env, &t.params)).encode(env))
                .collect();
            (atoms::moq_tracks(), list).encode(env)
        })
        .map_err(|_| PidDead)
}

/// `{:moq_track_format, token, format}` — a subscribed track's codec
/// parameters, sent once before its first frame.
pub(crate) fn send_track_format(pid: &LocalPid, token: Token, params: &TrackParams) {
    let _ = OwnedEnv::new().send_and_clear(pid, |env| {
        (atoms::moq_track_format(), token, encode_format(env, params)).encode(env)
    });
}

/// `{:moq_frame, token, payload, timestamp_us, keyframe?}` — one received frame.
pub(crate) fn send_frame(
    pid: &LocalPid,
    token: Token,
    payload: &[u8],
    timestamp_us: i64,
    keyframe: bool,
) -> Result<(), PidDead> {
    OwnedEnv::new()
        .send_and_clear(pid, |env| {
            (
                atoms::moq_frame(),
                token,
                make_binary(env, payload),
                timestamp_us,
                keyframe,
            )
                .encode(env)
        })
        .map_err(|_| PidDead)
}

/// `{:moq_track_ended, token, reason}` — a subscribed track ended or errored.
pub(crate) fn send_track_ended(pid: &LocalPid, token: Token, reason: String) {
    let _ = OwnedEnv::new().send_and_clear(pid, |env| {
        (atoms::moq_track_ended(), token, reason).encode(env)
    });
}
