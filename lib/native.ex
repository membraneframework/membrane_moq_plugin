defmodule Membrane.MoQ.Native do
  @moduledoc false
  use Rustler, otp_app: :membrane_moq_plugin, crate: "moq"

  # Session NIFs

  @doc """
  Connects to a MoQ relay and prepares a session.

  Sends `:moq_connected` to `pid` once the QUIC handshake completes, or
  `:moq_disconnected` if the session fails / is closed. Tracks opened on this
  session use the hang Legacy container.
  """
  def setup_session(_url, _pid),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down the session. Idempotent.
  """
  def close_session(_session),
    do: :erlang.nif_error(:nif_not_loaded)

  # Broadcast NIFs

  @doc """
  Opens a broadcast on the session and creates its hang + MSF catalog tracks.

  Returns `{:ok, broadcast_resource}` or `{:error, reason}` (e.g. duplicate path).
  """
  def open_broadcast(_session, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the broadcast, finishing the catalog and aborting in-flight tracks.
  """
  def close_broadcast(_broadcast),
    do: :erlang.nif_error(:nif_not_loaded)

  # Track NIFs

  @doc """
  Adds an H.264 video track to the broadcast.

  `codec_str` is a WebCodecs codec string (e.g. `"avc1.64001f"` or
  `"avc3.64001f"`). Returns `{:ok, track_resource}` or `{:error, reason}`.
  """
  def add_h264_track(_broadcast, _track_name, _codec_str, _width, _height, _framerate),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Adds an H.265 video track to the broadcast.

  `codec_str` is a WebCodecs codec string (e.g. `"hev1.1.6.L93.B0"`).
  """
  def add_h265_track(_broadcast, _track_name, _codec_str, _width, _height, _framerate),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Adds an AAC audio track to the broadcast.

  `profile` is the AAC profile byte (e.g. 2 for AAC-LC, 5 for HE-AAC).
  """
  def add_aac_track(_broadcast, _track_name, _profile, _sample_rate, _channels),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Adds an Opus audio track to the broadcast.
  """
  def add_opus_track(_broadcast, _track_name, _sample_rate, _channels),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Sends a frame to a track.

  `timestamp_us` is the presentation timestamp in microseconds. `keyframe?`
  must be `true` for IDR frames on video tracks (triggers a new MoQ group);
  for audio tracks pass `true` for every frame.
  """
  def send_frame(_track, _timestamp_us, _keyframe?, _data),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the track, removing its rendition from the catalog and finishing the
  underlying moq-lite track. Idempotent.
  """
  def remove_track(_track),
    do: :erlang.nif_error(:nif_not_loaded)

  # Subscriber (Source) NIFs

  @doc """
  Starts a MoQ subscriber session. TODO: not yet implemented.
  """
  def start_subscriber(_url, _broadcast, _track, _pid),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Signals the subscriber task to stop.
  """
  def stop_subscriber(_resource), do: :erlang.nif_error(:nif_not_loaded)
end
