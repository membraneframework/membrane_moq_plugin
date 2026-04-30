defmodule Membrane.MoQ.Native do
  @moduledoc false
  use Rustler, otp_app: :membrane_moq_plugin, crate: "moq"

  # Publisher (Sink) NIFs

  @doc """
  Connects to a MoQ relay and prepares a broadcast.

  Sends `:moq_connected` to `pid` once the QUIC handshake completes.
  Call `configure_publisher/5` afterwards (once codec parameters are known).
  """
  def setup_publisher(_url, _broadcast, _pid),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Publishes the hang catalog and opens the video track.

  Must be called after `:moq_connected` has been received.
  `codec` is a WebCodecs codec string, e.g. `"avc1.64001f"`.
  """
  def configure_publisher(_resource, _codec, _width, _height, _framerate),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Sends an H.264 frame to the relay.

  `timestamp_us` is the presentation timestamp in microseconds.
  `keyframe` must be `true` for IDR frames to trigger a new MoQ group.
  """
  def send_segment(_resource, _timestamp_us, _keyframe, _data),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Signals the publisher task to stop and close the relay session.
  """
  def stop_publisher(_resource), do: :erlang.nif_error(:nif_not_loaded)

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
