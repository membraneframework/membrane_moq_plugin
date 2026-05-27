defmodule Membrane.MoQ.Native do
  @moduledoc false
  use Rustler, otp_app: :membrane_moq_plugin, crate: "moq"

  # Publisher (Sink) NIFs

  @doc """
  Starts a MoQ publisher session in a background Tokio task.

  Connects to the relay at `url`, creates a broadcast named `broadcast`,
  then sends `:moq_connected` to `pid` once the handshake completes.
  """
  def start_publisher(_url, _broadcast, _pid),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Enqueues a binary frame to be sent on the publisher track.
  Each call creates a new MoQ group containing one frame.
  """
  def publish_frame(_resource, _data), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Signals the publisher task to shut down the session gracefully.
  """
  def stop_publisher(_resource), do: :erlang.nif_error(:nif_not_loaded)

  # Subscriber (Source) NIFs

  @doc """
  Starts a MoQ subscriber session in a background Tokio task.

  Connects to the relay at `url` and subscribes to `track` within `broadcast`.
  Incoming frames are sent to `pid` as `{:moq_frame, binary}` messages.
  On disconnect, sends `:moq_disconnected` to `pid`.
  """
  def start_subscriber(_url, _broadcast, _track, _pid),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Signals the subscriber task to stop receiving frames and close the session.
  """
  def stop_subscriber(_resource), do: :erlang.nif_error(:nif_not_loaded)


  @doc false
  def add_track(resource, track_id, header), do: :erlang.nif_error(:nif_not_loaded)

  @doc false
  def send_segment(resource, track_id, segment), do: :erlang.nif_error(:nif_not_loaded)
end
