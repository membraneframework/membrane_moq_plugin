defmodule ExMoQ.NativeTest do
  @moduledoc """
  Membrane-agnostic tests of the raw `ExMoQ.Native` API against a real relay.
  """

  use ExUnit.Case, async: false

  alias ExMoQ.Native
  alias Membrane.MoQ.Test.Relay

  @moduletag :integration

  @track "video"

  setup_all do
    [relay: Relay.ensure!()]
  end

  setup do
    [broadcast: "membrane/native-#{System.unique_integer([:positive])}"]
  end

  test "send_frame errors after the broadcast producer is closed", %{
    broadcast: broadcast,
    relay: relay
  } do
    {:ok, session} = Native.create_session(relay.url, self(), relay.disable_tls_verify?)
    assert_receive :moq_connected, 10_000

    {:ok, producer} = Native.create_broadcast_producer(session, broadcast)

    format =
      {:h264,
       %{
         params: %Native.VideoTrackParams{width: 1280, height: 720, framerate: 30.0},
         description: <<>>,
         codec: %Native.H264Codec{inline: true, profile: 66, constraints: 0, level: 30}
       }}

    {:ok, track} = Native.add_track(producer, @track, format, 60, :legacy, 0)
    assert :ok = Native.send_frame(track, 0, true, "frame")

    :ok = Native.close_broadcast_producer(producer)
    assert {:error, _reason} = Native.send_frame(track, 40_000_000, true, "frame")

    :ok = Native.close_session(session)
  end
end
