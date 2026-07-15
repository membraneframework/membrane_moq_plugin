defmodule Membrane.MoQ.SourceUnitTest do
  @moduledoc """
  Scenarios that would be hard to test deterministically with a live relay and pipeline
  """

  use ExUnit.Case, async: true

  require Membrane.Pad

  alias ExMoQ.Native
  alias Membrane.MoQ.Source
  alias Membrane.Pad

  @broadcast "unit/broadcast"
  @video_pad Pad.ref(:output, "video")

  setup do
    # 127.0.0.1:1 never answers, and the session's async :moq_setup_failed
    # goes to a throwaway pid, keeping the test mailbox clean.
    sink = spawn(fn -> Process.sleep(:infinity) end)
    {:ok, session} = Native.create_session("https://127.0.0.1:1", sink, true)
    {:ok, consumer} = Native.create_broadcast_consumer(session, @broadcast, sink, 0)

    on_exit(fn ->
      :ok = Native.close_broadcast_consumer(consumer)
      :ok = Native.close_session(session)
      Process.exit(sink, :kill)
    end)

    state = %Source.State{
      url: "https://127.0.0.1:1",
      broadcast: @broadcast,
      disable_tls_verify?: true,
      latency: 0,
      consumer: consumer,
      status: :ready
    }

    [state: state]
  end

  test "nothing subscribes while stopped; handle_playing resolves waiting pads", %{
    state: state
  } do
    stopped = %{
      playback: :stopped,
      pads: %{@video_pad => %{options: %{track: "video", priority: 0}, end_of_stream?: false}}
    }

    assert {[], state} = Source.handle_pad_added(@video_pad, stopped, state)

    assert {actions, state} =
             Source.handle_info(catalog([{"video", h264_format()}]), stopped, state)

    assert [notify_parent: {:new_track, _track}] = actions
    assert state.tracks.active == MapSet.new()

    playing = %{stopped | playback: :playing}

    assert {[stream_format: {@video_pad, %Membrane.H264{}}], state} =
             Source.handle_playing(playing, state)

    assert MapSet.member?(state.tracks.active, 0)
  end

  defp catalog(renditions) do
    {:moq_catalog, @broadcast, for({name, format} <- renditions, do: {name, {format, :legacy}})}
  end

  defp h264_format(width \\ 1280) do
    {:h264,
     %{
       params: %Native.VideoTrackParams{width: width, height: 720, framerate: 30.0},
       description: <<>>,
       codec: %Native.H264Codec{inline: true, profile: 66, constraints: 0, level: 30}
     }}
  end
end
