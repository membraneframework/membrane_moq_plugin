defmodule Membrane.MoQ.SourceUnitTest do
  @moduledoc """
  Unit tests of the Source's catalog diffing and pad-parking logic, driving
  the element callbacks directly with synthetic contexts — no relay, no
  pipeline, no network.

  The consumer resource comes from a session that never completes its
  handshake: subscribe/unsubscribe NIFs are fire-and-forget into the
  consumer's command queue, so the full callback logic runs against a real
  resource hermetically.

  NOTE: This module was LLM-generated.
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

  test "a pad whose track is not yet advertised parks and resolves on a later snapshot", %{
    state: state
  } do
    ctx = playing_ctx(%{@video_pad => "video"})

    assert {[], state} = Source.handle_pad_added(@video_pad, ctx, state)

    # A snapshot without the track notifies the parent but leaves the pad parked.
    assert {actions, state} = Source.handle_info(catalog([{"audio", opus_format()}]), ctx, state)
    assert [notify_parent: {:new_track, {"audio", %Membrane.Opus{}}}] = actions
    assert state.tracks.active == MapSet.new()

    # The track appears: only then does the pad subscribe and get its format.
    assert {actions, state} =
             Source.handle_info(
               catalog([{"audio", opus_format()}, {"video", h264_format()}]),
               ctx,
               state
             )

    assert [
             notify_parent: {:new_track, {"video", %Membrane.H264{}}},
             stream_format: {@video_pad, %Membrane.H264{width: 1280}}
           ] = actions

    assert MapSet.member?(state.tracks.active, 0)
  end

  test "a snapshot diff notifies removed, changed and added renditions", %{state: state} do
    ctx = playing_ctx(%{})

    {_actions, state} =
      Source.handle_info(
        catalog([{"video", h264_format()}, {"audio", opus_format()}]),
        ctx,
        state
      )

    # "video" changes in place, "audio" disappears, "audio2" appears.
    {actions, _state} =
      Source.handle_info(
        catalog([{"video", h264_format(640)}, {"audio2", opus_format()}]),
        ctx,
        state
      )

    assert [
             notify_parent: {:track_removed, "audio"},
             notify_parent: {:track_removed, "video"},
             notify_parent: {:new_track, {"video", %Membrane.H264{width: 640}}},
             notify_parent: {:new_track, {"audio2", %Membrane.Opus{}}}
           ] = actions
  end

  test "an in-place rendition change ends the subscribed pad without resubscribing it", %{
    state: state
  } do
    ctx = playing_ctx(%{@video_pad => "video"})

    {_actions, state} = Source.handle_info(catalog([{"video", h264_format()}]), ctx, state)

    assert {[stream_format: {@video_pad, %Membrane.H264{}}], state} =
             Source.handle_pad_added(@video_pad, ctx, state)

    {actions, state} = Source.handle_info(catalog([{"video", h264_format(640)}]), ctx, state)

    # The old pad ends and the replacement rendition is advertised for a
    # fresh pad. In particular, no stream_format may follow the
    # end_of_stream: the ended pad must not be resubscribed even though the
    # (stale) ctx still shows it as open.
    assert [
             notify_parent: {:track_removed, "video"},
             notify_parent: {:new_track, {"video", %Membrane.H264{width: 640}}},
             end_of_stream: @video_pad
           ] = actions

    assert state.tracks.active == MapSet.new()
    assert BiMap.size(state.tracks.tokens) == 0
  end

  test ":moq_track_ended ends the pad and releases its subscription slot", %{state: state} do
    ctx = playing_ctx(%{@video_pad => "video"})

    {_actions, state} = Source.handle_info(catalog([{"video", h264_format()}]), ctx, state)
    {_actions, state} = Source.handle_pad_added(@video_pad, ctx, state)

    assert {[end_of_stream: @video_pad], state} =
             Source.handle_info({:moq_track_ended, 0}, ctx, state)

    assert state.tracks.active == MapSet.new()
  end

  test "nothing subscribes while stopped; handle_playing resolves waiting pads", %{
    state: state
  } do
    stopped = %{playing_ctx(%{@video_pad => "video"}) | playback: :stopped}

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

  defp playing_ctx(pads) do
    %{
      playback: :playing,
      pads:
        Map.new(pads, fn {pad, track} ->
          {pad, %{options: %{track: track, priority: 0}, end_of_stream?: false}}
        end)
    }
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

  defp opus_format do
    {:opus, %{params: %Native.AudioTrackParams{sample_rate: 48_000, channels: 2}}}
  end
end
