defmodule Membrane.MoQ.FormatChangeTest do
  @moduledoc """
  Mid-stream stream-format changes on a single Sink pad,
  observed through MoQ.Source. Covers `ExMoQ.Native.replace_track/2`.
  """

  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad

  alias Membrane.MoQ.Source.TrackInfo
  alias Membrane.MoQ.Test.{Concatenator, Relay}
  alias Membrane.Pad
  alias Membrane.Testing

  @moduletag :integration

  @track "video"
  @fixture_dir "test/fixtures/format_change"

  setup_all do
    [relay: Relay.ensure!()]
  end

  setup do
    [broadcast: "membrane/format-change-#{System.unique_integer([:positive])}"]
  end

  # One Sink pad is fed three fixtures back to back through the Concatenator:
  #
  #   1. H264 1280x720 @ 25fps
  #   2. H264  640x360 @ 30fps   <- soft switch: same codec, different params
  #   3. H265 1280x720 @ 25fps   <- switch from H264 to H265
  #
  # Each format change replaces the published track under a fresh rendition
  # name, so the Source must report `:track_removed` + `:new_track` and the
  # receiver re-links a pad per rendition.
  test "three formats through one Sink pad arrive as three consecutive renditions", %{
    relay: relay,
    broadcast: broadcast
  } do
    receiver = start_receiver!(relay, broadcast)
    publisher = start_publisher!(relay, broadcast)

    # 1st track
    assert_pipeline_notified(
      receiver,
      :source,
      {:new_track, %TrackInfo{track: @track, stream_format: %Membrane.H264{}}},
      15_000
    )

    link_track!(receiver, @track)
    assert_sink_stream_format(receiver, {:sink, @track}, %Membrane.H264{width: 1280, height: 720})
    assert_sink_buffer(receiver, {:sink, @track}, %Membrane.Buffer{}, 10_000)

    assert_pipeline_notified(receiver, :source, {:track_removed, @track}, 15_000)
    # 1st track end

    # 2nd track
    assert_pipeline_notified(
      receiver,
      :source,
      {:new_track, %TrackInfo{track: track2, stream_format: %Membrane.H264{}}},
      15_000
    )

    assert track2 != @track
    assert_end_of_stream(receiver, {:sink, @track}, :input, 10_000)

    link_track!(receiver, track2)
    assert_sink_stream_format(receiver, {:sink, track2}, %Membrane.H264{width: 640, height: 360})
    assert_sink_buffer(receiver, {:sink, track2}, %Membrane.Buffer{}, 10_000)

    assert_pipeline_notified(receiver, :source, {:track_removed, ^track2}, 15_000)
    # 2nd track end

    # 3rd track
    assert_pipeline_notified(
      receiver,
      :source,
      {:new_track, %TrackInfo{track: track3, stream_format: %Membrane.H265{}}},
      15_000
    )

    assert track3 != track2
    assert_end_of_stream(receiver, {:sink, ^track2}, :input, 10_000)

    link_track!(receiver, track3)
    assert_sink_stream_format(receiver, {:sink, track3}, %Membrane.H265{width: 1280, height: 720})
    assert_sink_buffer(receiver, {:sink, track3}, %Membrane.Buffer{}, 10_000)

    assert_end_of_stream(publisher, :sink, Pad.ref(:input, :video), 20_000)

    assert_pipeline_notified(receiver, :source, {:track_removed, ^track3}, 15_000)
    # 3rd track end

    assert_end_of_stream(receiver, {:sink, ^track3}, :input, 10_000)
  end

  defp start_publisher!(relay, broadcast) do
    h264_parser = fn framerate ->
      %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: framerate},
        output_stream_structure: :avc1
      }
    end

    h265_parser = %Membrane.H265.Parser{
      generate_best_effort_timestamps: %{framerate: {25, 1}},
      output_stream_structure: :hvc1
    }

    inputs = [
      {0, "#{@fixture_dir}/h264_1280x720_25.h264", h264_parser.({25, 1})},
      {1, "#{@fixture_dir}/h264_640x360_30.h264", h264_parser.({30, 1})},
      {2, "#{@fixture_dir}/h265_1280x720_25.h265", h265_parser}
    ]

    Testing.Pipeline.start_link_supervised!(
      spec: [
        child(:concat, Concatenator)
        |> child(:realtimer, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :video), options: [track: @track])
        |> child(:sink, %Membrane.MoQ.Sink{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?
        })
        | Enum.map(inputs, fn {id, path, parser} ->
            child({:file, id}, %Membrane.File.Source{location: path})
            |> child({:parser, id}, parser)
            |> via_in(Pad.ref(:input, id))
            |> get_child(:concat)
          end)
      ]
    )
  end

  defp start_receiver!(relay, broadcast) do
    Testing.Pipeline.start_link_supervised!(
      spec:
        child(:source, %Membrane.MoQ.Source{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?,
          latency: Membrane.Time.milliseconds(200)
        })
    )
  end

  defp link_track!(receiver, track) do
    Testing.Pipeline.execute_actions(receiver,
      spec:
        get_child(:source)
        |> via_out(Pad.ref(:output, track), options: [track: track])
        |> child({:sink, track}, Testing.Sink)
    )
  end
end
