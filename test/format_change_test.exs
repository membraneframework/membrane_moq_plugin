defmodule Membrane.MoQ.FormatChangeTest do
  @moduledoc """
  Mid-stream stream-format changes on a single Sink pad,
  observed through MoQ.Source. Covers `ExMoQ.Native.update_track/2`.
  """

  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad

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
  # Each format change replaces the catalog rendition in place under the same track name,
  # so the Source must report `:track_removed` + `:new_track`
  # for that name and the receiver re-links a fresh pad per rendition.
  for container <- [:legacy, :loc] do
    test "three formats through one Sink pad arrive as three consecutive renditions (#{container})",
         %{
           relay: relay,
           broadcast: broadcast
         } do
      receiver = start_receiver!(relay, broadcast)
      publisher = start_publisher!(relay, broadcast, unquote(container), format_change_inputs())

      # 1st rendition
      assert_pipeline_notified(
        receiver,
        :source,
        {:new_track, {@track, %Membrane.H264{}}},
        15_000
      )

      link_track!(receiver, @track, 1)

      assert_sink_stream_format(receiver, {:sink, 1}, %Membrane.H264{
        width: 1280,
        height: 720
      })

      assert_sink_buffer(receiver, {:sink, 1}, %Membrane.Buffer{}, 10_000)

      assert_pipeline_notified(receiver, :source, {:track_removed, @track}, 15_000)
      # 1st rendition end

      # 2nd rendition
      assert_pipeline_notified(
        receiver,
        :source,
        {:new_track, {@track, %Membrane.H264{width: 640}}},
        15_000
      )

      assert_end_of_stream(receiver, {:sink, 1}, :input, 10_000)

      link_track!(receiver, @track, 2)

      assert_sink_stream_format(receiver, {:sink, 2}, %Membrane.H264{width: 640, height: 360})

      assert_sink_buffer(receiver, {:sink, 2}, %Membrane.Buffer{}, 10_000)

      assert_pipeline_notified(receiver, :source, {:track_removed, @track}, 15_000)
      # 2nd rendition end

      # 3rd rendition
      assert_pipeline_notified(
        receiver,
        :source,
        {:new_track, {@track, %Membrane.H265{}}},
        15_000
      )

      assert_end_of_stream(receiver, {:sink, 2}, :input, 10_000)

      link_track!(receiver, @track, 3)

      assert_sink_stream_format(receiver, {:sink, 3}, %Membrane.H265{
        width: 1280,
        height: 720
      })

      assert_sink_buffer(receiver, {:sink, 3}, %Membrane.Buffer{}, 10_000)

      assert_end_of_stream(publisher, :sink, Pad.ref(:input, :video), 20_000)

      assert_pipeline_notified(receiver, :source, {:track_removed, @track}, 15_000)
      # 3rd rendition end

      assert_end_of_stream(receiver, {:sink, 3}, :input, 10_000)

      :ok = Membrane.Pipeline.terminate(publisher)
      :ok = Membrane.Pipeline.terminate(receiver)
    end
  end

  test "an identical stream format re-sent mid-stream keeps the track", %{
    relay: relay,
    broadcast: broadcast
  } do
    inputs =
      for id <- [0, 1],
          do: {id, "#{@fixture_dir}/h264_1280x720_25.h264", h264_parser({25, 1})}

    receiver = start_receiver!(relay, broadcast)
    publisher = start_publisher!(relay, broadcast, :legacy, inputs)

    assert_pipeline_notified(
      receiver,
      :source,
      {:new_track, {@track, %Membrane.H264{}}},
      15_000
    )

    link_track!(receiver, @track, 1)

    assert_sink_stream_format(receiver, {:sink, 1}, %Membrane.H264{
      width: 1280,
      height: 720
    })

    assert_sink_buffer(receiver, {:sink, 1}, %Membrane.Buffer{}, 10_000)

    assert_end_of_stream(publisher, :sink, Pad.ref(:input, :video), 30_000)

    assert_pipeline_notified(receiver, :source, {:track_removed, @track}, 15_000)
    assert_end_of_stream(receiver, {:sink, 1}, :input, 10_000)

    refute_pipeline_notified(receiver, :source, {:new_track, _info}, 100)

    :ok = Membrane.Pipeline.terminate(publisher)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  defp h264_parser(framerate),
    do: %Membrane.H264.Parser{
      generate_best_effort_timestamps: %{framerate: framerate},
      output_stream_structure: :avc1
    }

  defp h265_parser(),
    do: %Membrane.H265.Parser{
      generate_best_effort_timestamps: %{framerate: {25, 1}},
      output_stream_structure: :hvc1
    }

  defp format_change_inputs(),
    do: [
      {0, "#{@fixture_dir}/h264_1280x720_25.h264", h264_parser({25, 1})},
      {1, "#{@fixture_dir}/h264_640x360_30.h264", h264_parser({30, 1})},
      {2, "#{@fixture_dir}/h265_1280x720_25.h265", h265_parser()}
    ]

  defp start_publisher!(relay, broadcast, container, inputs) do
    Testing.Pipeline.start_link_supervised!(
      spec: [
        child(:concat, Concatenator)
        |> child(:realtimer, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :video), options: [track: @track])
        |> child(:sink, %Membrane.MoQ.Sink{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?,
          container: container
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

  # The rendition name is stable across format changes, so each re-link needs
  # its own pad id and child name; the track is selected via the pad option.
  defp link_track!(receiver, track, pad_id) do
    Testing.Pipeline.execute_actions(receiver,
      spec:
        get_child(:source)
        |> via_out(Pad.ref(:output, pad_id), options: [track: track])
        |> child({:sink, pad_id}, Testing.Sink)
    )
  end
end
