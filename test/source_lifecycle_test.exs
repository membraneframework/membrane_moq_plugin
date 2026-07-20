defmodule Membrane.MoQ.SourceLifecycleTest do
  @moduledoc "Source lifecycle scenarios against a real relay"

  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad

  alias Membrane.MoQ.Test.{Relay, RestartingSubscriber, Take}
  alias Membrane.Pad
  alias Membrane.Testing

  @moduletag :integration

  @track "video"

  setup_all do
    [relay: Relay.ensure!()]
  end

  setup do
    [broadcast: "membrane/lifecycle-#{System.unique_integer([:positive])}"]
  end

  test "Source joining an in-progress broadcast receives frames", %{
    relay: relay,
    broadcast: broadcast
  } do
    publisher = start_publisher!(relay, broadcast)

    assert_start_of_stream(publisher, :sink, Pad.ref(:input, :video), 10_000)

    receiver = start_receiver!(relay, broadcast)

    assert_sink_stream_format(receiver, :sink, %Membrane.H264{}, 15_000)
    assert_sink_buffer(receiver, :sink, %Membrane.Buffer{}, 10_000)

    :ok = Membrane.Pipeline.terminate(publisher)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  test "a parser wired directly onto the pad decodes from the pad's stream format alone", %{
    relay: relay,
    broadcast: broadcast
  } do
    subscriber = start_subscriber!(relay, broadcast)
    publisher = start_publisher!(relay, broadcast)

    assert_pipeline_notified(
      subscriber,
      {:source, 0},
      {:new_track, {@track, %Membrane.H264{}}},
      15_000
    )

    assert_sink_stream_format(subscriber, {:sink, 0}, %Membrane.H264{alignment: :au}, 10_000)

    assert_sink_buffer(
      subscriber,
      {:sink, 0},
      %Membrane.Buffer{metadata: %{h264: %{key_frame?: true}}},
      10_000
    )

    for _i <- 1..20 do
      assert_sink_buffer(subscriber, {:sink, 0}, %Membrane.Buffer{}, 10_000)
    end

    :ok = Membrane.Pipeline.terminate(publisher)
    :ok = Membrane.Pipeline.terminate(subscriber)
  end

  test "the parent resubscribes after a broadcast drop and receives the second publish", %{
    relay: relay,
    broadcast: broadcast
  } do
    subscriber = start_subscriber!(relay, broadcast)

    publisher = start_publisher!(relay, broadcast)
    assert_pipeline_notified(subscriber, {:source, 0}, {:new_track, {_track, _format}}, 15_000)
    assert_sink_buffer(subscriber, {:sink, 0}, %Membrane.Buffer{}, 10_000)

    assert_end_of_stream(publisher, :sink, Pad.ref(:input, :video), 15_000)
    :ok = Membrane.Pipeline.terminate(publisher)
    assert_pipeline_notified(subscriber, {:source, 0}, {:disconnected, _reason}, 10_000)

    second_publisher = start_publisher!(relay, broadcast)
    assert_pipeline_notified(subscriber, {:source, 1}, {:new_track, {_track, _format}}, 15_000)
    assert_sink_buffer(subscriber, {:sink, 1}, %Membrane.Buffer{}, 10_000)

    :ok = Membrane.Pipeline.terminate(second_publisher)
    :ok = Membrane.Pipeline.terminate(subscriber)
  end

  test "a pad added after the source disconnected is immediately end_of_streamed", %{
    relay: relay,
    broadcast: broadcast
  } do
    publisher = start_publisher!(relay, broadcast)
    assert_start_of_stream(publisher, :sink, Pad.ref(:input, :video), 10_000)

    # No pads yet, so the disconnect leaves the source alive with nothing to EOS.
    receiver =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Membrane.MoQ.Source{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
      )

    assert_pipeline_notified(receiver, :source, {:new_track, {@track, _format}}, 15_000)

    :ok = Membrane.Pipeline.terminate(publisher)
    assert_pipeline_notified(receiver, :source, {:disconnected, _reason}, 15_000)

    Testing.Pipeline.execute_actions(receiver,
      spec:
        get_child(:source)
        |> via_out(Pad.ref(:output, @track), options: [track: @track])
        |> child(:late_sink, Testing.Sink)
    )

    assert_end_of_stream(receiver, :late_sink, :input, 10_000)

    :ok = Membrane.Pipeline.terminate(receiver)
  end

  test "a pad linked before its track is advertised parks until a later catalog snapshot", %{
    relay: relay,
    broadcast: broadcast
  } do
    # Endless synthetic audio keeps the broadcast alive
    # while the video track is added and the audio track is later removed.
    endless_opus =
      {0,
       fn i, size ->
         buffers =
           for n <- i..(i + size - 1) do
             %Membrane.Buffer{payload: <<n::32>>, pts: Membrane.Time.milliseconds(20 * n)}
           end

         {[buffer: {:output, buffers}], i + size}
       end}

    publisher =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:audio_source, %Testing.Source{
            stream_format: %Membrane.Opus{channels: 2, self_delimiting?: false},
            output: endless_opus
          })
          |> child(:audio_realtimer, Membrane.Realtimer)
          |> via_in(Pad.ref(:input, :audio), options: [track: "audio"])
          |> child(:sink, %Membrane.MoQ.Sink{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
      )

    receiver =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Membrane.MoQ.Source{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
          |> via_out(Pad.ref(:output, :video), options: [track: @track])
          |> child(:video_sink, Testing.Sink)
      )

    assert_pipeline_notified(receiver, :source, {:new_track, {"audio", %Membrane.Opus{}}}, 15_000)
    refute_sink_stream_format(receiver, :video_sink, _format, 500)

    Testing.Pipeline.execute_actions(publisher,
      spec:
        child(:video_file, %Membrane.File.Source{location: "test/fixtures/bbb_with_aud.h264"})
        |> child(:video_parser, %Membrane.H264.Parser{
          output_stream_structure: :avc1,
          generate_best_effort_timestamps: %{framerate: {30, 1}}
        })
        |> child(:video_take, %Take{count: 40})
        |> child(:video_realtimer, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :video), options: [track: @track])
        |> get_child(:sink)
    )

    assert_pipeline_notified(receiver, :source, {:new_track, {@track, %Membrane.H264{}}}, 15_000)
    assert_sink_stream_format(receiver, :video_sink, %Membrane.H264{}, 10_000)
    assert_sink_buffer(receiver, :video_sink, %Membrane.Buffer{}, 10_000)

    Testing.Pipeline.execute_actions(publisher,
      remove_children: [:audio_source, :audio_realtimer]
    )

    assert_pipeline_notified(receiver, :source, {:track_removed, "audio"}, 10_000)
    assert_sink_buffer(receiver, :video_sink, %Membrane.Buffer{}, 10_000)

    :ok = Membrane.Pipeline.terminate(publisher)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  test ":moq_track_error for a live subscription sends EOS without killing the source", %{
    relay: relay,
    broadcast: broadcast
  } do
    publisher = start_publisher!(relay, broadcast)

    receiver = start_receiver!(relay, broadcast)

    assert_sink_buffer(receiver, :sink, %Membrane.Buffer{}, 15_000)

    source_pid = Testing.Pipeline.get_child_pid!(receiver, :source)
    send(source_pid, {:moq_track_error, 0, "injected pump panic"})

    assert_end_of_stream(receiver, :sink, :input, 10_000)

    :ok = Membrane.Pipeline.terminate(publisher)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  test ":moq_track_error for an unknown token is dropped without killing the source", %{
    relay: relay,
    broadcast: broadcast
  } do
    publisher = start_publisher!(relay, broadcast)

    receiver = start_unlinked_receiver!(relay, broadcast)
    ref = Process.monitor(receiver)

    assert_sink_buffer(receiver, :sink, %Membrane.Buffer{}, 15_000)

    source_pid = Testing.Pipeline.get_child_pid!(receiver, :source)
    send(source_pid, {:moq_track_error, 999, "stale subscription"})

    refute_receive {:DOWN, ^ref, :process, ^receiver, _reason}, 500
    assert_sink_buffer(receiver, :sink, %Membrane.Buffer{}, 10_000)

    :ok = Membrane.Pipeline.terminate(publisher)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  defp start_publisher!(relay, broadcast, opts \\ []) do
    frames = Keyword.get(opts, :frames, 40)

    Testing.Pipeline.start_link_supervised!(
      spec:
        child(:file, %Membrane.File.Source{location: "test/fixtures/bbb_with_aud.h264"})
        |> child(:parser, %Membrane.H264.Parser{
          output_stream_structure: :avc1,
          generate_best_effort_timestamps: %{framerate: {30, 1}}
        })
        |> child(:take, %Take{count: frames})
        |> child(:realtimer, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :video), options: [track: @track])
        |> child(:sink, %Membrane.MoQ.Sink{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?
        })
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
        |> via_out(Pad.ref(:output, :video), options: [track: @track])
        |> child(:sink, Testing.Sink)
    )
  end

  # Like start_receiver!, but not linked to the test process, so an expected
  # element crash can be observed via a monitor instead of killing the test.
  defp start_unlinked_receiver!(relay, broadcast) do
    Testing.Pipeline.start_supervised!(
      spec:
        child(:source, %Membrane.MoQ.Source{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?,
          latency: Membrane.Time.milliseconds(200)
        })
        |> via_out(Pad.ref(:output, :video), options: [track: @track])
        |> child(:sink, Testing.Sink)
    )
  end

  defp start_subscriber!(relay, broadcast) do
    Testing.Pipeline.start_link_supervised!(
      module: RestartingSubscriber,
      custom_args: [
        url: relay.url,
        broadcast: broadcast,
        disable_tls_verify?: relay.disable_tls_verify?
      ]
    )
  end
end
