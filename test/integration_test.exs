defmodule Membrane.MoQ.IntegrationTest do
  @moduledoc """
  End-to-end tests that exercise `Membrane.MoQ.Sink` and `Membrane.MoQ.Source` against a real MoQ relay.
  """

  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad

  alias Membrane.MoQ.Test.{Relay, Take}
  alias Membrane.Pad
  alias Membrane.Testing

  @moduletag :integration

  @track "video"

  setup_all do
    [relay: Relay.ensure!()]
  end

  setup do
    broadcast = "membrane/test-#{System.unique_integer([:positive])}"
    [broadcast: broadcast]
  end

  test "frames sent through the Sink are received unchanged by the Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver = start_receiver!(relay, broadcast)
    # Without `await_source_connected!`, the sender races the receiver: the
    # source can finish setup AFTER the publisher has already finished, in
    # which case it sees no frames at all. The Sink announces in
    # `handle_pad_added`, so by the time we get past `start_sender!` the
    # broadcast is in the relay's announcement list.
    sender = start_sender!(relay, broadcast)
    await_source_connected!(receiver)

    assert_end_of_stream(sender, :expected_sink, :input, 30_000)
    expected_payloads = drain_payloads(sender, :expected_sink)
    assert expected_payloads != []

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    received_payloads = drain_payloads(receiver, :sink)

    assert received_payloads == expected_payloads

    :ok = Membrane.Pipeline.terminate(sender)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  test "avc3 frames (in-band parameter sets) round-trip unchanged through the Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver = start_receiver!(relay, broadcast)
    sender = start_sender!(relay, broadcast, stream_structure: :avc3)
    await_source_connected!(receiver)

    assert_end_of_stream(sender, :expected_sink, :input, 30_000)
    expected_payloads = drain_payloads(sender, :expected_sink)
    assert expected_payloads != []

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    received_payloads = drain_payloads(receiver, :sink)

    assert received_payloads == expected_payloads
  end

  test "LOC frames round-trip unchanged with keyframe flags intact", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver = start_receiver!(relay, broadcast)
    sender = start_sender!(relay, broadcast, sink_opts: [container: :loc])
    await_source_connected!(receiver)

    assert_end_of_stream(sender, :expected_sink, :input, 30_000)
    expected_buffers = drain_buffers(sender, :expected_sink)
    assert expected_buffers != []

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    received_buffers = drain_buffers(receiver, :sink)

    assert Enum.map(received_buffers, & &1.payload) ==
             Enum.map(expected_buffers, & &1.payload)

    # LOC doesn't carry the keyframe bit on the wire:
    # the producer starts a group per keyframe
    # and the consumer flags each group's first frame,
    # so the published flags must survive the round-trip 1:1.
    assert Enum.map(received_buffers, & &1.metadata.h264.key_frame?) ==
             Enum.map(expected_buffers, & &1.metadata.h264.key_frame?)
  end

  test "frames buffered with the latency option all arrive, unchanged and in order", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver = start_receiver!(relay, broadcast)

    sender =
      start_sender!(relay, broadcast,
        sink_opts: [container: :loc, latency: Membrane.Time.milliseconds(500)]
      )

    await_source_connected!(receiver)

    assert_end_of_stream(sender, :expected_sink, :input, 30_000)
    expected_payloads = drain_payloads(sender, :expected_sink)
    assert expected_payloads != []

    # EOS closes the track, which flushes the tail of the latency buffer.
    assert_end_of_stream(receiver, :sink, :input, 30_000)
    received_payloads = drain_payloads(receiver, :sink)

    assert received_payloads == expected_payloads
  end

  test "Source emits end_of_stream when the publisher disconnects after publishing a frame", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver = start_receiver!(relay, broadcast)
    sender = start_sender!(relay, broadcast)
    await_source_connected!(receiver)

    assert_sink_stream_format(receiver, :sink, _stream_format, 10_000)
    :ok = Membrane.Pipeline.terminate(sender)

    assert_end_of_stream(receiver, :sink, :input, 10_000)
  end

  test "Sink closes a pad that ended before its stream format without crashing", %{
    broadcast: broadcast,
    relay: relay
  } do
    defmodule EndOfStreamSource do
      use Membrane.Source

      def_output_pad :output, accepted_format: _any, flow_control: :push

      @impl true
      def handle_init(_ctx, _opts), do: {[], %{}}

      @impl true
      def handle_playing(_ctx, state), do: {[end_of_stream: :output], state}
    end

    spec =
      child(:source, EndOfStreamSource)
      |> via_in(Pad.ref(:input, :video), options: [track: @track])
      |> child(:moq_sink, %Membrane.MoQ.Sink{
        url: relay.url,
        broadcast: broadcast,
        disable_tls_verify?: relay.disable_tls_verify?
      })

    pipeline = Testing.Pipeline.start_supervised!(spec: spec)
    ref = Process.monitor(pipeline)

    refute_receive {:DOWN, ^ref, :process, ^pipeline,
                    {:membrane_child_crash, :moq_sink, _reason}},
                   5_000
  end

  test "Removing a pad and relinking one with the same track name doesn't crash the sink", %{
    broadcast: broadcast,
    relay: relay
  } do
    sender = start_sender!(relay, broadcast)

    assert_end_of_stream(sender, :moq_sink, Pad.ref(:input, :video), 30_000)

    Testing.Pipeline.execute_actions(sender,
      remove_children: [:file_source, :parser, :take, :tee, :realtimer, :expected_sink],
      spec:
        child(:second_file_source, %Membrane.File.Source{
          location: "test/fixtures/bbb_with_aud.h264"
        })
        |> child(:second_parser, %Membrane.H264.Parser{
          output_stream_structure: :avc1,
          generate_best_effort_timestamps: %{framerate: {30, 1}}
        })
        |> child(:second_take, %Take{count: 5})
        |> via_in(Pad.ref(:input, :video2), options: [track: @track])
        |> get_child(:moq_sink)
    )

    assert_end_of_stream(sender, :moq_sink, Pad.ref(:input, :video2), 30_000)

    :ok = Membrane.Pipeline.terminate(sender)
  end

  test "Sink skips delta frames preceding the first keyframe instead of crashing", %{
    broadcast: broadcast,
    relay: relay
  } do
    # Simulates a track joining mid-stream:
    # drops frames up to and including the first keyframe,
    # so the sink's fresh track (no open MoQ group) receives a delta frame first.
    defmodule MidStreamJoin do
      use Membrane.Filter

      def_input_pad :input, accepted_format: _any, flow_control: :auto
      def_output_pad :output, accepted_format: _any, flow_control: :auto

      @impl true
      def handle_init(_ctx, _opts), do: {[], %{joined?: false}}

      @impl true
      def handle_buffer(:input, buffer, _ctx, %{joined?: true} = state),
        do: {[buffer: {:output, buffer}], state}

      def handle_buffer(:input, buffer, _ctx, state),
        do: {[], %{state | joined?: buffer.metadata.h264.key_frame?}}
    end

    spec =
      child(:file_source, %Membrane.File.Source{
        location: "test/fixtures/bbb_with_aud.h264"
      })
      |> child(:parser, %Membrane.H264.Parser{
        output_stream_structure: :avc1,
        generate_best_effort_timestamps: %{framerate: {30, 1}}
      })
      |> child(:take, %Take{count: 30})
      |> child(:mid_stream_join, MidStreamJoin)
      |> via_in(Pad.ref(:input, :video), options: [track: @track])
      |> child(:moq_sink, %Membrane.MoQ.Sink{
        url: relay.url,
        broadcast: broadcast,
        disable_tls_verify?: relay.disable_tls_verify?
      })

    sender = Testing.Pipeline.start_link_supervised!(spec: spec)

    assert_end_of_stream(sender, :moq_sink, Pad.ref(:input, :video), 30_000)

    :ok = Membrane.Pipeline.terminate(sender)
  end

  test "Source emits parser-convention keyframe metadata, preserving grouping across a Source-to-Sink relay",
       %{broadcast: broadcast, relay: relay} do
    relay_broadcast = broadcast <> "-relay"

    receiver = start_receiver!(relay, relay_broadcast)

    relay_pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec: [
          child(:source, %Membrane.MoQ.Source{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
          |> via_out(Pad.ref(:output, :video), options: [track: @track])
          |> child(:tee, Membrane.Tee)
          |> via_in(Pad.ref(:input, :video), options: [track: @track])
          |> child(:moq_sink, %Membrane.MoQ.Sink{
            url: relay.url,
            broadcast: relay_broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          }),
          get_child(:tee)
          |> child(:probe, Testing.Sink)
        ]
      )

    sender = start_sender!(relay, broadcast)

    # `sender` -> `relay` -> `relay_pipeline` -> `relay` -> `receiver`

    assert_sink_playing(relay_pipeline, :probe, 10_000)
    await_source_connected!(receiver)

    assert_end_of_stream(sender, :expected_sink, :input, 30_000)
    expected_buffers = drain_buffers(sender, :expected_sink)
    assert expected_buffers != []
    expected_key_frames = Enum.map(expected_buffers, & &1.metadata.h264.key_frame?)

    assert_end_of_stream(relay_pipeline, :probe, :input, 30_000)
    relayed_buffers = drain_buffers(relay_pipeline, :probe)

    assert Enum.map(relayed_buffers, & &1.metadata.h264.key_frame?) == expected_key_frames

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    received_buffers = drain_buffers(receiver, :sink)

    assert Enum.map(received_buffers, & &1.payload) ==
             Enum.map(expected_buffers, & &1.payload)

    assert Enum.map(received_buffers, & &1.metadata.h264.key_frame?) == expected_key_frames
  end

  defp start_receiver!(relay, broadcast) do
    Testing.Pipeline.start_link_supervised!(
      spec:
        child(:source, %Membrane.MoQ.Source{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?
        })
        |> via_out(Pad.ref(:output, :video), options: [track: @track])
        |> child(:sink, Testing.Sink)
    )
  end

  defp start_sender!(relay, broadcast, opts \\ []) do
    max_buffers = Keyword.get(opts, :max_buffers, 30)
    stream_structure = Keyword.get(opts, :stream_structure, :avc1)

    sink =
      struct!(
        %Membrane.MoQ.Sink{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?
        },
        Keyword.get(opts, :sink_opts, [])
      )

    Testing.Pipeline.start_link_supervised!(
      spec: [
        child(:file_source, %Membrane.File.Source{
          location: "test/fixtures/bbb_with_aud.h264"
        })
        |> child(:parser, %Membrane.H264.Parser{
          output_stream_structure: stream_structure,
          generate_best_effort_timestamps: %{framerate: {30, 1}}
        })
        |> child(:take, %Take{count: max_buffers})
        |> child(:tee, Membrane.Tee)
        # Realtimer paces the publish based on the pts assigned by the parser,
        # so the relay has time to forward frames to the receiver before the
        # broadcast closes.
        |> child(:realtimer, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :video),
          options: [track: @track]
        )
        |> child(:moq_sink, sink),
        get_child(:tee)
        |> child(:expected_sink, Testing.Sink)
      ]
    )
  end

  defp await_source_connected!(receiver),
    do: assert_sink_playing(receiver, :sink, 10_000)

  defp drain_payloads(pipeline, child) do
    pipeline |> drain_buffers(child) |> Enum.map(& &1.payload)
  end

  defp drain_buffers(pipeline, child, acc \\ []) do
    receive do
      {Testing.Pipeline, ^pipeline,
       {:handle_child_notification, {{:buffer, %Membrane.Buffer{} = buffer}, ^child}}} ->
        drain_buffers(pipeline, child, [buffer | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
