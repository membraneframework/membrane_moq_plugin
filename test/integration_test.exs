defmodule Membrane.MoQ.IntegrationTest do
  @moduledoc """
  End-to-end tests that exercise `Membrane.MoQ.Sink` and `Membrane.MoQ.Source` against a real MoQ relay.
  """

  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad

  alias Membrane.MoQ.Test.Take
  alias Membrane.Pad
  alias Membrane.Testing

  alias ExMoQ.Test.Relay

  @moduletag :integration

  @track "video"
  @audio_track "audio"

  defmodule EndOfStreamSource do
    use Membrane.Source

    def_output_pad :output, accepted_format: _any, flow_control: :push

    @impl true
    def handle_init(_ctx, _opts), do: {[], %{}}

    @impl true
    def handle_playing(_ctx, state), do: {[end_of_stream: :output], state}
  end

  # Simulates a track joining mid-stream:
  # drops frames up to and including the first keyframe,
  # so a fresh track receives a delta frame first.
  defmodule MidStreamJoin do
    use Membrane.Filter

    def_input_pad :input, accepted_format: _any
    def_output_pad :output, accepted_format: _any

    @impl true
    def handle_init(_ctx, _opts), do: {[], %{joined?: false}}

    @impl true
    def handle_buffer(:input, buffer, _ctx, %{joined?: true} = state),
      do: {[buffer: {:output, buffer}], state}

    @impl true
    def handle_buffer(:input, buffer, _ctx, state),
      do: {[], %{state | joined?: buffer.metadata.h264.key_frame?}}
  end

  setup_all do
    [relay: Relay.start_supervised!()]
  end

  setup do
    broadcast = "membrane/test-#{System.unique_integer([:positive])}"
    [broadcast: broadcast]
  end

  @tag :flaky
  test "frames sent through the Sink are received unchanged by the Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    {expected, received} = round_trip!(relay, broadcast)

    assert Enum.map(received, & &1.payload) == Enum.map(expected, & &1.payload)
  end

  @tag :flaky
  test "a .msf broadcast name selects the MSF catalog for the Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    {expected, received} = round_trip!(relay, broadcast <> ".msf")

    assert Enum.map(received, & &1.payload) == Enum.map(expected, & &1.payload)
  end

  @tag :flaky
  test "avc3 frames (in-band parameter sets) round-trip unchanged through the Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    {expected, received} = round_trip!(relay, broadcast, stream_structure: :avc3)

    assert Enum.map(received, & &1.payload) == Enum.map(expected, & &1.payload)
  end

  @tag :flaky
  test "LOC frames round-trip unchanged with keyframe flags intact", %{
    broadcast: broadcast,
    relay: relay
  } do
    {expected, received} = round_trip!(relay, broadcast, sink_opts: [container: :loc])

    assert Enum.map(received, & &1.payload) == Enum.map(expected, & &1.payload)

    # LOC doesn't carry the keyframe bit on the wire:
    # the producer starts a group per keyframe
    # and the consumer flags each group's first frame,
    # so the published flags must survive the round-trip 1:1.
    assert Enum.map(received, & &1.metadata.h264.key_frame?) ==
             Enum.map(expected, & &1.metadata.h264.key_frame?)
  end

  @tag :flaky
  test "frames buffered with the latency option all arrive, unchanged and in order", %{
    broadcast: broadcast,
    relay: relay
  } do
    # EOS closes the track, which flushes the tail of the latency buffer,
    # so the receiver still reaches end of stream with frames in flight.
    {expected, received} =
      round_trip!(relay, broadcast,
        sink_opts: [container: :loc, latency: Membrane.Time.milliseconds(500)]
      )

    assert Enum.map(received, & &1.payload) == Enum.map(expected, & &1.payload)
  end

  @tag :flaky
  test "AAC frames round-trip unchanged through the Sink and Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver = start_receiver!(relay, broadcast, @audio_track)
    sender = start_audio_sender!(relay, broadcast)
    assert_sink_playing(receiver, :sink, 10_000)

    assert_sink_stream_format(
      receiver,
      :sink,
      %Membrane.AAC{profile: :LC, sample_rate: 44_100, channels: 2},
      10_000
    )

    assert_end_of_stream(sender, :expected_sink, :input, 30_000)
    expected_payloads = drain_payloads(sender, :expected_sink)
    assert expected_payloads != []

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    assert drain_payloads(receiver, :sink) == expected_payloads
  end

  @tag :flaky
  test "Opus frames round-trip unchanged through the Sink and Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    # The plugin treats audio payloads as opaque, so synthetic 20 ms frames
    # exercise the catalog and transport without an Opus encoder.
    buffers =
      for i <- 0..49 do
        %Membrane.Buffer{payload: <<i, 255 - i>>, pts: Membrane.Time.milliseconds(20 * i)}
      end

    receiver = start_receiver!(relay, broadcast, @audio_track)

    sender =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:audio_source, %Testing.Source{
            stream_format: %Membrane.Opus{channels: 2, self_delimiting?: false},
            output: Testing.Source.output_from_buffers(buffers)
          })
          |> child(:realtimer, Membrane.Realtimer)
          |> via_in(Pad.ref(:input, :audio), options: [track: @audio_track])
          |> child(:moq_sink, %Membrane.MoQ.Sink{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
      )

    assert_sink_playing(receiver, :sink, 10_000)

    assert_sink_stream_format(receiver, :sink, %Membrane.Opus{channels: 2}, 10_000)

    assert_end_of_stream(sender, :moq_sink, Pad.ref(:input, :audio), 30_000)

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    assert drain_payloads(receiver, :sink) == Enum.map(buffers, & &1.payload)
  end

  @tag :flaky
  test "a two-pad A/V broadcast through one Sink is consumed by a two-pad Source", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver =
      Testing.Pipeline.start_link_supervised!(
        spec: [
          child(:source, %Membrane.MoQ.Source{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
          |> via_out(Pad.ref(:output, :video), options: [track: @track])
          |> child(:video_sink, Testing.Sink),
          get_child(:source)
          |> via_out(Pad.ref(:output, :audio), options: [track: @audio_track])
          |> child(:audio_sink, Testing.Sink)
        ]
      )

    sink = %Membrane.MoQ.Sink{
      url: relay.url,
      broadcast: broadcast,
      disable_tls_verify?: relay.disable_tls_verify?
    }

    sender =
      Testing.Pipeline.start_link_supervised!(
        spec: [
          child(:video_source, %Membrane.File.Source{location: "test/fixtures/bbb_with_aud.h264"})
          |> child(:video_parser, %Membrane.H264.Parser{
            output_stream_structure: :avc1,
            generate_best_effort_timestamps: %{framerate: {30, 1}}
          })
          |> child(:video_take, %Take{count: 30})
          |> child(:video_tee, Membrane.Tee)
          |> child(:video_realtimer, Membrane.Realtimer)
          |> via_in(Pad.ref(:input, :video), options: [track: @track])
          |> child(:moq_sink, sink),
          child(:audio_source, %Membrane.File.Source{location: "test/fixtures/bbb.aac"})
          |> child(:audio_parser, %Membrane.AAC.Parser{out_encapsulation: :none})
          |> child(:audio_take, %Take{count: 43})
          |> child(:audio_tee, Membrane.Tee)
          |> child(:audio_realtimer, Membrane.Realtimer)
          |> via_in(Pad.ref(:input, :audio), options: [track: @audio_track])
          |> get_child(:moq_sink),
          get_child(:video_tee) |> child(:expected_video, Testing.Sink),
          get_child(:audio_tee) |> child(:expected_audio, Testing.Sink)
        ]
      )

    assert_sink_playing(receiver, :video_sink, 10_000)
    assert_sink_playing(receiver, :audio_sink, 10_000)

    assert_sink_stream_format(receiver, :video_sink, %Membrane.H264{}, 10_000)
    assert_sink_stream_format(receiver, :audio_sink, %Membrane.AAC{}, 10_000)

    assert_end_of_stream(sender, :expected_video, :input, 30_000)
    assert_end_of_stream(sender, :expected_audio, :input, 30_000)
    expected_video = drain_payloads(sender, :expected_video)
    expected_audio = drain_payloads(sender, :expected_audio)
    assert expected_video != []
    assert expected_audio != []

    assert_end_of_stream(receiver, :video_sink, :input, 30_000)
    assert_end_of_stream(receiver, :audio_sink, :input, 30_000)

    assert drain_payloads(receiver, :video_sink) == expected_video
    assert drain_payloads(receiver, :audio_sink) == expected_audio
  end

  test "removing a Sink pad mid-stream makes the Source report :track_removed and end the pad",
       %{broadcast: broadcast, relay: relay} do
    # An endless paced stream guarantees the pad removal happens mid-stream —
    # a finite fixture could end naturally first, which also produces
    # :track_removed + EOS and would mask a broken removal path.
    endless_opus =
      {0,
       fn i, size ->
         buffers =
           for n <- i..(i + size - 1) do
             %Membrane.Buffer{payload: <<n::32>>, pts: Membrane.Time.milliseconds(20 * n)}
           end

         {[buffer: {:output, buffers}], i + size}
       end}

    receiver = start_receiver!(relay, broadcast, @audio_track)

    sender =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:audio_source, %Testing.Source{
            stream_format: %Membrane.Opus{channels: 2, self_delimiting?: false},
            output: endless_opus
          })
          |> child(:realtimer, Membrane.Realtimer)
          |> via_in(Pad.ref(:input, :audio), options: [track: @audio_track])
          |> child(:moq_sink, %Membrane.MoQ.Sink{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
      )

    assert_sink_playing(receiver, :sink, 10_000)
    assert_sink_buffer(receiver, :sink, %Membrane.Buffer{}, 10_000)

    Testing.Pipeline.execute_actions(sender, remove_children: [:audio_source, :realtimer])

    assert_pipeline_notified(receiver, :source, {:track_removed, @audio_track}, 10_000)
    assert_end_of_stream(receiver, :sink, :input, 10_000)
  end

  test "Source emits end_of_stream when the publisher disconnects after publishing a frame", %{
    broadcast: broadcast,
    relay: relay
  } do
    receiver = start_receiver!(relay, broadcast)
    sender = start_sender!(relay, broadcast)
    assert_sink_playing(receiver, :sink, 10_000)

    assert_sink_stream_format(receiver, :sink, _stream_format, 10_000)
    :ok = Membrane.Pipeline.terminate(sender)

    assert_end_of_stream(receiver, :sink, :input, 10_000)
  end

  test "Sink closes a pad that ended before its stream format without crashing", %{
    broadcast: broadcast,
    relay: relay
  } do
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
  end

  test "Sink skips delta frames preceding the first keyframe instead of crashing", %{
    broadcast: broadcast,
    relay: relay
  } do
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
  end

  @tag :flaky
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
    assert_sink_playing(receiver, :sink, 10_000)

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

  defp round_trip!(relay, broadcast, sender_opts \\ []) do
    receiver = start_receiver!(relay, broadcast)
    sender = start_sender!(relay, broadcast, sender_opts)
    assert_sink_playing(receiver, :sink, 10_000)

    assert_end_of_stream(sender, :expected_sink, :input, 30_000)
    expected = drain_buffers(sender, :expected_sink)
    assert expected != []

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    {expected, drain_buffers(receiver, :sink)}
  end

  defp start_receiver!(relay, broadcast, track \\ @track) do
    Testing.Pipeline.start_link_supervised!(
      spec:
        child(:source, %Membrane.MoQ.Source{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?
        })
        |> via_out(Pad.ref(:output, track), options: [track: track])
        |> child(:sink, Testing.Sink)
    )
  end

  defp start_audio_sender!(relay, broadcast, fixture \\ "test/fixtures/bbb.aac") do
    Testing.Pipeline.start_link_supervised!(
      spec: [
        child(:file_source, %Membrane.File.Source{location: fixture})
        |> child(:parser, %Membrane.AAC.Parser{out_encapsulation: :none})
        |> child(:take, %Take{count: 43})
        |> child(:tee, Membrane.Tee)
        |> child(:realtimer, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :audio), options: [track: @audio_track])
        |> child(:moq_sink, %Membrane.MoQ.Sink{
          url: relay.url,
          broadcast: broadcast,
          disable_tls_verify?: relay.disable_tls_verify?
        }),
        get_child(:tee)
        |> child(:expected_sink, Testing.Sink)
      ]
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
