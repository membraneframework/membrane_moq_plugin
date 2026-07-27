defmodule Membrane.MoQ.SinkDisconnectTest do
  @moduledoc """
  Sink behavior when the MoQ session drops mid-stream while upstream keeps
  producing buffers.
  """

  use ExUnit.Case, async: true

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad

  alias Membrane.MoQ.Test.Relay
  alias Membrane.Pad
  alias Membrane.Testing

  @moduletag :integration

  @track "video"
  @fixture "test/fixtures/format_change/h264_1280x720_25.h264"

  test "buffers after :moq_disconnected are ignored so the parent can unlink the sink" do
    relay = Relay.start_supervised!()
    broadcast = "membrane/sink-disconnect-#{System.unique_integer([:positive])}"

    # Unlinked from the test process so a sink crash is observed via the
    # monitor instead of killing the test.
    publisher =
      Testing.Pipeline.start_supervised!(
        spec:
          child(:file, %Membrane.File.Source{location: @fixture})
          |> child(:parser, %Membrane.H264.Parser{
            generate_best_effort_timestamps: %{framerate: {25, 1}},
            output_stream_structure: :avc1
          })
          |> child(:realtimer, Membrane.Realtimer)
          |> via_in(Pad.ref(:input, @track), options: [track: @track])
          |> child(:sink, %Membrane.MoQ.Sink{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?
          })
      )

    ref = Process.monitor(publisher)

    receiver =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:source, %Membrane.MoQ.Source{
            url: relay.url,
            broadcast: broadcast,
            disable_tls_verify?: relay.disable_tls_verify?,
            latency: Membrane.Time.milliseconds(200)
          })
          |> via_out(Pad.ref(:output, @track), options: [track: @track])
          |> child(:sink, Testing.Sink)
      )

    assert_sink_buffer(receiver, :sink, %Membrane.Buffer{}, 15_000)
    stop_supervised!(Relay)

    assert_pipeline_notified(publisher, :sink, {:disconnected, _reason}, 10_000)
    assert_pipeline_notified(receiver, :source, {:disconnected, _reason}, 10_000)

    refute_receive {:DOWN, ^ref, :process, ^publisher, _reason}, 2_000

    # Unlinking upstream removes the living sink's input pad; only then is the
    # sink itself removed. Removing everything at once would terminate the
    # sink outright and never exercise its pad-removal path.
    Testing.Pipeline.execute_actions(publisher, remove_children: [:file, :parser, :realtimer])
    refute_receive {:DOWN, ^ref, :process, ^publisher, _reason}, 1_000

    Testing.Pipeline.execute_actions(publisher, remove_children: [:sink])
    refute_receive {:DOWN, ^ref, :process, ^publisher, _reason}, 1_000
  end
end
