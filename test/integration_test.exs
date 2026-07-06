defmodule Membrane.MoQ.IntegrationTest do
  @moduledoc """
  End-to-end tests that exercise `Membrane.MoQ.Sink` and `Membrane.MoQ.Source`
  against a real MoQ relay.

  These tests require a running MoQ relay; set `RELAY_URL` to point at it
  (default: `https://localhost:4443`, matching the `localhost.toml` config
  from moq-rs). They are tagged `:integration` and excluded from the default
  test run; opt in with:

      RELAY_URL=https://localhost:4443 mix test --include integration
  """

  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Membrane.Pad

  alias Membrane.MoQ.Test.BufferRecorder
  alias Membrane.Pad
  alias Membrane.Testing.{Pipeline, Sink}

  @moduletag :integration

  @relay_url System.get_env("RELAY_URL", "https://localhost:4443")
  # A local relay (like the one from `localhost.toml`) uses a self-signed
  # certificate, so TLS verification must be off; a remote relay is expected
  # to present a valid certificate.
  @disable_tls_verify? URI.parse(@relay_url).host in ["localhost", "127.0.0.1", "::1"]
  # Each test uses its own broadcast name so concurrent test runs don't fight
  # over the same path. The name is randomised per test below.
  @track "video"

  setup do
    broadcast = "membrane/test-#{System.unique_integer([:positive])}"
    [broadcast: broadcast]
  end

  test "frames sent through the Sink are received unchanged by the Source", %{
    broadcast: broadcast
  } do
    receiver = start_receiver!(broadcast)
    # Without `await_source_connected!`, the sender races the receiver: the
    # source can finish setup AFTER the publisher has already finished, in
    # which case it sees no frames at all. The Sink announces in
    # `handle_pad_added`, so by the time we get past `start_sender!` the
    # broadcast is in the relay's announcement list.
    sender = start_sender!(broadcast)
    await_source_connected!(receiver)

    expected_payloads = collect_recorded_payloads()
    assert expected_payloads != []

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    received_payloads = drain_received_payloads(receiver)

    assert received_payloads == expected_payloads

    :ok = Membrane.Pipeline.terminate(sender)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  test "avc3 frames (in-band parameter sets) round-trip unchanged through the Source", %{
    broadcast: broadcast
  } do
    receiver = start_receiver!(broadcast)
    _sender = start_sender!(broadcast, stream_structure: :avc3)
    await_source_connected!(receiver)

    expected_payloads = collect_recorded_payloads()
    assert expected_payloads != []

    assert_end_of_stream(receiver, :sink, :input, 30_000)
    received_payloads = drain_received_payloads(receiver)

    assert received_payloads == expected_payloads
  end

  test "Source emits end_of_stream when the publisher disconnects after publishing a frame", %{
    broadcast: broadcast
  } do
    receiver = start_receiver!(broadcast)
    sender = start_sender!(broadcast)
    await_source_connected!(receiver)

    assert_sink_stream_format(receiver, :sink, _stream_format, 10_000)
    :ok = Membrane.Pipeline.terminate(sender)

    assert_end_of_stream(receiver, :sink, :input, 10_000)
  end

  test "Sink closes a pad that ended before its stream format without crashing", %{
    broadcast: broadcast
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
        url: @relay_url,
        broadcast: broadcast,
        disable_tls_verify?: @disable_tls_verify?
      })

    pipeline = Pipeline.start_supervised!(spec: spec)
    ref = Process.monitor(pipeline)

    refute_receive {:DOWN, ^ref, :process, ^pipeline,
                    {:membrane_child_crash, :moq_sink, _reason}},
                   5_000
  end

  defp start_receiver!(broadcast) do
    Pipeline.start_link_supervised!(
      spec:
        child(:source, %Membrane.MoQ.Source{
          url: @relay_url,
          broadcast: broadcast,
          disable_tls_verify?: @disable_tls_verify?
        })
        |> via_out(Pad.ref(:output, :video), options: [track: @track])
        |> child(:sink, Sink)
    )
  end

  defp start_sender!(broadcast, opts \\ []) do
    max_buffers = Keyword.get(opts, :max_buffers, 30)
    pts_step_ms = Keyword.get(opts, :pts_step_ms, 33)
    stream_structure = Keyword.get(opts, :stream_structure, :avc1)

    Pipeline.start_link_supervised!(
      spec:
        child(:file_source, %Membrane.File.Source{
          location: "test/fixtures/bbb_with_aud.h264"
        })
        |> child(:parser, %Membrane.H264.Parser{output_stream_structure: stream_structure})
        |> child(:recorder, %BufferRecorder{
          recipient: self(),
          pts_step: Membrane.Time.milliseconds(pts_step_ms),
          max_buffers: max_buffers
        })
        # Realtimer paces the publish based on the pts assigned by
        # `BufferRecorder`, so the relay has time to forward frames to the
        # receiver before the broadcast closes.
        |> child(:realtimer, Membrane.Realtimer)
        |> via_in(Pad.ref(:input, :video),
          options: [track: @track]
        )
        |> child(:moq_sink, %Membrane.MoQ.Sink{
          url: @relay_url,
          broadcast: broadcast,
          disable_tls_verify?: @disable_tls_verify?
        })
    )
  end

  defp await_source_connected!(receiver),
    do: assert_sink_playing(receiver, :sink, 10_000)

  defp collect_recorded_payloads(acc \\ []) do
    receive do
      {:recorder, :buffer, payload} -> collect_recorded_payloads([payload | acc])
      {:recorder, :eos} -> Enum.reverse(acc)
    after
      30_000 ->
        flunk(
          "timed out waiting for recorder EOS after #{length(acc)} buffers; " <>
            "is the relay accepting publishes?"
        )
    end
  end

  defp drain_received_payloads(pipeline, acc \\ []) do
    receive do
      {Membrane.Testing.Pipeline, ^pipeline,
       {:handle_child_notification, {{:buffer, %Membrane.Buffer{payload: payload}}, :sink}}} ->
        drain_received_payloads(pipeline, [payload | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
