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

  test "Source emits end_of_stream when the publisher disconnects", %{broadcast: broadcast} do
    receiver = start_receiver!(broadcast)
    sender = start_sender!(broadcast)
    await_source_connected!(receiver)

    # Give the receiver time to also see at least one frame, then kill the
    # publisher mid-stream and check that EOS propagates.
    assert_receive {:recorder, :buffer, _payload}, 5_000
    :ok = Membrane.Pipeline.terminate(sender)

    assert_end_of_stream(receiver, :sink, :input, 10_000)
    :ok = Membrane.Pipeline.terminate(receiver)
  end

  defp start_receiver!(broadcast) do
    Pipeline.start_link_supervised!(
      spec:
        child(:source, %Membrane.MoQ.Source{
          url: @relay_url,
          broadcast: broadcast,
          track: @track,
          disable_tls_verify?: true
        })
        |> child(:sink, Sink)
    )
  end

  defp start_sender!(broadcast, opts \\ []) do
    max_buffers = Keyword.get(opts, :max_buffers, 30)
    pts_step_ms = Keyword.get(opts, :pts_step_ms, 33)

    Pipeline.start_link_supervised!(
      spec:
        child(:file_source, %Membrane.File.Source{
          location: "test/fixtures/bbb_with_aud.h264"
        })
        |> child(:parser, %Membrane.H264.Parser{output_stream_structure: :avc1})
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
          options: [broadcast: broadcast, track: @track]
        )
        |> child(:moq_sink, %Membrane.MoQ.Sink{
          url: @relay_url,
          disable_tls_verify?: true
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
       {:handle_child_notification,
        {{:buffer, %Membrane.Buffer{payload: payload}}, :sink}}} ->
        drain_received_payloads(pipeline, [payload | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
