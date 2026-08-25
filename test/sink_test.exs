defmodule Membrane.MoQ.SinkTest do
  @moduledoc "Sink-only tests exercising simple setup-phase behaviours"

  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias ExMoQ.Test.Relay
  alias Membrane.Testing

  test "sink crashes during setup when given a malformed url" do
    pipeline = Testing.Pipeline.start_supervised!()
    ref = Process.monitor(pipeline)

    spec = child(:sink, %Membrane.MoQ.Sink{url: "not a url", broadcast: "test"})
    :ok = Testing.Pipeline.execute_actions(pipeline, spec: spec)

    assert_receive {:DOWN, ^ref, :process, ^pipeline, reason}, 5_000
    assert {:membrane_child_crash, :sink, _error} = reason
  end

  @tag :integration
  test "a session disconnect after setup notifies the parent" do
    relay = Relay.start_supervised!()

    pipeline =
      Testing.Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Membrane.MoQ.Sink{
            url: relay.url,
            broadcast: "membrane/sink-disconnect-#{System.unique_integer([:positive])}",
            disable_tls_verify?: relay.disable_tls_verify?
          })
      )

    assert_child_setup_completed(pipeline, :sink, 10_000)

    sink_pid = Testing.Pipeline.get_child_pid!(pipeline, :sink)
    send(sink_pid, {:moq_disconnected, "injected session close"})

    assert_pipeline_notified(pipeline, :sink, {:disconnected, "injected session close"})
  end
end
