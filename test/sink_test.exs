defmodule Membrane.MoQ.SinkTest do
  @moduledoc "Sink-only tests exercising simple setup-phase behaviours"

  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.MoQ.Test.Relay
  alias Membrane.Testing.Pipeline

  test "sink crashes during setup when given a malformed url" do
    pipeline = Pipeline.start_supervised!()
    ref = Process.monitor(pipeline)

    spec = child(:sink, %Membrane.MoQ.Sink{url: "not a url", broadcast: "test"})
    :ok = Pipeline.execute_actions(pipeline, spec: spec)

    assert_receive {:DOWN, ^ref, :process, ^pipeline, reason}, 5_000
    assert {:membrane_child_crash, :sink, _error} = reason
  end

  @tag :integration
  test "a session disconnect after setup notifies the parent instead of crashing" do
    relay = Relay.ensure!()

    pipeline =
      Pipeline.start_link_supervised!(
        spec:
          child(:sink, %Membrane.MoQ.Sink{
            url: relay.url,
            broadcast: "membrane/sink-disconnect-#{System.unique_integer([:positive])}",
            disable_tls_verify?: relay.disable_tls_verify?
          })
      )

    ref = Process.monitor(pipeline)

    assert_child_setup_completed(pipeline, :sink, 10_000)

    sink_pid = Pipeline.get_child_pid!(pipeline, :sink)
    send(sink_pid, {:moq_disconnected, "injected session close"})

    assert_pipeline_notified(pipeline, :sink, {:disconnected, "injected session close"}, 5_000)
    refute_receive {:DOWN, ^ref, :process, ^pipeline, _reason}, 500

    :ok = Membrane.Pipeline.terminate(pipeline)
  end
end
