defmodule Membrane.MoQ.SinkTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  alias Membrane.Testing.Pipeline

  test "sink crashes during setup when given a malformed url" do
    pipeline = Pipeline.start_supervised!()
    ref = Process.monitor(pipeline)

    spec = child(:sink, %Membrane.MoQ.Sink{url: "not a url", broadcast: "test"})
    :ok = Pipeline.execute_actions(pipeline, spec: spec)

    assert_receive {:DOWN, ^ref, :process, ^pipeline, reason}, 5_000
    assert {:membrane_child_crash, :sink, _error} = reason
  end

  test "sink crashes on init when given the unsupported :cmaf container" do
    pipeline = Pipeline.start_supervised!()
    ref = Process.monitor(pipeline)

    spec =
      child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443",
        broadcast: "test",
        container: :cmaf
      })

    :ok = Pipeline.execute_actions(pipeline, spec: spec)

    # A raise in handle_init aborts the child startup, so the pipeline exits
    # with a ParentError instead of a child crash.
    assert_receive {:DOWN, ^ref, :process, ^pipeline, reason}, 5_000
    assert {%Membrane.ParentError{message: message}, _stacktrace} = reason
    assert message =~ ":cmaf is not supported"
  end
end
