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
end
