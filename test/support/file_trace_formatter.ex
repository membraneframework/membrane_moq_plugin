defmodule Membrane.MoQ.Test.FileTraceFormatter do
  @moduledoc """
  ExUnit formatter appending a start/finish line per test to a trace file.

  On CI, ExUnit's stdout dies mid-suite when a test fails (dots, failure
  reports and the summary vanish while Logger output keeps flowing), so test
  results are unrecoverable from the job log. This formatter records them
  through `File` I/O, which bypasses the `:user` IO device entirely — a test
  that hangs or dies is also visible as a `start` line with no matching
  `ok`/`FAILED`. Enabled via `test_helper.exs` when `CI=true`; the trace lands
  in `/tmp/exunit_trace.log` for the CI step to print after the run.

  NOTE: LLM-generated.
  """

  use GenServer

  @path "/tmp/exunit_trace.log"

  @impl true
  def init(_opts) do
    File.write!(@path, "")
    {:ok, nil}
  end

  @impl true
  def handle_cast({:test_started, test}, state) do
    append("start #{test.module} #{test.name}")
    {:noreply, state}
  end

  def handle_cast({:test_finished, %ExUnit.Test{state: nil} = test}, state) do
    append("ok #{test.module} #{test.name}")
    {:noreply, state}
  end

  def handle_cast({:test_finished, %ExUnit.Test{state: {:failed, failures}} = test}, state) do
    # format_test_failure can raise on hostile failure content (it took down both
    # this formatter and the CLI one on CI) — fall back to a bounded raw dump.
    failure =
      try do
        ExUnit.Formatter.format_test_failure(test, failures, 1, 120, fn _type, msg -> msg end)
      rescue
        e ->
          "format_test_failure raised: #{inspect(e)}\n" <>
            "raw failures: #{inspect(failures, limit: 200, printable_limit: 4096)}"
      end

    append("FAILED #{test.module} #{test.name}\n#{failure}")
    {:noreply, state}
  end

  def handle_cast({:test_finished, test}, state) do
    append("#{inspect(test.state)} #{test.module} #{test.name}")
    {:noreply, state}
  end

  def handle_cast({:suite_finished, times_us}, state) do
    append("suite finished in #{inspect(times_us)}")
    {:noreply, state}
  end

  def handle_cast(_event, state), do: {:noreply, state}

  defp append(line), do: File.write!(@path, line <> "\n", [:append])
end
