defmodule Membrane.MoQ.Test.Take do
  @moduledoc """
  NOTE: This test support module was LLM-generated.

  Test-only filter that forwards the first `count` buffers, then emits
  `end_of_stream` and drops the rest. Bounds an otherwise long fixture so a
  real-time-paced publish stays short.
  """

  use Membrane.Filter

  def_input_pad :input, accepted_format: _any, flow_control: :auto
  def_output_pad :output, accepted_format: _any, flow_control: :auto

  def_options count: [
                spec: pos_integer(),
                description: "Emit `end_of_stream` after forwarding this many buffers."
              ]

  @impl true
  def handle_init(_ctx, opts), do: {[], %{remaining: opts.count, done: false}}

  @impl true
  def handle_buffer(:input, _buffer, _ctx, %{done: true} = state), do: {[], state}

  def handle_buffer(:input, buffer, _ctx, state) do
    case state.remaining - 1 do
      0 -> {[buffer: {:output, buffer}, end_of_stream: :output], %{state | done: true}}
      remaining -> {[buffer: {:output, buffer}], %{state | remaining: remaining}}
    end
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, %{done: true} = state), do: {[], state}
  def handle_end_of_stream(:input, _ctx, state), do: {[end_of_stream: :output], state}
end
