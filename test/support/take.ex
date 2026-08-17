defmodule Membrane.MoQ.Test.Take do
  @moduledoc """
  Test-only filter that forwards the first `count` buffers,
  then emits `end_of_stream` and drops the rest.
  """

  use Membrane.Filter

  def_input_pad :input, accepted_format: _any, flow_control: :auto
  def_output_pad :output, accepted_format: _any, flow_control: :auto

  def_options count: [
                spec: pos_integer(),
                description: "Emit `end_of_stream` after forwarding this many buffers."
              ]

  @impl true
  def handle_init(_ctx, opts), do: {[], %{remaining: opts.count}}

  @impl true
  def handle_buffer(:input, _buffer, %{pads: %{output: %{end_of_stream?: true}}} = _ctx, state) do
    {[], state}
  end

  def handle_buffer(:input, buffer, _ctx, state) do
    case state.remaining - 1 do
      0 -> {[buffer: {:output, buffer}, end_of_stream: :output], state}
      remaining -> {[buffer: {:output, buffer}], %{state | remaining: remaining}}
    end
  end

  @impl true
  def handle_end_of_stream(:input, %{pads: %{output: %{end_of_stream?: true}}} = _ctx, state),
    do: {[], state}

  def handle_end_of_stream(:input, _ctx, state), do: {[end_of_stream: :output], state}
end
