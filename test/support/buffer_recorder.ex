defmodule Membrane.MoQ.Test.BufferRecorder do
  @moduledoc """
  Test-only filter that paces and mirrors a buffer stream.

  Sits in the middle of a pipeline and, for every buffer it forwards:

    * overwrites the buffer's `pts` with a monotonic timestamp that advances by
      `pts_step` per buffer (so a downstream `Membrane.Realtimer` paces the
      stream deterministically, independent of the source's own timestamps), and
    * sends `{:recorder, :buffer, payload}` to `recipient`.

  After forwarding `max_buffers` buffers it emits `end_of_stream`. On end of
  stream — whether reached via `max_buffers` or because the upstream ended
  first — it sends `{:recorder, :eos}` to `recipient`.

  This lets a test capture exactly the bytes published through the `Sink` and
  later assert they match the bytes received through the `Source`.
  """

  # __jm__ TODO: remove this module or add it to Membrane.Testing if necessary
  use Membrane.Filter

  def_options recipient: [
                spec: pid(),
                description:
                  "Process that receives `{:recorder, :buffer, payload}` and `{:recorder, :eos}` messages."
              ],
              pts_step: [
                spec: Membrane.Time.t(),
                description: "Amount the assigned `pts` advances per forwarded buffer."
              ],
              max_buffers: [
                spec: pos_integer(),
                description: "Stop and emit `end_of_stream` after forwarding this many buffers."
              ]

  def_input_pad :input, accepted_format: _any, flow_control: :auto
  def_output_pad :output, accepted_format: _any, flow_control: :auto

  @impl true
  def handle_init(_ctx, opts) do
    {[], %{recipient: opts.recipient, pts_step: opts.pts_step, max_buffers: opts.max_buffers, count: 0}}
  end

  @impl true
  def handle_buffer(:input, _buffer, _ctx, %{count: count, max_buffers: max} = state)
      when count >= max do
    # Cap already reached and `end_of_stream` already emitted; drop any buffers
    # that arrive before the upstream notices the stream is done.
    {[], state}
  end

  def handle_buffer(:input, buffer, _ctx, state) do
    # Reassign timestamps so the downstream Realtimer paces on our cadence
    # rather than on whatever timestamps the parser produced.
    buffer = %{buffer | pts: state.count * state.pts_step, dts: nil}
    send(state.recipient, {:recorder, :buffer, buffer.payload})

    state = %{state | count: state.count + 1}

    if state.count >= state.max_buffers do
      send(state.recipient, {:recorder, :eos})
      {[buffer: {:output, buffer}, end_of_stream: :output], state}
    else
      {[buffer: {:output, buffer}], state}
    end
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, %{count: count, max_buffers: max} = state)
      when count >= max do
    # We already emitted `end_of_stream` (and `{:recorder, :eos}`) on hitting
    # the cap; don't do it twice.
    {[], state}
  end

  def handle_end_of_stream(:input, _ctx, state) do
    send(state.recipient, {:recorder, :eos})
    {[end_of_stream: :output], state}
  end
end
