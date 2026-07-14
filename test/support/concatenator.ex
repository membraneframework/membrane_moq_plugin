defmodule Membrane.MoQ.Test.Concatenator do
  @moduledoc """
  Concatenates inputs into one continuous stream.

  Plays its on-request inputs one at a time, in linking order,
  switching to the next when the current input ends.
  Uses manual flow control: downstream demand is relayed only to the active input,
  so the others stay idle until selected.

  Forwards stream formats as they arrive and rebases PTS
  so the output clock is continuous across inputs.
  """

  # NOTE: it would probably be a good idea
  # to upstream this filter like Membrane.Funnel

  use Membrane.Filter

  require Membrane.Logger

  def_input_pad :input,
    availability: :on_request,
    accepted_format: any_of(Membrane.H264, Membrane.H265),
    flow_control: :manual,
    demand_unit: :buffers

  def_output_pad :output,
    accepted_format: any_of(Membrane.H264, Membrane.H265),
    flow_control: :manual

  @impl true
  def handle_init(_ctx, _opts) do
    {[],
     %{
       # Remaining inputs in play order; the head is the active input.
       order: [],
       # Added to every input PTS so the output clock never jumps back at a switch.
       offset: 0,
       last_pts: 0,
       # Last observed frame duration, used to leave a one-frame gap at a switch.
       frame_dur: Membrane.Time.milliseconds(40)
     }}
  end

  @impl true
  def handle_pad_added(Pad.ref(:input, _id) = pad, _ctx, state) do
    {[], %{state | order: state.order ++ [pad]}}
  end

  @impl true
  def handle_playing(_ctx, state) do
    [active | _rest] = order = Enum.sort_by(state.order, fn Pad.ref(:input, id) -> id end)

    Membrane.Logger.info("Concatenator starting with input #{inspect(active)}")
    {[], %{state | order: order}}
  end

  # Relay downstream demand to the active input only; inactive inputs stay idle.
  @impl true
  def handle_demand(:output, size, :buffers, _ctx, %{order: [active | _rest]} = state) do
    {[demand: {active, size}], state}
  end

  @impl true
  def handle_stream_format(pad, format, _ctx, %{order: [pad | _rest]} = state) do
    {[stream_format: {:output, format}], state}
  end

  @impl true
  def handle_stream_format(_pad, _format, _ctx, state) do
    {[], state}
  end

  @impl true
  def handle_buffer(pad, %Membrane.Buffer{} = buffer, _ctx, %{order: [pad | _rest]} = state) do
    pts = buffer.pts + state.offset
    delta = pts - state.last_pts
    frame_dur = if delta > 0, do: delta, else: state.frame_dur

    # Clear DTS so the downstream Realtimer paces on the rebased PTS.
    buffer = %Membrane.Buffer{buffer | pts: pts, dts: nil}
    {[buffer: {:output, buffer}], %{state | last_pts: pts, frame_dur: frame_dur}}
  end

  @impl true
  def handle_end_of_stream(pad, ctx, %{order: [pad | rest]} = state) do
    case Enum.drop_while(rest, &ctx.pads[&1].end_of_stream?) do
      [] ->
        Membrane.Logger.info("Concatenator: last input #{inspect(pad)} done, forwarding EOS")
        {[end_of_stream: :output], state}

      [next | _rest] = order ->
        Membrane.Logger.info("Concatenator: switching #{inspect(pad)} -> #{inspect(next)}")
        state = %{state | order: order, offset: state.last_pts + state.frame_dur}

        format_action =
          case ctx.pads[next].stream_format do
            nil -> []
            format -> [stream_format: {:output, format}]
          end

        {format_action ++ [redemand: :output], state}
    end
  end

  @impl true
  def handle_end_of_stream(_pad, _ctx, state) do
    {[], state}
  end
end
