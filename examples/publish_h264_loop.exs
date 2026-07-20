# Publishes an H.264 video track to a local MoQ relay through `Membrane.MoQ.Sink`,
# looping the source clip forever so the broadcast never ends.
#
#   Hackney ─▶ H264.Parser(avc3) ─▶ VideoLooper ─▶ Realtimer ─▶ MoQ.Sink
#
# Prerequisites:
#   - A MoQ relay running at https://localhost:4443 (e.g. moq-relay)
#
# Run with:
#   elixir examples/publish_h264_loop.exs <broadcast>
#
# where <broadcast> is the broadcast name, ending with .hang or .msf
# (e.g. h264-loop.hang).

Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand()},
  {:membrane_realtimer_plugin, "~> 0.11.0"},
  {:membrane_hackney_plugin, "~> 0.11.1"},
  {:membrane_h26x_plugin, "~> 0.10.7"}
])

Logger.configure(level: :info)

defmodule VideoLooper do
  use Membrane.Filter

  alias Membrane.{Buffer, Time}

  def_input_pad :input,
    accepted_format: _any,
    flow_control: :manual,
    demand_unit: :buffers

  def_output_pad :output,
    accepted_format: _any,
    flow_control: :manual,
    demand_unit: :buffers

  def_options framerate: [
                spec: {pos_integer(), pos_integer()},
                default: {25, 1},
                description: """
                Frame rate of the source as `{numerator, denominator}`. Used to size
                the gap inserted between the last frame of one loop and the first
                frame of the next, so the looped timeline stays evenly paced.
                """
              ]

  @impl true
  def handle_init(_ctx, opts) do
    {num, den} = opts.framerate
    frame_duration = div(Time.seconds(den), num)

    {[],
     %{
       frame_duration: frame_duration,
       # Phase 1 — recording the live pass.
       recording?: true,
       recorded: [],
       first_dts: nil,
       last_dts: nil,
       # Phase 2 — replaying the captured clip.
       clip: [],
       remaining: [],
       period: 0,
       offset: 0
     }}
  end

  @impl true
  def handle_demand(:output, size, :buffers, _ctx, %{recording?: true} = state) do
    {[demand: {:input, size}], state}
  end

  @impl true
  def handle_demand(:output, size, :buffers, _ctx, %{recording?: false} = state) do
    {buffers, state} = take(size, [], state)
    {[buffer: {:output, buffers}], state}
  end

  @impl true
  def handle_stream_format(:input, format, _ctx, state) do
    {[stream_format: {:output, format}], state}
  end

  @impl true
  def handle_buffer(:input, %Buffer{} = buffer, _ctx, %{recording?: true} = state) do
    dts = Buffer.get_dts_or_pts(buffer)

    state = %{
      state
      | recorded: [buffer | state.recorded],
        first_dts: state.first_dts || dts,
        last_dts: dts
    }

    {[buffer: {:output, buffer}], state}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, %{recording?: true} = state) do
    case Enum.reverse(state.recorded) do
      [] ->
        {[end_of_stream: :output], state}

      clip ->
        period = state.last_dts - state.first_dts + state.frame_duration

        state = %{
          state
          | recording?: false,
            clip: clip,
            remaining: clip,
            period: period,
            offset: period,
            recorded: []
        }

        {[redemand: :output], state}
    end
  end

  defp take(0, acc, state), do: {Enum.reverse(acc), state}

  defp take(size, acc, %{remaining: []} = state) do
    take(size, acc, %{state | remaining: state.clip, offset: state.offset + state.period})
  end

  defp take(size, acc, %{remaining: [%Buffer{} = buffer | rest]} = state) do
    shifted = %{
      buffer
      | pts: shift(buffer.pts, state.offset),
        dts: shift(buffer.dts, state.offset)
    }

    take(size - 1, [shifted | acc], %{state | remaining: rest})
  end

  defp shift(nil, _offset), do: nil
  defp shift(timestamp, offset), do: timestamp + offset
end

defmodule Example do
  use Membrane.Pipeline

  @video_url "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_720x480.h264"
  @framerate {25, 1}

  @impl true
  def handle_init(_ctx, broadcast) do
    spec = [
      child(:video_source, %Membrane.Hackney.Source{
        location: @video_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:video_parser, %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: @framerate},
        # avc3 keeps SPS/PPS in-band so every looped keyframe stays decodable.
        output_stream_structure: :avc3
      })
      |> child(:looper, %VideoLooper{framerate: @framerate})
      |> child(:realtimer, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :video), options: [track: "video"])
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443/anon",
        broadcast: broadcast,
        disable_tls_verify?: true
      })
    ]

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:sink, _pad, _ctx, state), do: {[terminate: :normal], state}

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state), do: {[], state}
end

broadcast =
  case System.argv() do
    [broadcast | _rest] ->
      if String.ends_with?(broadcast, [".hang", ".msf"]) do
        broadcast
      else
        IO.puts(:stderr, "Broadcast name must end with .hang or .msf, got: #{broadcast}")
        System.halt(1)
      end

    [] ->
      IO.puts(:stderr, """
      Usage: elixir #{Path.relative_to_cwd(__ENV__.file)} <broadcast>

      <broadcast> is the name of the MoQ broadcast to publish; it must end with
      .hang or .msf (e.g. h264-loop.hang).
      """)

      System.halt(1)
  end

{:ok, _supervisor_pid, pipeline_pid} = Membrane.Pipeline.start_link(Example, broadcast)

IO.gets("\nPublishing — press Enter to stop the pipeline.\n")
:ok = Membrane.Pipeline.terminate(pipeline_pid)
