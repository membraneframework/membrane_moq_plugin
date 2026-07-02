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
      .hang or .msf (e.g. format_change.hang).
      """)

      System.halt(1)
  end

Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand()},
  {:membrane_h26x_plugin, "~> 0.10.7"},
  {:membrane_file_plugin, "~> 0.17.3"},
  {:membrane_realtimer_plugin, "~> 0.11.0"}
])

Logger.configure(level: :debug)

# Exercises `Membrane.MoQ.Sink.handle_stream_format/4` firing more than once on
# the same pad while running, with real encoded media. Three fixtures are fed
# through one sink pad, in order:
#
#   1. H264 1280x720 @ 25fps
#   2. H264  640x360 @ 30fps   <- soft switch: same codec, different params
#   3. H265 1280x720 @ 25fps   <- switch from H264 to H265
#
# All three sources live in a single pipeline. A custom `Funnel` filter with one
# output and three on-request inputs plays them one at a time: using manual flow
# control it relays downstream demand only to the active input, switching to the
# next when the current one ends. It forwards each input's stream format (so the
# sink sees the change) and rebases timestamps so they stay monotonic across the
# switch.
#
# To watch the result live (e.g. with hang's web player against the relay) the
# stream is paced in real time: each parser stamps PTS from the framerate and a
# single `Membrane.Realtimer` sits between the funnel and the sink. The funnel
# uses manual flow control and relays downstream demand only to its active input,
# so the inactive inputs stay idle (never demanded) without any explicit pausing.
# The Realtimer paces the concatenated stream and, importantly, preserves the
# order of each stream-format change relative to its buffers, so the sink sees a
# format change exactly where it belongs in the paced output.

# Generates the three fixtures with ffmpeg (testsrc patterns, no B-frames so PTS
# is monotonic) into a temp dir, skipping any that already exist. The duration is
# baked into each filename, so bumping @duration_s regenerates them automatically
# (no need to delete the old ones, which live under System.tmp_dir!/0 - on macOS
# that's $TMPDIR, not /tmp).
defmodule Fixtures do
  @dir Path.join(System.tmp_dir!(), "moq_format_change_fixtures")

  # Length of each generated segment, in seconds. Bump this for longer segments.
  @duration_s 20

  # {key, basename, ffmpeg input/codec args (`DUR` and output path are filled in), ext}
  @specs [
    {:h264_720p25, "h264_1280x720_25",
     ~w(-f lavfi -i testsrc=size=1280x720:rate=25:duration=DUR -c:v libx264 -pix_fmt yuv420p -g 25 -bf 0 -f h264),
     "h264"},
    {:h264_360p30, "h264_640x360_30",
     ~w(-f lavfi -i testsrc2=size=640x360:rate=30:duration=DUR -c:v libx264 -pix_fmt yuv420p -g 30 -bf 0 -f h264),
     "h264"},
    {:h265_720p25, "h265_1280x720_25",
     ~w(-f lavfi -i testsrc=size=1280x720:rate=25:duration=DUR -c:v libx265 -pix_fmt yuv420p -g 25 -x265-params bframes=0 -f hevc),
     "h265"}
  ]

  def ensure_all() do
    File.mkdir_p!(@dir)

    Map.new(@specs, fn {key, basename, args, ext} ->
      name = "#{basename}_#{@duration_s}s.#{ext}"
      path = Path.join(@dir, name)

      unless File.exists?(path) do
        IO.puts("Generating fixture #{name} ...")
        args = Enum.map(args, &String.replace(&1, "duration=DUR", "duration=#{@duration_s}"))
        full_args = ~w(-hide_banner -loglevel error -y) ++ args ++ [path]
        {_out, 0} = System.cmd("ffmpeg", full_args, stderr_to_stdout: true)
      end

      {key, path}
    end)
  end
end

# Plays its on-request inputs one at a time, in pad-id order, switching to the
# next when the current input ends. Uses manual flow control: downstream demand
# is relayed only to the active input, so the others stay idle until selected.
# Forwards stream formats as they arrive (driving the sink's mid-stream format
# change) and rebases PTS so the output clock is continuous across inputs.
defmodule Funnel do
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
       order: [],
       active: nil,
       # Latest stream format seen per input. Formats aren't demand-gated, so an
       # inactive input's format can arrive early; we stash it and forward it when
       # that input becomes active.
       formats: %{},
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
    order = Enum.sort_by(state.order, fn Pad.ref(:input, id) -> id end)
    [active | _rest] = order

    Membrane.Logger.info("Funnel starting with input #{inspect(active)}")
    {[], %{state | order: order, active: active}}
  end

  # Relay downstream demand to the active input only; inactive inputs stay idle.
  @impl true
  def handle_demand(:output, size, :buffers, _ctx, %{active: active} = state) do
    {[demand: {active, size}], state}
  end

  @impl true
  def handle_stream_format(pad, format, _ctx, state) do
    state = put_in(state.formats[pad], format)

    if pad == state.active do
      {[stream_format: {:output, format}], state}
    else
      {[], state}
    end
  end

  @impl true
  def handle_buffer(pad, %Membrane.Buffer{} = buffer, _ctx, %{active: pad} = state) do
    pts = buffer.pts + state.offset
    delta = pts - state.last_pts
    frame_dur = if delta > 0, do: delta, else: state.frame_dur

    # Clear DTS so the downstream Realtimer paces on the rebased PTS.
    buffer = %Membrane.Buffer{buffer | pts: pts, dts: nil}
    {[buffer: {:output, buffer}], %{state | last_pts: pts, frame_dur: frame_dur}}
  end

  @impl true
  def handle_end_of_stream(pad, _ctx, %{active: pad} = state) do
    case next_pad(state.order, pad) do
      nil ->
        Membrane.Logger.info("Funnel: last input #{inspect(pad)} done, forwarding EOS")
        {[end_of_stream: :output], state}

      next ->
        Membrane.Logger.info("Funnel: switching #{inspect(pad)} -> #{inspect(next)}")
        state = %{state | active: next, offset: state.last_pts + state.frame_dur}

        # If the next input's format already arrived (it isn't demand-gated),
        # forward it now so it precedes the buffers; otherwise it'll be forwarded
        # live when it arrives. Then re-issue demand, relayed to the new input.
        format_action =
          case Map.fetch(state.formats, next) do
            {:ok, format} -> [stream_format: {:output, format}]
            :error -> []
          end

        {format_action ++ [redemand: :output], state}
    end
  end

  defp next_pad(order, pad) do
    order |> Enum.drop_while(&(&1 != pad)) |> Enum.drop(1) |> List.first()
  end
end

defmodule Example do
  use Membrane.Pipeline

  def start_link(opts) do
    Membrane.Pipeline.start_link(__MODULE__, opts)
  end

  @impl true
  def handle_init(_ctx, %{paths: paths, broadcast: broadcast}) do
    h264_parser = fn framerate ->
      %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: framerate},
        output_stream_structure: :avc1
      }
    end

    h265_parser =
      %Membrane.H265.Parser{
        generate_best_effort_timestamps: %{framerate: {25, 1}},
        output_stream_structure: :hvc1
      }

    inputs = [
      {0, paths.h264_720p25, h264_parser.({25, 1})},
      {1, paths.h264_360p30, h264_parser.({30, 1})},
      {2, paths.h265_720p25, h265_parser}
    ]

    spec = [
      child(:funnel, Funnel)
      |> child(:realtimer, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :video), options: [track: "video"])
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443/anon",
        broadcast: broadcast,
        disable_tls_verify?: true
      })
      | Enum.map(inputs, fn {id, path, parser} ->
          child({:source, id}, %Membrane.File.Source{location: path})
          |> child({:parser, id}, parser)
          |> via_in(Pad.ref(:input, id))
          |> get_child(:funnel)
        end)
    ]

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:sink, _pad, _ctx, state) do
    {[terminate: :normal], state}
  end

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state) do
    {[], state}
  end
end

paths = Fixtures.ensure_all()

{:ok, _supervisor_pid, pipeline_pid} = Example.start_link(%{paths: paths, broadcast: broadcast})
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, _pipeline_pid, _reason} ->
    :ok
end
