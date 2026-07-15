# Loopback example: publishes an H.264 video to a local MoQ relay through
# `Membrane.MoQ.Sink`, then subscribes to the same broadcast with
# `Membrane.MoQ.Source` and plays it back locally in an SDL window.
#
# This uses TWO separate pipelines:
#
#   Publisher: Hackney ─▶ H264.Parser(avc3) ─▶ Realtimer ─▶ MoQ.Sink
#   Player:    MoQ.Source ─▶ H264.Parser(→annexb) ─▶ FFmpeg.Decoder ─▶ SDL
#
# Prerequisites:
#   - A MoQ relay running at https://localhost:4443 (e.g. moq-relay)
#   - ffmpeg + SDL available for the decoder/player plugins
#
# Run with:
#   elixir examples/publish_and_play.exs <broadcast>
#
# where <broadcast> is the broadcast name, ending with .hang or .msf
# (e.g. playback.hang).

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

      <broadcast> is the name of the MoQ broadcast to publish and play back; it
      must end with .hang or .msf (e.g. playback.hang).
      """)

      System.halt(1)
  end

Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand()},
  {:membrane_realtimer_plugin, "~> 0.11.0"},
  {:membrane_hackney_plugin, "~> 0.11.1"},
  {:membrane_h26x_plugin, "~> 0.10.7"},
  {:membrane_h264_ffmpeg_plugin, "~> 0.32.6"},
  {:membrane_sdl_plugin, "~> 0.18.6"}
])

Logger.configure(level: :info)

defmodule Publisher do
  use Membrane.Pipeline

  @video_url "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s_720x480.h264"

  @impl true
  def handle_init(_ctx, opts) do
    spec = [
      child(:http_source, %Membrane.Hackney.Source{
        location: @video_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:parser, %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: {25, 1}},
        # avc3 keeps SPS/PPS in-band so the subscriber can configure its decoder.
        output_stream_structure: :avc3
      })
      |> child(:realtimer, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :video), options: [track: opts[:track]])
      |> child(:sink, %Membrane.MoQ.Sink{
        url: opts[:url],
        broadcast: opts[:broadcast],
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

defmodule Player do
  use Membrane.Pipeline

  @impl true
  def handle_init(_ctx, opts) do
    spec = [
      child(:source, %Membrane.MoQ.Source{
        url: opts[:url],
        broadcast: opts[:broadcast],
        disable_tls_verify?: true
      })
      |> via_out(Pad.ref(:output, :video), options: [track: opts[:track]])
      # `MoQ.Source` emits frames in decode order carrying their presentation
      # PTS but no DTS, so the decoder has no monotonic decode timeline to
      # reorder against and would emit frames still in decode order — which the
      # downstream renders as jitter (and which an unhelped `Realtimer` cannot
      # smooth, since it paces by a PTS that runs backwards). Regenerating
      # timestamps here derives monotonic DTS/PTS from the H.264 structure so the
      # decoder outputs clean presentation order. (See bench/ for the measurement.)
      # `:annexb` output is what `Membrane.H264.FFmpeg.Decoder` accepts.
      |> child(:parser, %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: {25, 1}},
        output_stream_structure: :annexb
      })
      |> child(:decoder, Membrane.H264.FFmpeg.Decoder)
      |> child(:rt, Membrane.Realtimer)
      |> child(:player, Membrane.SDL.Player)
    ]

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:player, _pad, _ctx, state), do: {[terminate: :normal], state}

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state), do: {[], state}
end

opts = [url: "https://localhost:4443/anon", broadcast: broadcast, track: "video"]

{:ok, _pub_sup, publisher} = Membrane.Pipeline.start_link(Publisher, opts)
{:ok, _play_sup, player} = Membrane.Pipeline.start_link(Player, opts)

ref = Process.monitor(player)

receive do
  {:DOWN, ^ref, :process, ^player, _reason} ->
    Membrane.Pipeline.terminate(publisher)
end
