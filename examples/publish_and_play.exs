# Loopback example: publishes an H.264 video to a local MoQ relay through
# `Membrane.MoQ.Sink`, then subscribes to the same broadcast with
# `Membrane.MoQ.Source` and plays it back locally in an SDL window.
#
# This uses TWO separate pipelines on purpose:
#
#   Publisher: Hackney ─▶ H264.Parser(avc3) ─▶ Realtimer ─▶ MoQ.Sink
#   Player:    MoQ.Source ─▶ H264.Parser(→annexb) ─▶ FFmpeg.Decoder ─▶ SDL
#
# They cannot share one pipeline: `MoQ.Source` keeps its setup `:incomplete`
# until it discovers the published track in the catalog, but the track only
# appears once the Sink publishes — which needs the pipeline `:playing`, which
# in turn waits for every element (including the Source) to finish setup. In one
# pipeline that is a deadlock. As two pipelines the publisher reaches `:playing`
# and starts publishing on its own, and the player's Source then completes setup.
#
# Two further details make the playback half work:
#
#   * The stream is published as `:avc3` (not `:avc1`). `MoQ.Source` surfaces the
#     catalog's H.264 config, but here that is a minimal avc3 decoder record with
#     no parameter sets, so the SPS/PPS must travel in-band. `:avc3` keeps them in
#     the bitstream; `:avc1` would move them into the DCR and the receiver would
#     have nothing to configure the decoder with.
#   * `MoQ.Source` emits the access units as `Membrane.H264` with an `:avc3`
#     stream structure (length-prefixed AVCC payloads). The player's `H264.Parser`
#     re-emits them as Annex B, which is what `Membrane.H264.FFmpeg.Decoder`
#     accepts.
#
# The publish side is paced with `Membrane.Realtimer` so the broadcast stays live
# long enough for the subscriber to join and keep up.
#
# Prerequisites:
#   - A MoQ relay running at https://localhost:4443 (e.g. moq-relay)
#   - ffmpeg + SDL available for the decoder/player plugins
#
# Run with:
#   elixir examples/publish_and_play.exs

Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true},
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

  def start_link(opts), do: Membrane.Pipeline.start_link(__MODULE__, opts)

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

  def start_link(opts), do: Membrane.Pipeline.start_link(__MODULE__, opts)

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

  # Tear the pipeline down once the playback window has shown the last frame.
  @impl true
  def handle_element_end_of_stream(:player, _pad, _ctx, state), do: {[terminate: :normal], state}

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state), do: {[], state}
end

opts = [url: "https://localhost:4443/anon", broadcast: "playback", track: "video"]

# Start the publisher first and give it a moment to establish the broadcast and
# begin publishing, so the player's Source subscribes to a catalog that is
# already serving the video rendition.
{:ok, _pub_sup, publisher} = Publisher.start_link(opts)
Process.sleep(1_000)
{:ok, _play_sup, player} = Player.start_link(opts)

# Exit when playback finishes (or the window is closed), then stop the publisher.
ref = Process.monitor(player)

receive do
  {:DOWN, ^ref, :process, _pid, _reason} ->
    Membrane.Pipeline.terminate(publisher)
end
