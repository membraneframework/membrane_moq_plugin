# Publishes a sample H.264 video stream to a local MoQ relay.
#
# Prerequisites:
#   - A MoQ relay running at https://localhost:4443 (e.g. moq-relay)
#   - A hang-compatible subscriber (e.g. moq-obs, or the hang watch player)
#     subscribed to broadcast "example"
#
# Run with:
#   elixir examples/publish_h264.exs

Mix.install([
  :membrane_realtimer_plugin,
  :membrane_h26x_plugin,
  :membrane_file_plugin,
  {:membrane_h264_ffmpeg_plugin, "~> 0.32.6"},
  {:membrane_sdl_plugin, "~> 0.18.6"},
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true}
])

Logger.configure(level: :warn)

defmodule Example do
  use Membrane.Pipeline

  def start_link() do
    Membrane.Pipeline.start_link(__MODULE__)
  end

  @impl true
  def handle_init(_ctx, _opts) do
    structure = [
      child(:video_source, %Membrane.File.Source{
        location: "test/fixtures/bbb.h264"
      })
      |> child(:video_parser, %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: {25, 1}},
        output_alignment: :au
      })
      |> child(:realtimer, Membrane.Realtimer)
      |> child(:video_tee, Membrane.Tee),

      # --- MoQ branch ---
      get_child(:video_tee)
      |> via_out(Pad.ref(:output, :moq))
      |> via_in(Pad.ref(:input, :main))
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443/anon",
        broadcast: "example"
      }),

      # --- Playback branch ---
      get_child(:video_tee)
      |> via_out(Pad.ref(:output, :play))
      |> child(:video_decoder, Membrane.H264.FFmpeg.Decoder)
      |> child(:video_player, Membrane.SDL.Player)
    ]

    {[spec: structure], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:video_player, _pad, _ctx, state) do
    {[terminate: :shutdown], state}
  end

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state) do
    {[], state}
  end
end

{:ok, _supervisor_pid, pipeline_pid} = Example.start_link()
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, _pipeline_pid, _reason} ->
    :ok
end
