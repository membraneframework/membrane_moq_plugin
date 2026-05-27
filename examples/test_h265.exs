# Test: H265 stream with DCR description passed in catalog.
# Raw H265 Annex B → parsed into hev1 (out-of-band VPS/SPS/PPS in DCR).
# The sink forwards the hvcC DCR as the catalog `description`.
#
# Usage: mix run examples/test_h265.exs

Mix.install([
  :membrane_realtimer_plugin,
  :membrane_hackney_plugin,
  :membrane_h26x_plugin,
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true}
])

Logger.configure(level: :debug)

defmodule TestH265 do
  use Membrane.Pipeline

  @video_url "http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/ffmpeg-testsrc.h265"

  def start_link() do
    Membrane.Pipeline.start_link(__MODULE__)
  end

  @impl true
  def handle_init(_ctx, _opts) do
    spec = [
      child(:source, %Membrane.Hackney.Source{
        location: @video_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:video_parser, %Membrane.H265.Parser{
        generate_best_effort_timestamps: %{framerate: {25, 1}},
        output_stream_structure: :hev1,
        output_alignment: :au
      })
      |> child(:realtimer, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :main), options: [broadcast: "bbb", track: "video"])
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443"
      })
    ]

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:sink, _pad, _ctx, state) do
    {[terminate: :shutdown], state}
  end

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state) do
    {[], state}
  end
end

{:ok, _supervisor_pid, pipeline_pid} = TestH265.start_link()
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, _pipeline_pid, _reason} ->
    :ok
end
