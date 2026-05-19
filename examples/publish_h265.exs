Mix.install([
  :membrane_realtimer_plugin,
  :membrane_hackney_plugin,
  :membrane_h26x_plugin,
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true},
])

Logger.configure(level: :debug)

defmodule Example do
  use Membrane.Pipeline

  def start_link() do
    Membrane.Pipeline.start_link(__MODULE__)
  end

  @video_url "http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/ffmpeg-testsrc.h265"

  @impl true
  def handle_init(_ctx, _opts) do
    spec = [
      child(:video_source, %Membrane.Hackney.Source{
        location: @video_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:video_parser, %Membrane.H265.Parser{
        generate_best_effort_timestamps: %{framerate: {25, 1}},
        output_stream_structure: :hvc1
      })
      |> child(:realtimer, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :main), options: [broadcast: "bbb", track: "video"])
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443",
        disable_tls_verify?: true
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

{:ok, _supervisor_pid, pipeline_pid} = Example.start_link()
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, _pipeline_pid, _reason} ->
    :ok
end
