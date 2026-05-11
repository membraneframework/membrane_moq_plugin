# TODO: inspect, the video is a little choppy

Mix.install([
  :membrane_realtimer_plugin,
  :membrane_aac_plugin,
  :membrane_h26x_plugin,
  :membrane_hackney_plugin,
  {:membrane_mp4_plugin, "~> 0.36.0"},
  {:membrane_h264_ffmpeg_plugin, "~> 0.32.6"},
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true}
])

defmodule Example do
  use Membrane.Pipeline

  alias Membrane.Time

  @input_url "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.mp4"

  def start_link() do
    Membrane.Pipeline.start_link(__MODULE__)
  end

  @impl true
  def handle_init(_ctx, _opts) do
    spec = [
      child(:sink, %Membrane.MoQ.Sink{
        url: "http://localhost:4443"
      }),

      child(:source, %Membrane.Hackney.Source{
        location: @input_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:demuxer, Membrane.MP4.Demuxer.ISOM),

      get_child(:demuxer)
      |> via_out(:output, options: [kind: :audio])
      |> child(:audio_parser, %Membrane.AAC.Parser{
        out_encapsulation: :ADTS
      })
      |> child(:audio_rt, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :audio1), options: [broadcast: "bbb", track: "audio"])
      |> get_child(:sink),

      get_child(:demuxer)
      |> via_out(:output, options: [kind: :video])
      |> child(:video_parser, %Membrane.H264.Parser{output_stream_structure: :annexb})
      |> child(:video_rt, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :video1), options: [broadcast: "bbb", track: "video"])
      |> get_child(:sink)
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
