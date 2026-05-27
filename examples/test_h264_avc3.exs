# Test: H264 avc3 stream with inline SPS/PPS (no DCR description needed).
# The H264 parser with `output_stream_structure: :avc3` puts SPS/PPS inline
# in each keyframe. The catalog description should be empty, and the decoder
# must be able to initialize from the in-band parameter sets.
#
# Usage: mix run examples/test_h264_avc3.exs

Mix.install([
  :membrane_realtimer_plugin,
  :membrane_hackney_plugin,
  :membrane_h26x_plugin,
  {:membrane_mp4_plugin, "~> 0.36.0"},
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true}
])

Logger.configure(level: :info)

defmodule TestH264Avc3 do
  use Membrane.Pipeline

  @input_url "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.mp4"

  def start_link() do
    Membrane.Pipeline.start_link(__MODULE__)
  end

  @impl true
  def handle_init(_ctx, _opts) do
    spec = [
      child(:source, %Membrane.Hackney.Source{
        location: @input_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:demuxer, Membrane.MP4.Demuxer.ISOM),

      # Demux video → parse as avc3 (SPS/PPS inline) → publish
      get_child(:demuxer)
      |> via_out(:output, options: [kind: :video])
      |> child(:video_parser, %Membrane.H264.Parser{
        output_stream_structure: :avc3
      })
      |> child(:realtimer, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :video1), options: [broadcast: "test-h264-avc3", track: "video"])
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

{:ok, _supervisor_pid, pipeline_pid} = TestH264Avc3.start_link()
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, _pipeline_pid, _reason} ->
    :ok
end
