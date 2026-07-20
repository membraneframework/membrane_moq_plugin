Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand()},
  {:membrane_realtimer_plugin, "~> 0.11.0"},
  {:membrane_aac_plugin, "~> 0.19.2"},
  {:membrane_h26x_plugin, "~> 0.10.7"},
  {:membrane_hackney_plugin, "0.11.1"},
  {:membrane_mp4_plugin, "~> 0.36.5"},
  {:membrane_h264_ffmpeg_plugin, "~> 0.32.6"}
])

defmodule Example do
  use Membrane.Pipeline

  @input_url "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.mp4"

  @impl true
  def handle_init(_ctx, broadcast) do
    spec = [
      child(:sink, %Membrane.MoQ.Sink{
        url: "http://localhost:4443/anon",
        broadcast: broadcast,
        disable_tls_verify?: true
      }),
      child(:source, %Membrane.Hackney.Source{
        location: @input_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:demuxer, Membrane.MP4.Demuxer.ISOM),
      get_child(:demuxer)
      |> via_out(:output, options: [kind: :audio])
      |> child(:audio_parser, %Membrane.AAC.Parser{
        out_encapsulation: :none
      })
      |> child(:audio_rt, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :audio1), options: [track: "audio"])
      |> get_child(:sink),
      get_child(:demuxer)
      |> via_out(:output, options: [kind: :video])
      |> child(:video_parser, %Membrane.H264.Parser{output_stream_structure: :avc1})
      |> child(:video_rt, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :video1), options: [track: "video"])
      |> get_child(:sink)
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
      .hang or .msf (e.g. bbb.hang).
      """)

      System.halt(1)
  end

{:ok, _supervisor_pid, pipeline_pid} = Membrane.Pipeline.start_link(Example, broadcast)
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, ^pipeline_pid, _reason} ->
    :ok
end
