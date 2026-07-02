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
      .hang or .msf (e.g. test-h264-avc1.hang).
      """)

      System.halt(1)
  end

Mix.install([
  :membrane_realtimer_plugin,
  :membrane_hackney_plugin,
  :membrane_h26x_plugin,
  {:membrane_mp4_plugin, "~> 0.36.0"},
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true}
])

Logger.configure(level: :info)

defmodule TestH264Avc1 do
  use Membrane.Pipeline

  @input_url "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/bun33s.mp4"

  def start_link(broadcast) do
    Membrane.Pipeline.start_link(__MODULE__, broadcast)
  end

  @impl true
  def handle_init(_ctx, broadcast) do
    spec = [
      child(:source, %Membrane.Hackney.Source{
        location: @input_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:demuxer, Membrane.MP4.Demuxer.ISOM)
      |> via_out(:output, options: [kind: :video])
      |> child(:video_parser, %Membrane.H264.Parser{
        output_stream_structure: :avc1
      })
      |> child(:realtimer, Membrane.Realtimer)
      |> via_in(Pad.ref(:input, :video1), options: [track: "video"])
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443/anon",
        broadcast: broadcast,
        disable_tls_verify?: true
      }),
      get_child(:demuxer)
      |> via_out(:output, options: [kind: :audio])
      |> child(:fake, Membrane.Fake.Sink)
    ]

    {[spec: spec], %{}}
  end

  @impl true
  def handle_element_end_of_stream(:sink, _pad, _ctx, state), do: {[terminate: :normal], state}

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state), do: {[], state}
end

{:ok, _supervisor_pid, pipeline_pid} = TestH264Avc1.start_link(broadcast)
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, _pipeline_pid, _reason} ->
    :ok
end
