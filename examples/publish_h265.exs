Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand()},
  {:membrane_realtimer_plugin, " ~> 0.11.0"},
  {:membrane_hackney_plugin, "~> 0.11.1"},
  {:membrane_h26x_plugin, "~> 0.10.7"}
])

Logger.configure(level: :info)

defmodule Example do
  use Membrane.Pipeline

  @video_url "http://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/ffmpeg-testsrc.h265"

  @impl true
  def handle_init(_ctx, broadcast) do
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
      |> via_in(Pad.ref(:input, :main), options: [track: "video"])
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443/anon",
        broadcast: broadcast,
        disable_tls_verify?: true
      })
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
      .hang or .msf (e.g. h265.hang).
      """)

      System.halt(1)
  end

{:ok, _supervisor_pid, pipeline_pid} = Membrane.Pipeline.start_link(Example, broadcast)
ref = Process.monitor(pipeline_pid)

receive do
  {:DOWN, ^ref, :process, ^pipeline_pid, _reason} ->
    :ok
end
