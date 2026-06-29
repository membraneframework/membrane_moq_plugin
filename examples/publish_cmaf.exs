Mix.install([
  :membrane_aac_plugin,
  :membrane_h26x_plugin,
  :membrane_hackney_plugin,
  {:membrane_mp4_plugin, "~> 0.34.0"},
  {:membrane_h264_ffmpeg_plugin, "~> 0.32.6"},
  {:membrane_aac_fdk_plugin, "~> 0.18.13"},
  {:membrane_sdl_plugin, "~> 0.18.6"},
  {:membrane_portaudio_plugin, "~> 0.19.4"},
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true}
])

defmodule Example do
  use Membrane.Pipeline

  alias Membrane.Time

  @samples_url "https://raw.githubusercontent.com/membraneframework/static/gh-pages/samples/big-buck-bunny/"
  @audio_url @samples_url <> "bun33s.aac"
  @video_url @samples_url <> "bun33s_720x480.h264"

  def start_link() do
    Membrane.Pipeline.start_link(__MODULE__)
  end

  @impl true
  def handle_init(_ctx, _opts) do
    structure = [
      # Sources and parsers — shared by both branches
      child(:video_source, %Membrane.Hackney.Source{
        location: @video_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:video_parser, %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: {25, 1}}
      })
      |> child(:video_tee, Membrane.Tee),
      child(:audio_source, %Membrane.Hackney.Source{
        location: @audio_url,
        hackney_opts: [follow_redirect: true]
      })
      |> child(:audio_parser, Membrane.AAC.Parser)
      |> child(:audio_tee, Membrane.Tee),

      # --- MoQ branch (comment out to use playback branch only) ---
      get_child(:video_tee)
      |> via_out(Pad.ref(:output, :moq))
      |> child(:video_parser_moq, %Membrane.H264.Parser{output_stream_structure: :avc1})
      |> via_in(Pad.ref(:input, :video))
      |> get_child(:muxer),
      get_child(:audio_tee)
      |> via_out(Pad.ref(:output, :moq))
      |> child(:audio_parser_moq, %Membrane.AAC.Parser{
        out_encapsulation: :none,
        output_config: :esds
      })
      |> via_in(Pad.ref(:input, :audio))
      |> get_child(:muxer),
      child(:muxer, %Membrane.MP4.Muxer.CMAF{
        segment_min_duration: Time.seconds(2)
      })
      |> child(:sink, %Membrane.MoQ.Sink{
        url: "https://localhost:4443/anon",
        broadcast: "example",
        disable_tls_verify?: true
      }),

      # --- Playback branch (comment out to use MoQ branch only) ---
      get_child(:video_tee)
      |> via_out(Pad.ref(:output, :play))
      |> child(:video_decoder, Membrane.H264.FFmpeg.Decoder)
      |> child(:video_player, Membrane.SDL.Player),
      get_child(:audio_tee)
      |> via_out(Pad.ref(:output, :play))
      |> child(:audio_decoder, Membrane.AAC.FDK.Decoder)
      |> child(:audio_player, Membrane.PortAudio.Sink)
    ]

    {[spec: structure], %{}}
  end

  @impl true
  def handle_element_end_of_stream(sink, _pad, _ctx, state)
      when sink in [ :video_player, :audio_player ] do
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
