# Reference for notification-driven pad wiring: follows a broadcast and plays
# whatever video it advertises in an SDL window.
#
#   * subscribes to the lowest-named advertised video track (audio is skipped),
#   * when that track is withdrawn (`:track_removed`), tears its playback
#     subtree down and subscribes to the next available one,
#   * when the broadcast drops (`:disconnected`), restarts the Source and waits
#     for a republish.
#
# Prerequisites:
#   - a MoQ relay at https://localhost:4443 (e.g. moq-relay)
#   - ffmpeg + SDL available for the decoder/player plugins

Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand()},
  {:membrane_h26x_plugin, "~> 0.10.7"},
  {:membrane_h264_ffmpeg_plugin, "~> 0.32.6"},
  {:membrane_h265_ffmpeg_plugin, "~> 0.4.3"},
  {:membrane_sdl_plugin, "~> 0.18.6"},
  {:membrane_realtimer_plugin, "~> 0.11.0"}
])

Logger.configure(level: :info)

defmodule Subscriber do
  use Membrane.Pipeline

  require Membrane.Logger
  require Membrane.Pad

  alias Membrane.Pad

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{
            url: String.t(),
            broadcast: String.t(),
            generation: non_neg_integer(),
            available_tracks: %{String.t() => Membrane.StreamFormat.t()},
            current_track: String.t() | nil
          }

    @enforce_keys [:url, :broadcast]
    defstruct @enforce_keys ++ [:current_track, generation: 0, available_tracks: %{}]
  end

  @impl true
  def handle_init(_ctx, opts) do
    state = %State{url: opts[:url], broadcast: opts[:broadcast]}
    {[spec: source_spec(state)], state}
  end

  @impl true
  def handle_child_notification(
        {:new_track, {track, %module{} = stream_format}},
        {:source, gen},
        _ctx,
        %State{generation: gen} = state
      ) do
    Membrane.Logger.info("announced #{track} (#{inspect(module)})")

    if module in [Membrane.H264, Membrane.H265] do
      maybe_subscribe(put_in(state.available_tracks[track], stream_format))
    else
      {[], state}
    end
  end

  @impl true
  def handle_child_notification(
        {:track_removed, name},
        {:source, gen},
        _ctx,
        %State{generation: gen} = state
      ) do
    Membrane.Logger.info("withdrawn #{name}")
    state = %{state | available_tracks: Map.delete(state.available_tracks, name)}

    if name == state.current_track do
      {actions, state} = maybe_subscribe(%{state | current_track: nil})
      {[remove_children: subtree(name)] ++ actions, state}
    else
      {[], state}
    end
  end

  @impl true
  def handle_child_notification(
        {:disconnected, reason},
        {:source, gen},
        _ctx,
        %State{generation: gen} = state
      ) do
    Membrane.Logger.info("broadcast gone (#{inspect(reason)}); restarting source to resubscribe")

    teardown =
      case state.current_track do
        nil -> [{:source, gen}]
        name -> [{:source, gen} | subtree(name)]
      end

    state = %{state | generation: gen + 1, available_tracks: %{}, current_track: nil}
    {[remove_children: teardown, spec: source_spec(state)], state}
  end

  @impl true
  def handle_child_notification(_notification, _child, _ctx, state), do: {[], state}

  defp source_spec(%State{generation: gen} = state) do
    child({:source, gen}, %Membrane.MoQ.Source{
      url: state.url,
      broadcast: state.broadcast,
      disable_tls_verify?: true,
      latency: Membrane.Time.milliseconds(200)
    })
  end

  # Subscribe to the lowest-named advertised video track when idle.
  defp maybe_subscribe(%State{current_track: nil, available_tracks: available} = state)
       when map_size(available) > 0 do
    {name, stream_format} = Enum.min_by(available, fn {name, _format} -> name end)
    Membrane.Logger.info("subscribing to #{name}")
    {[spec: track_spec(name, stream_format, state.generation)], %{state | current_track: name}}
  end

  defp maybe_subscribe(state), do: {[], state}

  defp track_spec(name, stream_format, gen) do
    {parser, decoder} = playback_for(stream_format)

    get_child({:source, gen})
    |> via_out(Pad.ref(:output, name), options: [track: name])
    |> child({:parser, name}, parser)
    |> child({:decoder, name}, decoder)
    |> child({:rt, name}, Membrane.Realtimer)
    |> child({:player, name}, Membrane.SDL.Player)
  end

  defp subtree(name),
    do: [{:parser, name}, {:decoder, name}, {:rt, name}, {:player, name}]

  # `MoQ.Source` emits frames in decode order carrying their presentation PTS
  # but no DTS, so the parser regenerates monotonic DTS/PTS from the bitstream
  # structure (using the catalog's framerate) — otherwise the decoder would
  # emit frames still in decode order and the playback would jitter. `:annexb`
  # is what the FFmpeg decoders accept.
  defp playback_for(%Membrane.H264{} = format) do
    {%Membrane.H264.Parser{
       generate_best_effort_timestamps: %{framerate: format.framerate || {30, 1}},
       output_stream_structure: :annexb
     }, Membrane.H264.FFmpeg.Decoder}
  end

  defp playback_for(%Membrane.H265{} = format) do
    {%Membrane.H265.Parser{
       generate_best_effort_timestamps: %{framerate: format.framerate || {30, 1}},
       output_stream_structure: :annexb
     }, Membrane.H265.FFmpeg.Decoder}
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

      <broadcast> is the name of the MoQ broadcast to subscribe to; it must end
      with .hang or .msf (e.g. format_change.hang).
      """)

      System.halt(1)
  end

opts = [url: "https://localhost:4443/anon", broadcast: broadcast]

{:ok, _supervisor, subscriber} = Membrane.Pipeline.start_link(Subscriber, opts)

# Follow the broadcast until a key is pressed, then shut the pipeline down
# gracefully so every element's terminate path runs.
IO.gets("\nFollowing broadcast — press Enter to stop the pipeline.\n")
Membrane.Pipeline.terminate(subscriber)
