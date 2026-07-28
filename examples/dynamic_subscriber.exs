# Reference for notification-driven pad wiring: follows a broadcast and plays
# whatever video it advertises in an SDL window.
#
#   * subscribes to the lowest-named advertised video track (audio is skipped),
#   * when that track is withdrawn (`:track_removed`), tears its playback
#     subtree down and subscribes to the next available one,
#
# NOTE: Might crash when run against a broadcast
# where a track modifies its format in-place.
#
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
            available_tracks: %{String.t() => Membrane.StreamFormat.t()},
            current_track: String.t() | nil,
            generation: non_neg_integer(),
            moq_disconnected?: boolean()
          }

    @enforce_keys [:url, :broadcast]
    defstruct @enforce_keys ++
                [:current_track, available_tracks: %{}, generation: 0, moq_disconnected?: false]
  end

  @impl true
  def handle_init(_ctx, opts) do
    state = %State{url: opts[:url], broadcast: opts[:broadcast]}

    source_spec =
      child(:source, %Membrane.MoQ.Source{
        url: state.url,
        broadcast: state.broadcast,
        disable_tls_verify?: true,
        latency: Membrane.Time.milliseconds(200)
      })

    {[spec: source_spec], state}
  end

  @impl true
  def handle_child_notification(
        {:new_track, {track, %module{} = stream_format}},
        :source,
        _ctx,
        %State{} = state
      ) do
    Membrane.Logger.info("announced #{track} (#{inspect(module)})")

    if module in [Membrane.H264, Membrane.H265] do
      put_in(state.available_tracks[track], stream_format) |> maybe_subscribe()
    else
      {[], state}
    end
  end

  @impl true
  def handle_child_notification(
        {:track_removed, name},
        :source,
        ctx,
        %State{} = state
      ) do
    Membrane.Logger.info("withdrawn #{name}")
    state = %{state | available_tracks: Map.delete(state.available_tracks, name)}

    if name == state.current_track do
      {actions, state} = maybe_subscribe(%{state | current_track: nil})
      processing_children = for {child, _spec} <- ctx.children, child != :source, do: child
      {[remove_children: processing_children] ++ actions, state}
    else
      {[], state}
    end
  end

  @impl true
  def handle_child_notification(
        {:disconnected, reason},
        :source,
        _ctx,
        %State{} = state
      ) do
    Membrane.Logger.warning("MoQ session disconnected, reason: #{inspect(reason)}")
    state = %{state | available_tracks: %{}, current_track: nil, moq_disconnected?: true}
    {[], state}
  end

  @impl true
  def handle_child_notification(_notification, _child, _ctx, state), do: {[], state}

  @impl true
  def handle_element_end_of_stream(
        {:player, _generation},
        _pad,
        _ctx,
        %State{moq_disconnected?: true} = state
      ),
      do: {[terminate: :normal], state}

  @impl true
  def handle_element_end_of_stream(_child, _pad, _ctx, state), do: {[], state}

  defp maybe_subscribe(%State{current_track: nil, available_tracks: available} = state)
       when map_size(available) > 0 do
    {name, stream_format} = Enum.min_by(available, fn {name, _format} -> name end)
    Membrane.Logger.info("subscribing to #{name}")

    {[spec: track_spec(name, state.generation, stream_format)],
     %{state | current_track: name, generation: state.generation + 1}}
  end

  defp maybe_subscribe(state), do: {[], state}

  # `MoQ.Source` emits frames in decode order carrying their presentation PTS
  # but no DTS, so the parser regenerates monotonic DTS/PTS from the bitstream
  # structure (using the catalog's framerate) — otherwise the decoder would
  # emit frames still in decode order and the playback would jitter. `:annexb`
  # is what the FFmpeg decoders accept.
  @spec track_spec(String.t(), non_neg_integer(), Membrane.StreamFormat.t()) ::
          Membrane.ChildrenSpec.t()
  defp track_spec(name, generation, %Membrane.H264{framerate: framerate}),
    do:
      get_child(:source)
      |> via_out(Pad.ref(:output, generation), options: [track: name])
      |> child({:parser, generation}, %Membrane.H264.Parser{
        generate_best_effort_timestamps: %{framerate: framerate || {30, 1}},
        output_stream_structure: :annexb
      })
      |> child({:decoder, generation}, Membrane.H264.FFmpeg.Decoder)
      |> child({:rt, generation}, Membrane.Realtimer)
      |> child({:player, generation}, Membrane.SDL.Player)

  defp track_spec(name, generation, %Membrane.H265{framerate: framerate}),
    do:
      get_child(:source)
      |> via_out(Pad.ref(:output, generation), options: [track: name])
      |> child({:parser, generation}, %Membrane.H265.Parser{
        generate_best_effort_timestamps: %{framerate: framerate || {30, 1}},
        output_stream_structure: :annexb
      })
      |> child({:decoder, generation}, Membrane.H265.FFmpeg.Decoder)
      |> child({:rt, generation}, Membrane.Realtimer)
      |> child({:player, generation}, Membrane.SDL.Player)
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

{:ok, _supervisor, subscriber_pid} = Membrane.Pipeline.start_link(Subscriber, opts)

io_pid =
  spawn(fn ->
    IO.gets("""
    Subscriber pipeline started, waiting for broadcast: #{inspect(broadcast)}.
    Press Enter to stop.
    """)
  end)

subscriber_ref = Process.monitor(subscriber_pid)
io_ref = Process.monitor(io_pid)

receive do
  {:DOWN, ref, :process, _pid, _reason} when ref in [subscriber_ref, io_ref] ->
    :ok
end
