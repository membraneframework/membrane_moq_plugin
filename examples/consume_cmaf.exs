# NOTE: This example was LLM-generated.
#
# Consumes a CMAF (fragmented-MP4) broadcast published by an external tool,
# demonstrating that `Membrane.MoQ.Source` picks each rendition's wire
# container (legacy vs CMAF) from the catalog automatically — the same
# pipeline works unchanged for broadcasts published by `Membrane.MoQ.Sink`
# (legacy container) and by the moq CLI's fmp4 importer (CMAF).
#
# The track name is chosen by the publisher, so the pipeline wires its pad
# from the Source's `{:new_track, {track, stream_format}}` notification
# instead of hardcoding it.
#
# Prerequisites:
#   - A MoQ relay at https://localhost:4443 with anonymous auth (e.g. moq-relay
#     with the localhost.toml demo config)
#   - ffmpeg and the moq CLI (`cargo install moq-cli`)
#
# Publish a CMAF test pattern (note: the moq CLI cannot publish LOC, so that
# container stays uncovered by this route):
#
#   ffmpeg -hide_banner -loglevel error -re \
#     -f lavfi -i testsrc2=duration=30:size=640x360:rate=30 \
#     -pix_fmt yuv420p -c:v libx264 -preset ultrafast -tune zerolatency \
#     -x264-params keyint=30:min-keyint=30:scenecut=0 \
#     -f mp4 -movflags cmaf+frag_keyframe - \
#   | moq --client-connect http://localhost:4443 --broadcast demo.hang import fmp4
#
# (`http://` makes the moq CLI trust the relay's self-signed certificate by
# fetching its fingerprint from /certificate.sha256 first.)
#
# Then run:
#
#   elixir examples/consume_cmaf.exs demo.hang

broadcast =
  case System.argv() do
    [broadcast | _rest] ->
      broadcast

    [] ->
      IO.puts(:stderr, """
      Usage: elixir #{Path.relative_to_cwd(__ENV__.file)} <broadcast>

      <broadcast> is the name of the MoQ broadcast to subscribe to
      (the value passed to the publisher's --broadcast, e.g. demo.hang).
      """)

      System.halt(1)
  end

Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true}
])

Logger.configure(level: :info)

defmodule FrameLogger do
  @moduledoc false
  use Membrane.Sink

  require Logger

  def_input_pad :input, accepted_format: _any, flow_control: :auto

  def_options track: [spec: String.t()]

  @impl true
  def handle_init(_ctx, opts), do: {[], %{track: opts.track, count: 0}}

  @impl true
  def handle_stream_format(:input, fmt, _ctx, state) do
    Logger.info("[#{state.track}] stream format: #{inspect(fmt)}")
    {[], state}
  end

  @impl true
  def handle_buffer(:input, _buffer, _ctx, state) do
    count = state.count + 1
    if rem(count, 30) == 0, do: Logger.info("[#{state.track}] #{count} frames received")
    {[], %{state | count: count}}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    Logger.info("[#{state.track}] end of stream after #{state.count} frames")
    {[], state}
  end
end

defmodule Subscriber do
  @moduledoc false
  use Membrane.Pipeline

  require Logger
  require Membrane.Pad

  alias Membrane.Pad

  @impl true
  def handle_init(_ctx, opts) do
    spec =
      child(:source, %Membrane.MoQ.Source{
        url: opts[:url],
        broadcast: opts[:broadcast],
        disable_tls_verify?: true,
        latency: Membrane.Time.milliseconds(500)
      })

    {[spec: spec], %{subscribed: MapSet.new()}}
  end

  @impl true
  def handle_child_notification(
        {:new_track, {track, stream_format}},
        :source,
        _ctx,
        state
      ) do
    if MapSet.member?(state.subscribed, track) do
      {[], state}
    else
      Logger.info("announced #{track} (#{inspect(stream_format.__struct__)}), subscribing")

      spec =
        get_child(:source)
        |> via_out(Pad.ref(:output, track), options: [track: track])
        |> child({:logger, track}, %FrameLogger{track: track})

      {[spec: spec], %{state | subscribed: MapSet.put(state.subscribed, track)}}
    end
  end

  def handle_child_notification({:track_removed, name}, :source, _ctx, state) do
    Logger.info("withdrawn #{name}")
    {[], state}
  end

  def handle_child_notification({:disconnected, reason}, :source, _ctx, state) do
    Logger.info("broadcast closed: #{inspect(reason)}, terminating")
    {[terminate: :normal], state}
  end

  def handle_child_notification(_notification, _child, _ctx, state), do: {[], state}
end

url = System.get_env("URL", "https://localhost:4443")

{:ok, _supervisor, pipeline} =
  Membrane.Pipeline.start_link(Subscriber, url: url, broadcast: broadcast)

ref = Process.monitor(pipeline)

receive do
  {:DOWN, ^ref, :process, ^pipeline, _reason} -> :ok
end
