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

Mix.install([
  {:membrane_moq_plugin, path: __DIR__ |> Path.join("..") |> Path.expand(), override: true},
  {:membrane_h26x_plugin, "~> 0.10.7"}
])

Logger.configure(level: :debug)

defmodule AuLogger do
  use Membrane.Sink

  require Logger

  def_input_pad :input,
    accepted_format: any_of(Membrane.H264, Membrane.H265),
    flow_control: :auto

  def_options track: [spec: String.t()]

  @impl true
  def handle_init(_ctx, opts), do: {[], %{track: opts.track, count: 0}}

  @impl true
  def handle_buffer(:input, _buffer, _ctx, state) do
    count = state.count + 1
    if rem(count, 30) == 0, do: Logger.info("[#{state.track}] #{count} access units decoded")
    {[], %{state | count: count}}
  end

  @impl true
  def handle_end_of_stream(:input, _ctx, state) do
    Logger.info("[#{state.track}] end of stream (#{state.count} access units)")
    {[], state}
  end
end

defmodule Subscriber do
  use Membrane.Pipeline

  require Logger
  require Membrane.Pad

  alias Membrane.Pad

  def start_link(opts), do: Membrane.Pipeline.start_link(__MODULE__, opts)

  @impl true
  def handle_init(_ctx, opts) do
    state = %{url: opts[:url], broadcast: opts[:broadcast], gen: 0, available: %{}, current: nil}
    {[spec: source_spec(state)], state}
  end

  @impl true
  def handle_child_notification({:new_track, info}, {:source, gen}, _ctx, %{gen: gen} = state) do
    Logger.info("announced #{info.track} (#{info.type})")

    if info.type == :video do
      maybe_subscribe(put_in(state.available[info.track], info))
    else
      {[], state}
    end
  end

  def handle_child_notification({:track_removed, name}, {:source, gen}, _ctx, %{gen: gen} = state) do
    Logger.info("withdrawn #{name}")
    state = %{state | available: Map.delete(state.available, name)}

    if name == state.current do
      {actions, state} = maybe_subscribe(%{state | current: nil})
      {[remove_children: subtree(name)] ++ actions, state}
    else
      {[], state}
    end
  end

  def handle_child_notification(
        {:disconnected, reason},
        {:source, gen},
        _ctx,
        %{gen: gen} = state
      ) do
    Logger.info("broadcast gone (#{inspect(reason)}); restarting source to resubscribe")

    teardown =
      case state.current do
        nil -> [{:source, gen}]
        name -> [{:source, gen} | subtree(name)]
      end

    state = %{state | gen: gen + 1, available: %{}, current: nil}
    {[remove_children: teardown, spec: source_spec(state)], state}
  end

  def handle_child_notification(_notification, _child, _ctx, state), do: {[], state}

  defp source_spec(%{gen: gen} = state) do
    child({:source, gen}, %Membrane.MoQ.Source{
      url: state.url,
      broadcast: state.broadcast,
      disable_tls_verify?: true,
      latency: Membrane.Time.milliseconds(200)
    })
  end

  # Subscribe to the lowest-named advertised video track when idle.
  defp maybe_subscribe(%{current: nil, available: available} = state)
       when map_size(available) > 0 do
    info = available |> Map.values() |> Enum.min_by(& &1.track)
    Logger.info("subscribing to #{info.track}")
    {[spec: track_spec(info, state.gen)], %{state | current: info.track}}
  end

  defp maybe_subscribe(state), do: {[], state}

  defp track_spec(info, gen) do
    name = info.track

    get_child({:source, gen})
    |> via_out(Pad.ref(:output, name), options: [track: name])
    |> child({:parser, name}, parser_for(info.stream_format))
    |> child({:logger, name}, %AuLogger{track: name})
  end

  defp subtree(name), do: [{:parser, name}, {:logger, name}]

  defp parser_for(%Membrane.H264{}), do: Membrane.H264.Parser
  defp parser_for(%Membrane.H265{}), do: Membrane.H265.Parser
end

opts = [url: "https://localhost:4443/anon", broadcast: broadcast]

{:ok, _supervisor, subscriber} = Subscriber.start_link(opts)

# Follow the broadcast until a key is pressed, then shut the pipeline down
# gracefully so every element's terminate path runs.
IO.gets("\nFollowing broadcast — press Enter to stop the pipeline.\n")
Membrane.Pipeline.terminate(subscriber)
