defmodule Membrane.MoQ.Source do
  @moduledoc """
  Membrane Source acting as a MoQ subscriber.

  Connects to a MoQ relay server and subscribes to tracks of a single broadcast over one shared QUIC connection.
  An output pad corresponds to one MoQ track.

  ## Parent notifications

  #{__MODULE__} watches the broadcast catalog and notifies its parent about track changes:

    * `{:new_track, {track :: String.t(), stream_format :: struct()}}`
        when a track is advertised. `track` is the catalog rendition key to pass
        as the `:track` option of an output pad; `stream_format` is the format
        the pad will start with.
    * `{:track_removed, track :: String.t()}`
        when an advertised track disappears from the catalog (e.g. the publisher ended it).
    * A track whose codec parameters change mid-broadcast is reported as a `:track_removed`,
        followed by a `:new_track`, so a stale pad can be torn down and re-wired against the new format.
    * `{:disconnected, reason :: String.t()}`
        when the broadcast goes away (the publisher left) or the session drops.
        The source sends `:end_of_stream` to all active pads.
  """
  use Membrane.Source

  require Membrane.Logger

  alias ExMoQ.Native
  alias Membrane.MoQ.Source.Tracks
  alias Membrane.MoQ.TrackFormat

  def_output_pad :output,
    availability: :on_request,
    accepted_format:
      any_of(
        Membrane.AAC,
        Membrane.Opus,
        Membrane.H264,
        Membrane.H265,
        %Membrane.RemoteStream{type: :packetized}
      ),
    flow_control: :push,
    options: [
      track: [
        spec: String.t(),
        description: """
        Catalog rendition key within the broadcast to subscribe to on this pad,
        see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology
        """
      ],
      priority: [
        spec: 0..255,
        default: 0,
        description: """
        Delivery priority of this subscription.
        Under congestion, tracks with a higher value are sent first.
        Recommended values are `80` for audio, `60` for video
        """
      ]
    ]

  def_options url: [
                spec: String.t(),
                description:
                  "URL of the MoQ relay to connect to, e.g. `\"https://localhost:4443\"`."
              ],
              broadcast: [
                spec: String.t(),
                description: """
                Broadcast path to subscribe to,
                see `Broadcast` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology
                """
              ],
              disable_tls_verify?: [
                spec: boolean(),
                default: false,
                description: """
                If `true`, the QUIC client skips TLS certificate verification.
                Useful for self-signed local relays only.
                """
              ],
              latency: [
                spec: Membrane.Time.t(),
                default: Membrane.Time.seconds(1),
                description: """
                How long each track buffers received frames before emitting them,
                trading end-to-end delay for resilience to network jitter and reordering.
                """
              ]

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{
            url: String.t(),
            broadcast: String.t(),
            disable_tls_verify?: boolean(),
            latency: Membrane.Time.t(),
            session: Native.session() | nil,
            consumer: Native.broadcast_consumer() | nil,
            tracks: Tracks.t(),
            status: :connecting | :ready | :disconnect_pending | :disconnected
          }

    @enforce_keys [:url, :broadcast, :disable_tls_verify?, :latency]
    defstruct @enforce_keys ++
                [
                  :session,
                  :consumer,
                  tracks: %Tracks{},
                  status: :connecting
                ]
  end

  @impl true
  def handle_init(_ctx, opts),
    do:
      {[],
       %State{
         url: opts.url,
         broadcast: opts.broadcast,
         disable_tls_verify?: opts.disable_tls_verify?,
         latency: opts.latency
       }}

  @impl true
  def handle_setup(ctx, %State{} = state) do
    {:ok, session} = Native.create_session(state.url, self(), state.disable_tls_verify?)

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.close_session(session)
    end)

    {[setup: :incomplete], %{state | session: session}}
  end

  @impl true
  def handle_pad_added(pad, _ctx, %{status: :disconnected} = state) do
    Membrane.Logger.warning(
      "Pad #{inspect(pad)} added after the source disconnected, sending end_of_stream"
    )

    {[end_of_stream: pad], state}
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    {token, tracks} = Tracks.add_pad(state.tracks, pad)
    state = %{state | tracks: tracks}

    case ctx.playback do
      :playing -> subscribe_pad(token, pad, ctx, state)
      :stopped -> {[], state}
    end
  end

  @impl true
  def handle_playing(ctx, %{status: :disconnect_pending} = state),
    do: {eos_all(ctx.pads), %{state | status: :disconnected}}

  @impl true
  def handle_playing(ctx, state), do: subscribe_ready(ctx, state)

  @impl true
  def handle_info(:moq_connected, ctx, state) do
    {:ok, consumer} =
      Native.create_broadcast_consumer(
        state.session,
        state.broadcast,
        self(),
        Membrane.Time.as_nanoseconds(state.latency, :round)
      )

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.close_broadcast_consumer(consumer)
    end)

    {[], %{state | consumer: consumer}}
  end

  @impl true
  def handle_info({:moq_broadcast_ready, _path}, _ctx, state),
    do: {[setup: :complete], %{state | status: :ready}}

  @impl true
  def handle_info({:moq_catalog, _path, renditions}, ctx, state) do
    {diff, tracks} =
      Tracks.apply_snapshot(state.tracks, renditions, fn pad -> ctx.pads[pad].options.track end)

    track_removed =
      Stream.concat(diff.removed, diff.changed)
      |> Stream.map(fn name -> {:notify_parent, {:track_removed, name}} end)

    new_track =
      Stream.concat(diff.changed, diff.added)
      |> Stream.map(fn name ->
        {format, _container} = Tracks.rendition(tracks, name)
        {:notify_parent, {:new_track, {name, TrackFormat.to_stream_format(format)}}}
      end)

    notifications = Enum.concat(track_removed, new_track)

    state = %{state | tracks: tracks}

    case ctx.playback do
      :stopped ->
        {notifications, state}

      :playing ->
        eos_actions = end_subscriptions(diff.ended, ctx, state)
        {subscribe_actions, state} = subscribe_ready(ctx, state)
        {notifications ++ eos_actions ++ subscribe_actions, state}
    end
  end

  @impl true
  def handle_info({:moq_frame, token, payload, timestamp_ns, keyframe?}, ctx, state) do
    actions =
      case active_pad(ctx, state, token) do
        nil ->
          []

        pad ->
          buffer = %Membrane.Buffer{
            payload: payload,
            pts: Membrane.Time.nanoseconds(timestamp_ns),
            metadata: TrackFormat.buffer_metadata(keyframe?, ctx.pads[pad].stream_format)
          }

          [buffer: {pad, buffer}]
      end

    {actions, state}
  end

  @impl true
  def handle_info({:moq_track_ended, token}, ctx, state) do
    state = %{state | tracks: Tracks.deactivate(state.tracks, token)}

    actions =
      case active_pad(ctx, state, token) do
        nil -> []
        pad -> [end_of_stream: pad]
      end

    {actions, state}
  end

  @impl true
  def handle_info({:moq_track_error, token, reason}, ctx, state) do
    # NOTE: should we bubble this up as a parent notif?
    state = %{state | tracks: Tracks.deactivate(state.tracks, token)}

    case active_pad(ctx, state, token) do
      nil ->
        {[], state}

      pad ->
        track = ctx.pads[pad].options.track

        Membrane.Logger.warning("""
        MoQ subscription for track #{inspect(track)} failed, sending EOS.
        reason: #{inspect(reason)}
        """)

        {[end_of_stream: pad], state}
    end
  end

  @impl true
  def handle_info({:moq_setup_failed, reason}, _ctx, _state),
    do: raise("MoQ subscriber setup failed: #{inspect(reason)}")

  @impl true
  def handle_info({:moq_disconnected, reason}, ctx, state), do: handle_closed(reason, ctx, state)

  @impl true
  def handle_info({:moq_broadcast_closed, _path, reason}, ctx, state),
    do: handle_closed(reason, ctx, state)

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    case Tracks.remove_pad(state.tracks, pad) do
      {nil, _tracks} ->
        {[], state}

      {token, tracks} ->
        if state.consumer != nil do
          Native.unsubscribe_track(state.consumer, token)
        end

        {[], %{state | tracks: tracks}}
    end
  end

  @spec handle_closed(String.t(), map(), State.t()) ::
          {[Membrane.Element.Action.t()], State.t()}
  defp handle_closed(reason, _ctx, %{status: :connecting}),
    do: raise("MoQ subscriber setup failed: #{inspect(reason)}")

  defp handle_closed(_reason, _ctx, %{status: status} = state)
       when status in [:disconnect_pending, :disconnected],
       do: {[], state}

  defp handle_closed(reason, ctx, %{status: :ready} = state) do
    Membrane.Logger.debug("MoQ subscriber disconnected: #{inspect(reason)}")

    notify_disconnected = [notify_parent: {:disconnected, reason}]

    case ctx.playback do
      :playing -> {notify_disconnected ++ eos_all(ctx.pads), %{state | status: :disconnected}}
      :stopped -> {notify_disconnected, %{state | status: :disconnect_pending}}
    end
  end

  @spec end_subscriptions([{Tracks.token(), Membrane.Pad.ref()}], map(), State.t()) ::
          [Membrane.Element.Action.t()]
  defp end_subscriptions(ended, ctx, state) do
    Enum.each(ended, fn {token, _pad} -> Native.unsubscribe_track(state.consumer, token) end)

    for {_token, pad} <- ended, not ctx.pads[pad].end_of_stream?, do: {:end_of_stream, pad}
  end

  # Subscribes every waiting pad whose track the catalog advertises.
  # Pads whose track is absent stay parked until a later catalog update.
  @spec subscribe_ready(map(), State.t()) :: {[Membrane.Element.Action.t()], State.t()}
  defp subscribe_ready(ctx, state),
    do:
      Enum.flat_map_reduce(Tracks.waiting(state.tracks), state, fn {token, pad}, state ->
        subscribe_pad(token, pad, ctx, state)
      end)

  @spec subscribe_pad(Tracks.token(), Membrane.Pad.ref(), map(), State.t()) ::
          {[Membrane.Element.Action.t()], State.t()}
  defp subscribe_pad(token, pad, ctx, state) do
    %{options: %{track: track, priority: priority}, end_of_stream?: eos?} = ctx.pads[pad]

    with false <- eos?,
         {format, container} <- Tracks.rendition(state.tracks, track),
         :ok <- Native.subscribe_track(state.consumer, track, container, token, priority) do
      {[stream_format: {pad, TrackFormat.to_stream_format(format)}],
       %{state | tracks: Tracks.activate(state.tracks, token)}}
    else
      true ->
        {[], state}

      nil ->
        # track not advertised yet; parked until a catalog update resolves it
        {[], state}

      {:error, reason} ->
        Membrane.Logger.warning("""
        Cannot subscribe to track #{inspect(track)}, sending EOS.
        reason: #{inspect(reason)}
        """)

        {[end_of_stream: pad], state}
    end
  end

  @spec eos_all(%{Membrane.Pad.ref() => map()}) :: [Membrane.Element.Action.t()]
  defp eos_all(pads) do
    for {pad, %{end_of_stream?: false}} <- pads, do: {:end_of_stream, pad}
  end

  @spec active_pad(map(), State.t(), Tracks.token()) :: Membrane.Pad.ref() | nil
  defp active_pad(ctx, state, token) do
    with pad when pad != nil <- Tracks.pad_for(state.tracks, token),
         %{end_of_stream?: false} <- ctx.pads[pad] do
      pad
    else
      _absent_or_ended -> nil
    end
  end
end
