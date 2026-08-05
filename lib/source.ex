defmodule Membrane.MoQ.Source do
  @moduledoc """
  Membrane Source acting as a MoQ subscriber.

  Connects to a MoQ relay server and subscribes to tracks of a single broadcast over one shared QUIC connection.
  An output pad corresponds to one MoQ track.

  ## Parent notifications

  #{inspect(__MODULE__)} watches the broadcast catalog and notifies its parent about track changes:

    * `{:new_track, {track :: ExMoQ.Native.track(), stream_format :: struct()}}`
        when a track is advertised. `track` is the catalog rendition key to pass
        as the `:track` option of an output pad; `stream_format` is the format
        the pad will start with.
    * `{:track_removed, track :: ExMoQ.Native.track()}`
        when an advertised track disappears from the catalog (e.g. the publisher ended it).
    * A track whose codec parameters change mid-broadcast is reported as a `:track_removed`,
        followed by a `:new_track`, so a stale pad can be torn down and re-wired against the new format.
    * `{:subscription_died, {track :: ExMoQ.Native.track(), reason :: String.t()}}`
        when the native subscription feeding a pad fails while the track may still be
        advertised in the catalog. The source sends `:end_of_stream` to that pad;
        re-linking a pad for the track starts a fresh subscription.
    * `{:disconnected, reason :: String.t() | ExMoQ.Native.close_reason()}`
        when the broadcast goes away or the session drops.
        The source sends `:end_of_stream` to all active pads.
  """
  use Membrane.Source

  require Membrane.Logger

  alias ExMoQ.Native
  alias Membrane.MoQ.Source.Catalog
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
        spec: ExMoQ.Native.track(),
        description: """
        Catalog rendition key within the broadcast to subscribe to on this pad,
        see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology
        """
      ],
      priority: [
        spec: 0..255 | nil,
        default: nil,
        description: """
        Delivery priority of this subscription.
        Under congestion, tracks with a higher value are sent first.
        When nil, hang defaults for the track's media kind are used.
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
                see `Broadcast` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology.
                A `.msf` suffix pulls track info from the MSF catalog,
                and the default fallback is hang.
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

    @type subscription :: {Membrane.Pad.ref(), Native.track()}

    @type t :: %__MODULE__{
            url: String.t(),
            broadcast: String.t(),
            disable_tls_verify?: boolean(),
            latency: Membrane.Time.t(),
            session: Native.session() | nil,
            consumer: Native.broadcast_consumer() | nil,
            next_token: Native.token(),
            # subscriptions waiting for playback to start
            # and the track to be announced by the catalog
            waiting: %{Native.token() => subscription()},
            # subscriptions for which a native task forwarding frames exists,
            # entries only move from `waiting` to `active`
            active: %{Native.token() => subscription()},
            catalog: Catalog.t(),
            status: :connecting | :ready | :disconnect_pending | :disconnected
          }

    @enforce_keys [:url, :broadcast, :disable_tls_verify?, :latency]
    defstruct @enforce_keys ++
                [
                  :session,
                  :consumer,
                  next_token: 0,
                  waiting: %{},
                  active: %{},
                  catalog: %Catalog{},
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
  def handle_pad_added(_pad, _ctx, %{status: :disconnected}) do
    raise "Cannot link pads to #{inspect(__MODULE__)} after session closed"
  end

  @impl true
  def handle_pad_added(pad, ctx, state) do
    track = ctx.pads[pad].options.track
    token = state.next_token

    state = %{
      state
      | next_token: token + 1,
        waiting: Map.put(state.waiting, token, {pad, track})
    }

    case ctx.playback do
      :playing -> subscribe_pad(token, pad, track, ctx, state)
      :stopped -> {[], state}
    end
  end

  @impl true
  def handle_playing(ctx, %{status: :disconnect_pending} = state),
    do: become_disconnected(ctx, state)

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
    {diff, catalog} = Catalog.update(state.catalog, renditions)

    track_notifs = track_notifications(diff, catalog)

    state = %{state | catalog: catalog}

    case ctx.playback do
      :stopped ->
        {track_notifs, state}

      :playing ->
        {eos_actions, state} = end_subscriptions(diff.changed, state)
        {subscribe_actions, state} = subscribe_ready(ctx, state)
        {track_notifs ++ eos_actions ++ subscribe_actions, state}
    end
  end

  @impl true
  def handle_info({:moq_frame, token, payload, timestamp_ns, keyframe?}, ctx, state) do
    case state.active[token] do
      nil ->
        {[], state}

      {pad, _track} ->
        buffer = %Membrane.Buffer{
          payload: payload,
          pts: Membrane.Time.nanoseconds(timestamp_ns),
          metadata: TrackFormat.buffer_metadata(keyframe?, ctx.pads[pad].stream_format)
        }

        {[buffer: {pad, buffer}], state}
    end
  end

  @impl true
  def handle_info({:moq_track_finished, token}, _ctx, state) do
    case Map.pop(state.active, token) do
      {nil, _active} ->
        {[], state}

      {{pad, _track}, active} ->
        {[end_of_stream: pad], %{state | active: active}}
    end
  end

  @impl true
  def handle_info({:moq_track_error, token, reason}, _ctx, state) do
    case Map.pop(state.active, token) do
      {nil, _active} ->
        {[], state}

      {{pad, track}, active} ->
        Membrane.Logger.warning("""
        MoQ subscription for track #{inspect(track)} failed, sending EOS.
        reason: #{inspect(reason)}
        """)

        {[
           notify_parent: {:subscription_died, {track, reason}},
           end_of_stream: pad
         ], %{state | active: active}}
    end
  end

  @impl true
  def handle_info({:moq_setup_failed, reason}, _ctx, _state) do
    raise "MoQ subscriber setup failed: #{inspect(reason)}"
  end

  @impl true
  def handle_info({:moq_disconnected, reason}, ctx, state),
    do: handle_closed(reason, ctx, state)

  @impl true
  def handle_info({:moq_broadcast_closed, _path, reason}, ctx, state),
    do: handle_closed(reason, ctx, state)

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_pad_removed(pad, ctx, state) do
    track = ctx.pads[pad].options.track
    same_sub? = fn {_token, sub} -> sub == {pad, track} end

    case {Enum.find(state.active, same_sub?), Enum.find(state.waiting, same_sub?)} do
      {{token, _sub}, nil} ->
        Native.unsubscribe_track(state.consumer, token)
        {[], %{state | active: Map.delete(state.active, token)}}

      {nil, {token, _sub}} ->
        {[], %{state | waiting: Map.delete(state.waiting, token)}}

      {nil, nil} ->
        {[], state}
    end
  end

  @spec handle_closed(
          String.t() | Native.close_reason(),
          Membrane.Element.CallbackContext.t(),
          State.t()
        ) :: {[Membrane.Element.Action.t()], State.t()}
  defp handle_closed(reason, _ctx, %{status: :connecting}) do
    raise "MoQ subscriber setup failed: #{inspect(reason)}"
  end

  defp handle_closed(_reason, _ctx, %{status: status} = state)
       when status in [:disconnect_pending, :disconnected],
       do: {[], state}

  defp handle_closed(reason, ctx, %{status: :ready} = state) do
    Membrane.Logger.debug("MoQ subscriber disconnected: #{inspect(reason)}")

    notify_disconnected = [notify_parent: {:disconnected, reason}]

    case ctx.playback do
      :playing ->
        {actions, state} = become_disconnected(ctx, state)
        {notify_disconnected ++ actions, state}

      :stopped ->
        {notify_disconnected, %{state | status: :disconnect_pending}}
    end
  end

  @spec track_notifications(Catalog.diff(), Catalog.t()) :: [
          Membrane.Element.Action.notify_parent()
        ]
  defp track_notifications(diff, catalog) do
    tracks_removed =
      Stream.concat(diff.removed, diff.changed)
      |> Stream.map(fn name -> {:notify_parent, {:track_removed, name}} end)

    new_tracks =
      Stream.concat(diff.changed, diff.added)
      |> Stream.map(fn name ->
        {format, _container} = Catalog.rendition(catalog, name)
        {:notify_parent, {:new_track, {name, TrackFormat.to_stream_format(format)}}}
      end)

    Enum.concat(tracks_removed, new_tracks)
  end

  @spec end_subscriptions([Native.track()], State.t()) ::
          {[Membrane.Element.Action.t()], State.t()}
  defp end_subscriptions(changed_tracks, state) do
    {ended, active} =
      Map.split_with(state.active, fn {_token, {_pad, track}} -> track in changed_tracks end)

    eos_actions =
      Enum.map(ended, fn {token, {pad, _track}} ->
        Native.unsubscribe_track(state.consumer, token)
        {:end_of_stream, pad}
      end)

    {eos_actions, %{state | active: active}}
  end

  @spec subscribe_ready(Membrane.Element.CallbackContext.t(), State.t()) ::
          {[Membrane.Element.Action.t()], State.t()}
  defp subscribe_ready(ctx, state) do
    Enum.flat_map_reduce(state.waiting, state, fn {token, {pad, track}}, state ->
      subscribe_pad(token, pad, track, ctx, state)
    end)
  end

  @spec subscribe_pad(
          integer(),
          Membrane.Pad.ref(),
          Native.track(),
          Membrane.Element.CallbackContext.t(),
          State.t()
        ) ::
          {[Membrane.Element.Action.t()], State.t()}
  defp subscribe_pad(token, pad, track, ctx, state) do
    priority = ctx.pads[pad].options.priority

    with {format, container} <- Catalog.rendition(state.catalog, track),
         priority = priority || TrackFormat.default_priority(format),
         :ok <- Native.subscribe_track(state.consumer, track, container, token, priority) do
      state = %{
        state
        | waiting: Map.delete(state.waiting, token),
          active: Map.put(state.active, token, {pad, track})
      }

      {[stream_format: {pad, TrackFormat.to_stream_format(format)}], state}
    else
      nil ->
        # track not advertised yet; parked until a catalog update resolves it
        {[], state}

      {:error, reason} ->
        Membrane.Logger.warning("""
        Cannot subscribe to track #{inspect(track)}, sending EOS.
        reason: #{inspect(reason)}
        """)

        {[end_of_stream: pad], %{state | waiting: Map.delete(state.waiting, token)}}
    end
  end

  @spec become_disconnected(Membrane.Element.CallbackContext.t(), State.t()) ::
          {[Membrane.Element.Action.t()], State.t()}
  defp become_disconnected(ctx, %State{} = state) do
    actions = for {pad, %{end_of_stream?: false}} <- ctx.pads, do: {:end_of_stream, pad}
    {actions, %{state | status: :disconnected, waiting: %{}, active: %{}, catalog: %Catalog{}}}
  end
end
