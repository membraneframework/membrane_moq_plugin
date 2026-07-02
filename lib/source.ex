defmodule Membrane.MoQ.Source do
  @moduledoc """
  Membrane Source acting as a MoQ subscriber.

  Connects to a MoQ relay server and subscribes to tracks of a single broadcast over one shared QUIC connection.
  An output pad corresponds to one MoQ track.

  ## Parent notifications

  #{__MODULE__} watches the broadcast catalog and notifies its parent about track changes:

    * `{:new_track, Membrane.MoQ.Source.TrackInfo.t()}`
        when a track is advertised.
    * `{:track_removed, track :: String.t()}`
        when an advertised track disappears from the catalog (e.g. the publisher ended it).
    * A track whose codec parameters change mid-broadcast is reported as a
      `:track_removed` immediately followed by a `:new_track`, so a stale pad can be
      torn down and re-wired against the new format.
    * `{:disconnected, reason :: String.t()}`
        when the broadcast goes away (the publisher left) or the session drops.
        The source end-of-streams its pads and terminates.
  """
  use Membrane.Source

  require Membrane.Logger
  require Membrane.{H264, H265}

  alias Membrane.MoQ.{Native, TrackFormat}

  def_output_pad :output,
    availability: :on_request,
    accepted_format:
      any_of(
        Membrane.AAC,
        Membrane.Opus,
        %Membrane.H264{stream_structure: ss} when Membrane.H264.is_avc(ss),
        %Membrane.H265{stream_structure: ss} when Membrane.H265.is_hvc(ss),
        Membrane.RemoteStream
      ),
    flow_control: :push,
    options: [
      track: [
        spec: String.t(),
        description: """
        Catalog rendition key within the broadcast to subscribe to on this pad,
        see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology
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

  defmodule TrackInfo do
    @moduledoc """
    Describes a track advertised by the subscribed broadcast.

    Carried by the `{:new_track, t:t/0}` parent notification (see `Membrane.MoQ.Source`).
    """

    @type t :: %__MODULE__{
            track: String.t(),
            type: :video | :audio | :unknown,
            stream_format:
              Membrane.H264.t()
              | Membrane.H265.t()
              | Membrane.AAC.t()
              | Membrane.Opus.t()
              | Membrane.RemoteStream.t()
          }

    @enforce_keys [:track, :type, :stream_format]
    defstruct @enforce_keys
  end

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{
            url: String.t(),
            broadcast: String.t(),
            disable_tls_verify?: boolean(),
            latency: Membrane.Time.t(),
            subscriber: Native.subscriber() | nil,
            next_token: integer(),
            tokens: BiMap.t(),
            disconnect_pending?: boolean()
          }

    @enforce_keys [:url, :broadcast, :disable_tls_verify?, :latency]
    defstruct @enforce_keys ++
                [
                  :subscriber,
                  next_token: 0,
                  tokens: BiMap.new(),
                  disconnect_pending?: false
                ]
  end

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %State{
       url: opts.url,
       broadcast: opts.broadcast,
       disable_tls_verify?: opts.disable_tls_verify?,
       latency: opts.latency
     }}
  end

  @impl true
  def handle_setup(ctx, %State{} = state) do
    {:ok, subscriber} =
      Native.start_subscriber(
        state.url,
        state.broadcast,
        self(),
        state.disable_tls_verify?,
        Membrane.Time.as_nanoseconds(state.latency, :round)
      )

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.stop_subscriber(subscriber)
    end)

    {[setup: :incomplete], %{state | subscriber: subscriber}}
  end

  @impl true
  def handle_pad_added(pad, %{pad_options: %{track: track}} = ctx, state) do
    token = state.next_token

    state = %{
      state
      | next_token: token + 1,
        tokens: BiMap.put(state.tokens, token, pad)
    }

    if ctx.playback == :playing do
      start_pad(track, token, state)
    end

    {[], state}
  end

  @impl true
  def handle_playing(ctx, %{disconnect_pending?: true} = state) do
    {eos_all(ctx.pads), state}
  end

  def handle_playing(ctx, state) do
    Enum.each(state.tokens, fn {token, pad} ->
      start_pad(ctx.pads[pad].options.track, token, state)
    end)

    {[], state}
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    case BiMap.get_key(state.tokens, pad) do
      nil ->
        {[], state}

      token ->
        Native.unsubscribe_track(state.subscriber, token)
        {[], %{state | tokens: BiMap.delete_value(state.tokens, pad)}}
    end
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state), do: {[setup: :complete], state}

  def handle_info({:moq_track_added, name, format}, _ctx, state) do
    {[notify_parent: {:new_track, build_track_info(name, format)}], state}
  end

  def handle_info({:moq_track_removed, name}, _ctx, state) do
    {[notify_parent: {:track_removed, name}], state}
  end

  def handle_info({:moq_track_format, token, format}, ctx, state) do
    actions =
      case active_pad(ctx, state, token) do
        nil -> []
        pad -> [stream_format: {pad, TrackFormat.to_stream_format(format)}]
      end

    {actions, state}
  end

  def handle_info({:moq_frame, token, payload, timestamp_us, keyframe?}, ctx, state) do
    actions =
      case active_pad(ctx, state, token) do
        nil ->
          []

        pad ->
          buffer = %Membrane.Buffer{
            payload: payload,
            pts: Membrane.Time.microseconds(timestamp_us),
            metadata: %{keyframe?: keyframe?}
          }

          [buffer: {pad, buffer}]
      end

    {actions, state}
  end

  def handle_info({:moq_track_ended, token, reason}, ctx, state) do
    Membrane.Logger.debug("MoQ track ended: #{inspect(reason)}")

    actions =
      case active_pad(ctx, state, token) do
        nil -> []
        pad -> [end_of_stream: pad]
      end

    {actions, state}
  end

  def handle_info({:moq_setup_failed, reason}, _ctx, _state) do
    raise "MoQ subscriber setup failed: #{inspect(reason)}"
  end

  def handle_info({:moq_disconnected, reason}, ctx, state) do
    Membrane.Logger.debug("MoQ subscriber disconnected: #{inspect(reason)}")

    notify_disconnected = [notify_parent: {:disconnected, reason}]

    case ctx.playback do
      :playing -> {notify_disconnected ++ eos_all(ctx.pads), state}
      _other -> {notify_disconnected, %{state | disconnect_pending?: true}}
    end
  end

  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message: #{inspect(msg)}")
    {[], state}
  end

  @spec start_pad(String.t(), integer(), State.t()) :: :ok
  defp start_pad(track, token, state) do
    Native.subscribe_track(state.subscriber, track, token)
  end

  @spec eos_all(%{Membrane.Pad.ref() => map()}) :: [Membrane.Element.Action.t()]
  defp eos_all(pads) do
    for {pad, %{end_of_stream?: false}} <- pads do
      {:end_of_stream, pad}
    end
  end

  @spec build_track_info(String.t(), Native.track_format()) :: TrackInfo.t()
  defp build_track_info(name, format) do
    %TrackInfo{
      track: name,
      type: TrackFormat.media_type(format),
      stream_format: TrackFormat.to_stream_format(format)
    }
  end

  @spec active_pad(map(), State.t(), integer()) :: Membrane.Pad.ref() | nil
  defp active_pad(ctx, state, token) do
    with {:ok, pad} <- BiMap.fetch(state.tokens, token),
         %{end_of_stream?: false} <- ctx.pads[pad] do
      pad
    else
      _error -> nil
    end
  end
end
