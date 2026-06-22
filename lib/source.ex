defmodule Membrane.MoQ.Source do
  @moduledoc """
  Membrane Source acting as a MoQ subscriber.

  Connects to a MoQ relay server and subscribes to tracks of a single
  broadcast over one shared QUIC connection. Each `:output` pad subscribes to
  one track, named via the pad's `track` option; add as many output pads as the
  tracks you want to receive. Received frames are emitted as `Membrane.Buffer`s
  on the pad with `pts` set to the frame's presentation timestamp and a
  `keyframe?` flag in `metadata`. When a track ends (or the session
  disconnects) the source sends an end-of-stream on the affected pad(s).

  Pads can be added or removed at any time during the pipeline lifecycle.
  """
  use Membrane.Source

  require Membrane.Logger

  alias Membrane.MoQ.Native

  def_output_pad :output,
    availability: :on_request,
    accepted_format: Membrane.RemoteStream,
    flow_control: :push,
    options: [
      track: [
        spec: String.t(),
        description:
          "Catalog rendition key within the broadcast to subscribe to on this pad, " <>
            "see `Track` at " <>
            "https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
      ]
    ]

  def_options url: [
                spec: String.t(),
                description:
                  "URL of the MoQ relay to connect to, e.g. `\"https://localhost:4443\"`."
              ],
              broadcast: [
                spec: String.t(),
                description:
                  "Broadcast path to subscribe to, see `Broadcast` at " <>
                    "https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
              ],
              disable_tls_verify?: [
                spec: boolean(),
                default: false,
                description:
                  "If `true`, the QUIC client skips TLS certificate verification. " <>
                    "Useful for self-signed local relays only."
              ],
              latency: [
                spec: Membrane.Time.t(),
                default: Membrane.Time.seconds(1),
                description:
                  "How long each track buffers received frames before emitting them, " <>
                    "trading end-to-end delay for resilience to network jitter and reordering."
              ]

  defmodule State do
    @moduledoc false

    @type pad_state :: %{
            track: String.t(),
            token: integer(),
            eos?: boolean()
          }

    @type t :: %__MODULE__{
            url: String.t(),
            broadcast: String.t(),
            disable_tls_verify?: boolean(),
            latency: Membrane.Time.t(),
            subscriber: Native.subscriber() | nil,
            next_token: integer(),
            pads: %{Membrane.Pad.ref() => pad_state()},
            tokens: %{integer() => Membrane.Pad.ref()},
            disconnect_pending?: boolean()
          }

    @enforce_keys [:url, :broadcast, :disable_tls_verify?, :latency]
    defstruct @enforce_keys ++
                [
                  subscriber: nil,
                  next_token: 0,
                  pads: %{},
                  tokens: %{},
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

    pad_state = %{track: track, token: token, eos?: false}

    state = %{
      state
      | next_token: token + 1,
        pads: Map.put(state.pads, pad, pad_state),
        tokens: Map.put(state.tokens, token, pad)
    }

    # A pad added after we're already playing must start streaming immediately;
    # one added earlier is started in `handle_playing`.
    if ctx.playback == :playing do
      start_pad(pad, state)
    else
      {[], state}
    end
  end

  @impl true
  def handle_playing(_ctx, state) do
    {actions, state} =
      Enum.reduce(state.pads, {[], state}, fn {pad, _}, {acc, state} ->
        {pad_actions, state} = start_pad(pad, state)
        {acc ++ pad_actions, state}
      end)

    # A disconnect can arrive while we're still in setup; in that case we
    # remembered it and emit EOS (after stream_format above) now that we can.
    if state.disconnect_pending? do
      {eos_actions, state} = eos_all(state)
      {actions ++ eos_actions, %{state | disconnect_pending?: false}}
    else
      {actions, state}
    end
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    case Map.pop(state.pads, pad) do
      {nil, _pads} ->
        {[], state}

      {%{token: token}, pads} ->
        Native.unsubscribe_track(state.subscriber, token)
        {[], %{state | pads: pads, tokens: Map.delete(state.tokens, token)}}
    end
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state), do: {[setup: :complete], state}

  def handle_info({:moq_frame, token, payload, timestamp_us, keyframe?}, _ctx, state)
      when is_integer(token) and is_binary(payload) and is_integer(timestamp_us) and
             is_boolean(keyframe?) do
    case pad_for_token(state, token) do
      {pad, %{eos?: false}} ->
        buffer = %Membrane.Buffer{
          payload: payload,
          pts: Membrane.Time.microseconds(timestamp_us),
          metadata: %{keyframe?: keyframe?}
        }

        {[buffer: {pad, buffer}], state}

      # Frame for a removed/ended track; drop it.
      _ ->
        {[], state}
    end
  end

  def handle_info({:moq_track_ended, token, reason}, _ctx, state) do
    Membrane.Logger.info("MoQ track ended: #{inspect(reason)}")

    case pad_for_token(state, token) do
      {pad, _pad_state} -> eos_pad(pad, state)
      nil -> {[], state}
    end
  end

  def handle_info({:moq_setup_failed, reason}, _ctx, _state) do
    raise "MoQ subscriber setup failed: #{inspect(reason)}"
  end

  def handle_info({:moq_disconnected, reason}, ctx, state) do
    Membrane.Logger.info("MoQ subscriber disconnected: #{inspect(reason)}")

    if ctx.playback == :playing do
      eos_all(state)
    else
      # EOS actions are only valid in :playing; defer until we get there.
      {[], %{state | disconnect_pending?: true}}
    end
  end

  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message: #{inspect(msg)}")
    {[], state}
  end

  # Sends the stream format and opens the track subscription for a pad. Frames
  # arrive asynchronously as `{:moq_frame, token, ...}` once the relay starts
  # serving the track.
  @spec start_pad(Membrane.Pad.ref(), State.t()) :: {[Membrane.Element.Action.t()], State.t()}
  defp start_pad(pad, state) do
    pad_state = Map.fetch!(state.pads, pad)
    :ok = Native.subscribe_track(state.subscriber, pad_state.track, pad_state.token)
    {[stream_format: {pad, %Membrane.RemoteStream{}}], state}
  end

  @spec eos_pad(Membrane.Pad.ref(), State.t()) :: {[Membrane.Element.Action.t()], State.t()}
  defp eos_pad(pad, state) do
    case Map.fetch(state.pads, pad) do
      {:ok, %{eos?: true}} ->
        {[], state}

      {:ok, pad_state} ->
        {[end_of_stream: pad], put_in(state.pads[pad], %{pad_state | eos?: true})}

      :error ->
        {[], state}
    end
  end

  @spec eos_all(State.t()) :: {[Membrane.Element.Action.t()], State.t()}
  defp eos_all(state) do
    Enum.reduce(state.pads, {[], state}, fn {pad, _}, {acc, state} ->
      {actions, state} = eos_pad(pad, state)
      {acc ++ actions, state}
    end)
  end

  @spec pad_for_token(State.t(), integer()) :: {Membrane.Pad.ref(), State.pad_state()} | nil
  defp pad_for_token(state, token) do
    with pad when not is_nil(pad) <- Map.get(state.tokens, token),
         {:ok, pad_state} <- Map.fetch(state.pads, pad) do
      {pad, pad_state}
    else
      _ -> nil
    end
  end
end
