defmodule Membrane.MoQ.Source do
  @moduledoc """
  Membrane Source acting as a MoQ subscriber.

  Connects to a MoQ relay server and subscribes to one rendition of one
  broadcast. Each received frame is emitted as a `Membrane.Buffer` on the
  `:output` pad with `pts` set to the frame's presentation timestamp and a
  `keyframe?` flag in `metadata`. When the relay closes the subscription
  the source sends an end-of-stream.
  """
  use Membrane.Source

  require Membrane.Logger

  alias Membrane.MoQ.Native

  def_output_pad :output, accepted_format: Membrane.RemoteStream, flow_control: :push

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
              track: [
                spec: String.t(),
                default: "data",
                description:
                  "Catalog rendition key within the broadcast, see `Track` at " <>
                    "https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
              ],
              disable_tls_verify?: [
                spec: boolean(),
                default: false,
                description:
                  "If `true`, the QUIC client skips TLS certificate verification. " <>
                    "Useful for self-signed local relays only."
              ]

  defmodule State do
    @moduledoc false
    @type t :: %__MODULE__{
            url: String.t(),
            broadcast: String.t(),
            track: String.t(),
            disable_tls_verify?: boolean(),
            subscriber: reference() | nil,
            disconnect_pending?: boolean()
          }

    @enforce_keys [:url, :broadcast, :track, :disable_tls_verify?]
    defstruct @enforce_keys ++ [subscriber: nil, disconnect_pending?: false]
  end

  @impl true
  def handle_init(_ctx, opts) do
    {[], struct(State, Map.from_struct(opts))}
  end

  @impl true
  def handle_setup(ctx, %State{} = state) do
    {:ok, subscriber} =
      Native.start_subscriber(
        state.url,
        state.broadcast,
        state.track,
        self(),
        state.disable_tls_verify?
      )

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.stop_subscriber(subscriber)
    end)

    {[setup: :incomplete], %{state | subscriber: subscriber}}
  end

  @impl true
  def handle_playing(_ctx, state) do
    actions = [stream_format: {:output, %Membrane.RemoteStream{}}]
    # A disconnect can arrive while we're still in setup; in that case we
    # remembered it and emit EOS as soon as we transition to playing.
    actions = if state.disconnect_pending?, do: actions ++ [end_of_stream: :output], else: actions
    {actions, %{state | disconnect_pending?: false}}
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state), do: {[setup: :complete], state}

  def handle_info({:moq_setup_failed, reason}, _ctx, _state) do
    raise "MoQ subscriber setup failed: #{inspect(reason)}"
  end

  def handle_info({:moq_frame, payload, timestamp_us, keyframe?}, _ctx, state)
      when is_binary(payload) and is_integer(timestamp_us) and is_boolean(keyframe?) do
    buffer = %Membrane.Buffer{
      payload: payload,
      pts: Membrane.Time.microseconds(timestamp_us),
      metadata: %{keyframe?: keyframe?}
    }

    {[buffer: {:output, buffer}], state}
  end

  def handle_info({:moq_disconnected, reason}, ctx, state) do
    Membrane.Logger.info("MoQ subscriber disconnected: #{inspect(reason)}")

    cond do
      ctx.playback != :playing ->
        # EOS actions are only valid in :playing — defer until we get there.
        {[], %{state | disconnect_pending?: true}}

      ctx.pads.output.end_of_stream? ->
        {[], state}

      true ->
        {[end_of_stream: :output], state}
    end
  end

  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message: #{inspect(msg)}")
    {[], state}
  end
end
