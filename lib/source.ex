defmodule Membrane.MoQ.Source do
  @moduledoc """
  Membrane Source acting as a MoQ subscriber.

  Connects to a MoQ relay server and subscribes to the specified broadcast
  and track. Received frames are emitted as `Membrane.Buffer` payloads on
  the `:output` pad. When the relay closes the subscription, an
  end-of-stream action is sent.
  """
  use Membrane.Source
  require Membrane.Logger
  alias Membrane.MoQ.Native

  def_output_pad :output, accepted_format: Membrane.RemoteStream, flow_control: :push

  def_options url: [
                spec: String.t(),
                description: """
                URL of the MoQ relay server to connect to (e.g. "https://relay.example.com:4443").
                """
              ],
              broadcast: [
                spec: String.t(),
                description: """
                Name of the broadcast to subscribe to.
                """
              ],
              track: [
                spec: String.t(),
                default: "data",
                description: """
                Name of the track within the broadcast to subscribe to. Defaults to "data".
                """
              ]

  @impl true
  def handle_init(_ctx, opts) do
    {[], Map.from_struct(opts)}
  end

  @impl true
  def handle_playing(ctx, state) do
    {:ok, resource} = Native.start_subscriber(state.url, state.broadcast, state.track, self())
    state = Map.put(state, :resource, resource)

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.stop_subscriber(resource)
    end)

    {[stream_format: {:output, %Membrane.RemoteStream{}}], state}
  end

  @impl true
  def handle_info({:moq_frame, payload}, _ctx, state) do
    {[buffer: {:output, %Membrane.Buffer{payload: payload}}], state}
  end

  @impl true
  def handle_info(:moq_disconnected, _ctx, state) do
    {[end_of_stream: :output], state}
  end

  @impl true
  def handle_info(message, _ctx, state) do
    Membrane.Logger.warning("Received unknown message: #{inspect(message)}")
    {[], state}
  end
end
