defmodule Membrane.MoQ.Sink do
  @moduledoc """
  Membrane Sink acting as a MoQ publisher.

  Connects to a MoQ relay server and publishes CMAF tracks as a MoQ broadcast.
  Each dynamic input pad corresponds to one track (audio or video) in the broadcast.

  The element completes setup once the QUIC session handshake with the relay finishes.
  Tracks are registered when their stream format (`Membrane.CMAF.Track`) arrives, which
  triggers population of the MoQ catalog via the fMP4 init segment in the header field.
  Subsequent buffers are fMP4 fragments sent directly to the relay.
  """
  use Membrane.Sink
  require Membrane.Logger
  alias Membrane.MoQ.Native

  def_input_pad(:input,
    accepted_format: %Membrane.CMAF.Track{},
    availability: :on_request
  )

  def_options(
    url: [
      spec: String.t(),
      description: """
      URL of the MoQ relay server (e.g. "https://localhost:4443").
      """
    ],
    broadcast: [
      spec: String.t(),
      description: """
      Name of the broadcast to publish. Subscribers connect to the relay using this name.
      """
    ]
  )

  @impl true
  def handle_init(_ctx, opts) do
    state = %{
      url: opts.url,
      broadcast: opts.broadcast,
      resource: nil,
      tracks: %{}
    }

    {[], state}
  end

  @impl true
  def handle_setup(_ctx, state) do
    {:ok, resource} = Native.start_publisher(state.url, state.broadcast, self())
    {[setup: :incomplete], %{state | resource: resource}}
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state) do
    {[setup: :complete], state}
  end

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message received: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_pad_added(Pad.ref(:input, track_id), _ctx, state) do
    new_tracks = Map.put(state.tracks, track_id, nil)
    {[], %{state | tracks: new_tracks}}
  end

  @impl true
  def handle_stream_format(
        Pad.ref(:input, track_id),
        %Membrane.CMAF.Track{header: header},
        _ctx,
        state
      ) do
    :ok = Native.add_track(state.resource, track_id, header)
    {[], state}
  end

  @impl true
  def handle_buffer(Pad.ref(:input, track_id), buffer, _ctx, state) do
    :ok = Native.send_segment(state.resource, track_id, buffer.payload)
    {[], state}
  end

  @impl true
  def handle_end_of_stream(Pad.ref(:input, _track_id), _ctx, state) do
    #    if map_size(new_tracks) == 0 do
    #      :ok = Native.stop_publisher(state.resource)
    #    end

    {[], state}
  end
end
