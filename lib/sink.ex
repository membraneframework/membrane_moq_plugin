defmodule Membrane.MoQ.Sink do
  @moduledoc """
  Membrane Sink acting as a MoQ publisher.

  Connects to a MoQ relay server and publishes audio and video tracks
  to a single, configured broadcast.

  Tracks are advertised in both the hang and the IETF MSF catalog,
  so hang-native and MSF-based consumers can subscribe alike.
  Pads can be added or removed at any time during the pipeline lifecycle.
  The catalogs are republished on every track add/remove and mid-stream format change.

  Frames are encapsulated in the wire container selected with the `container` option
  and optionally batched with `latency`.

  ## Parent notifications

    * `{:disconnected, reason :: String.t()}`
        when the session drops after setup. The sink takes no action on its own;
        the parent decides whether to tear it down.
  """
  use Membrane.Sink

  require Membrane.Logger
  require Membrane.H264
  require Membrane.H265

  alias ExMoQ.Native
  alias Membrane.MoQ.TrackFormat

  # MoQ explicitly expects raw AAC/Opus frames:
  # https://github.com/moq-dev/moq/blob/21da717b92c73d8b3643a0028e7554b8b1149943/rs/moq-mux/src/codec/aac/mod.rs#L4
  # https://github.com/moq-dev/moq/blob/21da717b92c73d8b3643a0028e7554b8b1149943/rs/moq-mux/src/codec/opus/mod.rs#L4

  def_input_pad :input,
    availability: :on_request,
    accepted_format:
      any_of(
        %Membrane.AAC{encapsulation: :none},
        %Membrane.Opus{self_delimiting?: false},
        %Membrane.H264{stream_structure: ss} when Membrane.H264.is_avc(ss),
        %Membrane.H265{stream_structure: ss} when Membrane.H265.is_hvc(ss)
      ),
    options: [
      track: [
        spec: ExMoQ.Native.track(),
        description: """
        Track name for this pad's stream,
        this will be the track name advertised by the hang/MSF catalog.
        See `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology.
        """
      ],
      priority: [
        spec: 0..255 | nil,
        default: nil,
        description: """
        Delivery priority of this pad's track.
        Under congestion, tracks with a higher value are sent first.
        When nil, hang defaults for the track's media kind are used.
        """
      ]
    ]

  def_options url: [
                spec: String.t(),
                description: "URL to the MoQ relay server."
              ],
              broadcast: [
                spec: String.t(),
                description:
                  "Broadcast path, see `Broadcast` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
              ],
              container: [
                spec: :legacy | :loc,
                default: :legacy,
                description: """
                Wire container for media frames, applied to every track of this sink
                and advertised per rendition in the catalog.
                """
              ],
              latency: [
                spec: Membrane.Time.t(),
                default: Membrane.Time.milliseconds(0),
                description: """
                How long each track buffers frames before writing them to the wire,
                trading latency for batched writes.
                With the default of `0`, every frame is written immediately.
                Audio tracks are unaffected apart from up to one frame of added delay:
                every audio frame starts a new MoQ group, which flushes the buffer.
                """
              ],
              disable_tls_verify?: [
                spec: boolean(),
                default: false,
                description:
                  "Whether to disable TLS verification when connecting to the relay. Useful for local development."
              ]

  defmodule State do
    @moduledoc false

    @type t :: %__MODULE__{
            url: String.t(),
            container: Native.container(),
            latency: Membrane.Time.t(),
            disable_tls_verify?: boolean(),
            session: Native.session() | nil,
            broadcast: String.t(),
            producer: Native.broadcast_producer() | nil,
            tracks: %{Membrane.Pad.ref() => Native.track_resource()}
          }

    @enforce_keys [:url, :broadcast, :container, :latency, :disable_tls_verify?]
    defstruct @enforce_keys ++
                [
                  :session,
                  :producer,
                  tracks: %{}
                ]
  end

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %State{
       url: opts.url,
       broadcast: opts.broadcast,
       container: opts.container,
       latency: opts.latency,
       disable_tls_verify?: opts.disable_tls_verify?
     }}
  end

  @impl true
  def handle_setup(ctx, %State{url: url, disable_tls_verify?: disable_tls_verify?} = state) do
    {:ok, session} = Native.create_session(url, self(), disable_tls_verify?)

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.close_session(session)
    end)

    {[setup: :incomplete], %{state | session: session}}
  end

  @impl true
  def handle_info(:moq_connected, ctx, %State{session: session, broadcast: broadcast} = state) do
    {:ok, producer} = Native.create_broadcast_producer(session, broadcast)

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.close_broadcast_producer(producer)
    end)

    {[setup: :complete], %{state | producer: producer}}
  end

  @impl true
  def handle_info({:moq_setup_failed, reason}, _ctx, _state),
    do: raise("MoQ session setup failed with reason: #{inspect(reason)}")

  @impl true
  def handle_info({:moq_disconnected, reason}, _ctx, %State{producer: nil}),
    do: raise("MoQ session closed during setup with reason: #{inspect(reason)}")

  @impl true
  def handle_info({:moq_disconnected, reason}, _ctx, state) do
    Membrane.Logger.warning("MoQ session disconnected: #{inspect(reason)}")
    {[notify_parent: {:disconnected, reason}], state}
  end

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message received: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_stream_format(pad, fmt, ctx, state) do
    %{options: options} = ctx.pads[pad]

    state =
      case ctx.old_stream_format do
        ^fmt -> state
        nil -> add_track(pad, fmt, options, state)
        _changed -> update_track(pad, fmt, state)
      end

    {[], state}
  end

  @impl true
  def handle_buffer(pad, %Membrane.Buffer{} = buffer, ctx, state) do
    track_resource = state.tracks[pad]
    track_name = ctx.pads[pad].options.track

    if buffer.pts == nil or buffer.pts < 0 do
      raise "Buffer PTS must be a non-negative integer, received: #{inspect(buffer.pts)}"
    end

    case Native.send_frame(
           track_resource,
           buffer.pts,
           TrackFormat.keyframe?(buffer, ctx.pads[pad].stream_format),
           buffer.payload
         ) do
      :ok ->
        :ok

      :moq_missing_keyframe ->
        Membrane.Logger.debug("""
        Buffer rejected because it is not a keyframe.
        Starting a MoQ group requires a keyframe to be sent first.
        """)

      {:error, reason} ->
        raise "Failed to send frame to track #{inspect(track_name)}: #{reason}"
    end

    {[], state}
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state), do: {[], close_pad(pad, state)}

  @impl true
  def handle_end_of_stream(pad, _ctx, state), do: {[], close_pad(pad, state)}

  @spec add_track(Membrane.Pad.ref(), Membrane.StreamFormat.t(), map(), State.t()) :: State.t()
  defp add_track(pad, fmt, %{track: track, priority: priority}, state) do
    track_fmt = TrackFormat.from_stream_format(fmt)

    Native.add_track(
      state.producer,
      track,
      track_fmt,
      priority || TrackFormat.default_priority(track_fmt),
      state.container,
      Membrane.Time.as_nanoseconds(state.latency, :round)
    )
    |> case do
      {:ok, track_resource} ->
        put_in(state.tracks[pad], track_resource)

      {:error, reason} ->
        raise "Failed to add track #{inspect(track)} for pad #{inspect(pad)}, reason: #{inspect(reason)}"
    end
  end

  @spec update_track(Membrane.Pad.ref(), Membrane.StreamFormat.t(), State.t()) :: State.t()
  defp update_track(pad, fmt, state) do
    case Native.update_track(state.tracks[pad], TrackFormat.from_stream_format(fmt)) do
      :ok ->
        state

      {:error, reason} ->
        raise "Failed to update stream format of pad #{inspect(pad)}, reason: #{inspect(reason)}"
    end
  end

  @spec close_pad(Membrane.Pad.ref(), State.t()) :: State.t()
  defp close_pad(pad, state) do
    case Map.pop(state.tracks, pad) do
      {nil, _pads} ->
        state

      {track_resource, tracks} ->
        :ok = Native.remove_track(track_resource)
        %{state | tracks: tracks}
    end
  end
end
