defmodule Membrane.MoQ.Sink do
  @moduledoc """
  Membrane Sink acting as a MoQ publisher.

  Connects to a MoQ relay server and publishes audio and video tracks. One Sink
  instance owns one MoQ session and may host multiple broadcasts; each input
  pad maps to one rendition in one broadcast.

  Pads can be added or removed at any time during the pipeline lifecycle. The
  catalog is republished on every track add/remove.
  """
  use Membrane.Sink

  require Membrane.Logger
  require Membrane.H264
  require Membrane.H265

  alias Membrane.{AAC, H264, H265, Opus}
  alias Membrane.MoQ.Native

  def_input_pad :input,
    availability: :on_request,
    accepted_format:
      any_of(
        AAC,
        Opus,
        %H264{stream_structure: ss} when H264.is_avc(ss),
        %H265{stream_structure: ss} when H265.is_hvc(ss)
      ),
    options: [
      broadcast: [
        spec: String.t(),
        description:
          "Broadcast path, see `Broadcast` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
      ],
      track: [
        spec: String.t(),
        description:
          "Track name for this pad's stream, see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
      ]
    ]

  def_options url: [
                spec: String.t(),
                description: "URL to the MoQ relay server."
              ],
              container: [
                spec: :legacy,
                default: :legacy,
                description:
                  "Container format for media frames. Only :legacy is supported for now."
              ],
              disable_tls_verify?: [
                spec: boolean(),
                default: false,
                description:
                  "Whether to disable TLS verification when connecting to the relay. Useful for local development."
              ]

  defmodule State do
    @moduledoc false

    @type pad_state :: %{
            broadcast: String.t(),
            broadcast_resource: Native.broadcast(),
            track: String.t(),
            track_resource: Native.track() | nil
          }

    @type t :: %__MODULE__{
            url: String.t(),
            container: :legacy | :cmaf,
            disable_tls_verify?: boolean(),
            session: Native.session(),
            broadcasts: %{String.t() => Native.broadcast()},
            pads: %{Membrane.Pad.ref() => pad_state()}
          }

    @enforce_keys [:url, :container, :disable_tls_verify?]
    defstruct @enforce_keys ++
                [
                  session: nil,
                  broadcasts: %{},
                  pads: %{}
                ]
  end

  @impl true
  def handle_init(_ctx, opts) do
    {[], struct(State, Map.from_struct(opts))}
  end

  @impl true
  def handle_setup(_ctx, %State{url: url, disable_tls_verify?: disable_tls_verify?} = state) do
    {:ok, session} = Native.setup_session(url, self(), disable_tls_verify?)
    {[setup: :incomplete], %{state | session: session}}
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state), do: {[setup: :complete], state}

  @impl true
  def handle_info({:moq_write_failed, track}, _ctx, state) do
    Membrane.Logger.warning("Frame write failed for track: #{inspect(track)}")
    {[], state}
  end

  @impl true
  def handle_info({:moq_setup_failed, reason}, _ctx, _state) do
    raise "MoQ session setup failed with reason: #{inspect(reason)}"
  end

  @impl true
  def handle_info({:moq_disconnected, reason}, _ctx, _state) do
    raise "MoQ session closed with reason: #{inspect(reason)}"
  end

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.info("Unknown message: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_pad_added(
        pad,
        %{pad_options: %{broadcast: broadcast, track: track}} = _ctx,
        state
      ) do
    {broadcast_resource, state} = ensure_broadcast(state, broadcast)

    pad_state = %{
      broadcast: broadcast,
      broadcast_resource: broadcast_resource,
      track: track,
      track_resource: nil
    }

    {[], put_in(state.pads[pad], pad_state)}
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state), do: {[], close_pad(pad, state)}

  @impl true
  def handle_stream_format(pad, fmt, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    track_resource = add_track(pad_state, fmt)
    {[], put_in(state.pads[pad], %{pad_state | track_resource: track_resource})}
  end

  @impl true
  def handle_buffer(pad, %Membrane.Buffer{} = buffer, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    timestamp_us = Membrane.Time.as_microseconds(buffer.pts, :round)

    :ok =
      Native.send_frame(pad_state.track_resource, timestamp_us, keyframe?(buffer), buffer.payload)

    {[], state}
  end

  @impl true
  def handle_end_of_stream(pad, _ctx, state) do
    state = close_pad(pad, state)

    if state.pads == %{} do
      :ok = Native.close_session(state.session)
    end

    {[], state}
  end

  @spec close_pad(Membrane.Pad.ref(), State.t()) :: State.t()
  defp close_pad(pad, state) do
    {pad_state, pads} = Map.pop(state.pads, pad)
    state = %{state | pads: pads}

    case pad_state do
      %{track_resource: track_resource, broadcast: broadcast} ->
        :ok = Native.remove_track(track_resource)
        maybe_close_broadcast(state, broadcast)

      nil ->
        state
    end
  end

  @spec ensure_broadcast(State.t(), path :: String.t()) ::
          {Native.broadcast(), State.t()}
  defp ensure_broadcast(state, path) do
    case Map.fetch(state.broadcasts, path) do
      {:ok, resource} ->
        {resource, state}

      :error ->
        {:ok, resource} = Native.open_broadcast(state.session, path)
        {resource, put_in(state.broadcasts[path], resource)}
    end
  end

  @spec maybe_close_broadcast(State.t(), path :: String.t() | nil) :: State.t()
  defp maybe_close_broadcast(state, nil), do: state

  defp maybe_close_broadcast(state, path) do
    still_used? = Enum.any?(state.pads, fn {_pad, ps} -> ps.broadcast == path end)

    if still_used? do
      state
    else
      case Map.pop(state.broadcasts, path) do
        {nil, _broadcasts} ->
          state

        {resource, broadcasts} ->
          :ok = Native.close_broadcast(resource)
          %{state | broadcasts: broadcasts}
      end
    end
  end

  @spec add_track(State.pad_state(), Membrane.StreamFormat.t()) :: Native.track()
  defp add_track(pad_state, fmt) do
    {:ok, track_resource} = do_add_track(pad_state.broadcast_resource, pad_state.track, fmt)
    track_resource
  end

  @spec do_add_track(
          Native.broadcast(),
          track :: String.t(),
          Membrane.StreamFormat.t()
        ) :: {:ok, Native.track()} | {:error, reason :: String.t()}
  defp do_add_track(broadcast_resource, track, %H264{
         height: height,
         width: width,
         framerate: framerate,
         stream_structure: {tag, dcr}
       }) do
    dcr_parsed = Membrane.H264.DecoderConfigurationRecord.parse(dcr)

    Native.add_h264_track(
      self(),
      broadcast_resource,
      track,
      %Membrane.MoQ.Native.VideoTrackParams{
        width: width,
        height: height,
        framerate: framerate_to_float(framerate)
      },
      dcr,
      %Membrane.MoQ.Native.H264Codec{
        inline:
          case tag do
            :avc1 -> false
            :avc3 -> true
          end,
        profile: dcr_parsed.avc_profile_indication,
        constraints: dcr_parsed.profile_compatibility,
        level: dcr_parsed.avc_level
      }
    )
  end

  defp do_add_track(broadcast_resource, track, %H265{
         height: height,
         width: width,
         framerate: framerate,
         stream_structure: {tag, dcr}
       }) do
    dcr_parsed = Membrane.H265.DecoderConfigurationRecord.parse(dcr)

    Native.add_h265_track(
      self(),
      broadcast_resource,
      track,
      %Membrane.MoQ.Native.VideoTrackParams{
        width: width,
        height: height,
        framerate: framerate_to_float(framerate)
      },
      dcr,
      %Membrane.MoQ.Native.H265Codec{
        in_band:
          case tag do
            :hev1 -> true
            :hvc1 -> false
          end,
        profile_space: dcr_parsed.profile_space,
        profile_idc: dcr_parsed.profile_idc,
        profile_compatibility_flags:
          <<dcr_parsed.profile_compatibility_flags::32>> |> :binary.bin_to_list(),
        tier_flag: dcr_parsed.tier_flag > 0,
        level_idc: dcr_parsed.level_idc,
        constraint_flags: <<dcr_parsed.constraint_indicator_flags::48>> |> :binary.bin_to_list()
      }
    )
  end

  defp do_add_track(broadcast_resource, track, %AAC{
         profile: profile,
         sample_rate: sample_rate,
         channels: channels
       }),
       do:
         Native.add_aac_track(
           self(),
           broadcast_resource,
           track,
           AAC.profile_to_aot_id(profile),
           sample_rate,
           channels
         )

  defp do_add_track(broadcast_resource, track, %Opus{channels: channels}),
    do: Native.add_opus_track(self(), broadcast_resource, track, 48_000, channels)

  @spec framerate_to_float({integer(), integer()} | nil) :: float()
  defp framerate_to_float({num, den}) when is_integer(num) and is_integer(den) and den > 0,
    do: num / den

  defp framerate_to_float(nil), do: 0.0

  @spec keyframe?(Membrane.Buffer.t()) :: boolean()
  defp keyframe?(%Membrane.Buffer{metadata: %{h264: %{key_frame?: kf}}}), do: kf
  defp keyframe?(%Membrane.Buffer{metadata: %{h265: %{key_frame?: kf}}}), do: kf
  defp keyframe?(%Membrane.Buffer{}), do: true
end
