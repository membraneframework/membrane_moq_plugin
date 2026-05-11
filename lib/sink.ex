defmodule Membrane.MoQ.Sink do
  @moduledoc """
  Membrane Sink acting as a MoQ publisher.

  Connects to a MoQ relay server and publishes audio and video tracks. One Sink
  instance owns one MoQ session and may host multiple broadcasts; each input
  pad maps to one rendition in one broadcast.

  ## Pad options

    * `:broadcast` — broadcast path this pad publishes to.
    * `:track` — rendition key inside the broadcast catalog.
                 Must be unique within the broadcast.

  Pads can be added or removed at any time during the pipeline lifecycle. The
  catalog is republished on every track add/remove.
  """
  use Membrane.Sink

  require Membrane.Logger
  require Membrane.H264
  require Membrane.H265

  alias Membrane.{AAC, Opus, H264, H265}
  alias Membrane.MoQ.Native

  def_input_pad :input,
    availability: :on_request,
    accepted_format: any_of(AAC, Opus, H264, H265),
    options: [
      broadcast: [
        spec: String.t(),
        description:
          "Broadcast path, see `Broadcast` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
      ],
      track: [
        spec: String.t(),
        description:
          "Catalog rendition key, see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
      ]
    ]

  def_options url: [
                spec: String.t(),
                description: "URL to the MoQ relay server."
              ],
              broadcast: [
                spec: String.t() | nil,
                default: nil,
                description: "Default broadcast path for pads that don't override it."
              ],
              container: [
                spec: :legacy,
                default: :legacy,
                description:
                  "Container format for media frames. Only :legacy is supported for now."
              ]

  defmodule State do
    @type resource :: reference()

    @type t :: %__MODULE__{
            url: String.t(),
            container: :legacy | :cmaf,
            # TODO: replace any with session type
            session: any(),
            broadcasts: %{String.t() => resource()},
            pads: %{
              Membrane.Pad.ref() => %{
                broadcast: String.t(),
                # TODO: we should think about getting rid of the ref here for `broadcasts` to be the only source of truth for resources
                broadcast_resource: resource(),
                track: String.t(),
                track_resource: resource() | nil
              }
            }
          }

    @enforce_keys [:url]
    defstruct @enforce_keys ++
                [
                  container: :legacy,
                  session: nil,
                  broadcasts: %{},
                  pads: %{}
                ]
  end

  @impl true
  def handle_init(_ctx, %__MODULE__{url: url} = _opts),
    do: {[], %State{url: url, session: nil, broadcasts: %{}, pads: %{}}}

  @impl true
  def handle_setup(_ctx, %State{url: url} = state) do
    {:ok, session} = Native.setup_session(url, self())
    {[setup: :incomplete], %{state | session: session}}
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state), do: {[setup: :complete], state}

  @impl true
  def handle_info(:moq_disconnected, _ctx, _state) do
    # TODO: I guess we should receive this message only when termination is unexpected and just crash
    # and have the parent restart the node if really necessary.
    # Graceful termination doesn't need to do any work
    raise "MoQ session disconnected"
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
          {resource :: State.resource(), State.t()}
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
        {nil, _} ->
          state

        {resource, broadcasts} ->
          :ok = Native.close_broadcast(resource)
          %{state | broadcasts: broadcasts}
      end
    end
  end

  defp add_track(pad_state, fmt) do
    {:ok, track_resource} = do_add_track(fmt).(pad_state.broadcast_resource, pad_state.track)
    track_resource
  end

  defp do_add_track(%H264{height: height, width: width, framerate: framerate} = fmt),
    do:
      &Native.add_h264_track(
        &1,
        &2,
        h264_codec_string(fmt),
        width,
        height,
        framerate_to_float(framerate)
      )

  defp do_add_track(%H265{height: height, width: width, framerate: framerate} = fmt),
    do:
      &Native.add_h265_track(
        &1,
        &2,
        h265_codec_string(fmt),
        width,
        height,
        framerate_to_float(framerate)
      )

  defp do_add_track(%AAC{profile: profile, sample_rate: sample_rate, channels: channels}),
    do: &Native.add_aac_track(&1, &2, aac_profile_byte(profile), sample_rate, channels)

  defp do_add_track(%Opus{channels: channels}),
    do: &Native.add_opus_track(&1, &2, 48_000, channels)

  @spec framerate_to_float({integer(), integer()} | nil) :: float()
  defp framerate_to_float({num, den}) when is_integer(num) and is_integer(den) and den > 0,
    do: num / den

  defp framerate_to_float(nil), do: 0.0

  # ---------- Codec string helpers ----------

  # H.264: produces a WebCodecs / hang codec string, e.g. "avc1.64001f".
  # Reads profile, compatibility, and level directly from the avcC DCR bytes
  # embedded in the stream_structure. Falls back to a profile-only guess for
  # raw Annex B streams that carry no DCR.
  @spec h264_codec_string(H264.t()) :: String.t()
  defp h264_codec_string(%H264{
         stream_structure: {base, <<_version, profile, compat, level, _::binary>>}
       })
       when base in [:avc1, :avc3] do
    "#{base}.#{Base.encode16(<<profile, compat, level>>, case: :lower)}"
  end

  defp h264_codec_string(%H264{profile: profile}) do
    profile_byte = h264_profile_byte(profile)
    "avc1.#{Base.encode16(<<profile_byte, 0x00, 0x1F>>, case: :lower)}"
  end

  @spec h264_profile_byte(H264.profile()) :: integer()
  defp h264_profile_byte(:baseline), do: 0x42
  defp h264_profile_byte(:main), do: 0x4D
  defp h264_profile_byte(:high), do: 0x64
  defp h264_profile_byte(:high_10), do: 0x6E
  defp h264_profile_byte(:high_422), do: 0x7A
  defp h264_profile_byte(:high_444), do: 0xF4
  defp h264_profile_byte(_), do: 0x42

  # H.265: WebCodecs string is structured like "hev1.1.6.L93.B0". Parsing the
  # full HEVC config record requires a HEVC bitstream parser we don't pull in,
  # so fall back to a sensible Main-profile default. Override via pad opts if
  # you need exact codec advertisement.
  @spec h265_codec_string(H265.t()) :: String.t()
  defp h265_codec_string(%H265{}), do: "hev1.1.6.L93.B0"

  @spec aac_profile_byte(AAC.profile()) :: integer()
  # TODO: these need correction!!!
  defp aac_profile_byte(:mpeg_4_lc), do: 2
  defp aac_profile_byte(:mpeg_4_he_v1), do: 5
  defp aac_profile_byte(:mpeg_4_he_v2), do: 29
  defp aac_profile_byte(profile) when is_integer(profile), do: profile
  defp aac_profile_byte(_), do: 2

  @spec keyframe?(Membrane.Buffer.t()) :: boolean()
  # TODO: check if h26x buffers' metadata actually contain these keys
  defp keyframe?(%Membrane.Buffer{metadata: %{h264: %{key_frame?: kf}}}), do: kf
  defp keyframe?(%Membrane.Buffer{metadata: %{h265: %{key_frame?: kf}}}), do: kf
  defp keyframe?(%Membrane.Buffer{}), do: true
end
