defmodule Membrane.MoQ.Sink do
  @moduledoc """
  Membrane Sink acting as a MoQ publisher.

  Connects to a MoQ relay server and publishes audio and video tracks. One Sink
  instance owns one MoQ session and may host multiple broadcasts; each input
  pad maps to one rendition in one broadcast.

  ## Pad options

    * `:broadcast` — broadcast path this pad publishes to. Defaults to the
      Sink-level `:broadcast` option. Required if neither is set.
    * `:track_name` — rendition key inside the broadcast catalog. Defaults to
      `"video"` for video formats and `"audio"` for audio formats. Must be
      unique within the broadcast.

  Pads can be added or removed at any time during the pipeline lifecycle. The
  catalog is republished on every track add/remove.
  """
  use Membrane.Sink

  require Membrane.Logger
  require Membrane.H264
  require Membrane.H265

  alias Membrane.{H264, H265, AAC, Opus}
  alias Membrane.MoQ.Native

  def_input_pad :input,
    availability: :on_request,
    accepted_format: any_of(
      %AAC{config: {:esds, _esds}},
        %Opus{self_delimiting?: false},
        H264, H265
        # %H264{stream_structure: structure, alignment: :au} when H264.is_avc(structure),
        # %H265{stream_structure: structure, alignment: :au} when H265.is_hvc(structure)
    ),
    options: [
      broadcast: [
        spec: String.t() | nil,
        default: nil,
        description: "Broadcast path. Overrides the Sink's :broadcast option."
      ],
      track_name: [
        spec: String.t() | nil,
        default: nil,
        description: "Catalog rendition key. Defaults to \"video\" or \"audio\" based on format."
      ]
    ]

  def_options url: [
                spec: String.t(),
                description: "URL of the MoQ relay server (e.g. \"https://localhost:4443\")."
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
                  "Container format for media frames. Only :legacy is supported right now; the option exists so future container formats (e.g. CMAF) can be selected without an API break."
              ]

#  defmodule State do
#    @type t :: %__MODULE__{
#      default_url: String.t() | nil,
#      default_broadcast: String.t() | nil,
#      container: :legacy | :cmaf,
#      session: any(), # TODO: replace any with session type
#      broadcasts: %{String.t() => any()}, # TODO: replace any with resource type
#      pads: %{Membrane.Pad.t() => %{
#        broadcast: String.t(),
#        # TODO: we should think about getting rid of the ref here for broadcasts to be the only source of truth for resources
#        broadcast_resource: any(), # TODO: replace any with resource type
#        track: String.t() | nil
#      }}
#    }
#  end

  @impl true
  def handle_init(_ctx, opts) do
    # opts.container is accepted for forward compat but the only value supported
    # by the native layer right now is :legacy.
    {[],
     %{
       url: opts.url,
       default_broadcast: opts.broadcast,
       session: nil,
       broadcasts: %{},
       pads: %{}
     }}
  end

  @impl true
  def handle_setup(_ctx, state) do
    {:ok, session} = Native.setup_session(state.url, self())
    {[setup: :incomplete], %{state | session: session}}
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state) do
    {[setup: :complete], state}
  end

  @impl true
  def handle_info(:moq_disconnected, _ctx, state) do
    Membrane.Logger.warning("MoQ session disconnected")
    {[], state}
  end

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_pad_added(pad, %{pad_options: pad_opts} = _ctx, state) do
    broadcast_path = pad_opts[:broadcast] || state.default_broadcast

    if broadcast_path == nil do
      raise "#{inspect(__MODULE__)} pad #{inspect(pad)} has no :broadcast option and Sink has no :broadcast default"
    end

    {broadcast_resource, state} = ensure_broadcast(state, broadcast_path)

    pad_state = %{
      broadcast: broadcast_path,
      broadcast_resource: broadcast_resource,
      track_name_override: pad_opts[:track_name],
      track: nil
    }

    {[], put_in(state.pads[pad], pad_state)}
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state) do
    {pad_state, pads} = Map.pop(state.pads, pad)

    if pad_state && pad_state.track do
      :ok = Native.remove_track(pad_state.track)
    end

    state = %{state | pads: pads}
    state = maybe_close_broadcast(state, pad_state && pad_state.broadcast)
    {[], state}
  end

  @impl true
  def handle_stream_format(pad, %H264{} = fmt, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    %H264{width: width, height: height, framerate: framerate} = fmt
    fps = framerate_to_float(framerate)
    codec = h264_codec_string(fmt)
    track_name = pad_state.track_name_override || "video"

    {:ok, track} =
      Native.add_h264_track(
        pad_state.broadcast_resource,
        track_name,
        codec,
        width,
        height,
        fps
      )

    {[], put_in(state.pads[pad], %{pad_state | track: track})}
  end

  @impl true
  def handle_stream_format(pad, %H265{} = fmt, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    %H265{width: width, height: height, framerate: framerate} = fmt
    fps = framerate_to_float(framerate)
    codec = h265_codec_string(fmt)
    track_name = pad_state.track_name_override || "video"

    {:ok, track} =
      Native.add_h265_track(
        pad_state.broadcast_resource,
        track_name,
        codec,
        width,
        height,
        fps
      )

    {[], put_in(state.pads[pad], %{pad_state | track: track})}
  end

  @impl true
  def handle_stream_format(pad, %AAC{} = fmt, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    %AAC{profile: profile, sample_rate: sample_rate, channels: channels} = fmt
    track_name = pad_state.track_name_override || "audio"

    {:ok, track} =
      Native.add_aac_track(
        pad_state.broadcast_resource,
        track_name,
        aac_profile_byte(profile),
        sample_rate,
        channels
      )

    {[], put_in(state.pads[pad], %{pad_state | track: track})}
  end

  @impl true
  def handle_stream_format(pad, %Opus{channels: channels}, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    track_name = pad_state.track_name_override || "audio"

    {:ok, track} =
      Native.add_opus_track(
        pad_state.broadcast_resource,
        track_name,
        48_000,
        channels
      )

    {[], put_in(state.pads[pad], %{pad_state | track: track})}
  end

  @impl true
  def handle_buffer(pad, %Membrane.Buffer{} = buffer, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    timestamp_us = Membrane.Time.as_microseconds(buffer.pts, :round)
    :ok = Native.send_frame(pad_state.track, timestamp_us, keyframe?(buffer), buffer.payload)
    {[], state}
  end

  @impl true
  def handle_end_of_stream(_pad, ctx, state) do
    all_done =
      ctx.pads
      |> Map.keys()
      |> Enum.all?(fn pad -> ctx.pads[pad].end_of_stream? end)

    if all_done do
      :ok = Native.close_session(state.session)
    end

    {[], state}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp ensure_broadcast(state, path) do
    case Map.fetch(state.broadcasts, path) do
      {:ok, resource} ->
        {resource, state}

      :error ->
        {:ok, resource} = Native.open_broadcast(state.session, path)
        {resource, put_in(state.broadcasts[path], resource)}
    end
  end

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

  defp framerate_to_float({num, den}) when is_integer(num) and is_integer(den) and den > 0,
    do: num / den

  defp framerate_to_float(nil), do: 0.0

  # ---------- Codec string helpers ----------

  # H.264: produces a WebCodecs / hang codec string, e.g. "avc1.64001f".
  # Reads profile, compatibility, and level directly from the avcC DCR bytes
  # embedded in the stream_structure. Falls back to a profile-only guess for
  # raw Annex B streams that carry no DCR.
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
  defp h265_codec_string(%H265{}), do: "hev1.1.6.L93.B0"

  defp aac_profile_byte(:mpeg_4_lc), do: 2
  defp aac_profile_byte(:mpeg_4_he_v1), do: 5
  defp aac_profile_byte(:mpeg_4_he_v2), do: 29
  defp aac_profile_byte(profile) when is_integer(profile), do: profile
  defp aac_profile_byte(_), do: 2

  defp keyframe?(%Membrane.Buffer{metadata: %{h264: %{key_frame?: kf}}}), do: kf
  defp keyframe?(%Membrane.Buffer{metadata: %{h265: %{key_frame?: kf}}}), do: kf
  # Audio: every frame is independently decodable, so it's effectively a keyframe.
  defp keyframe?(%Membrane.Buffer{}), do: true
end
