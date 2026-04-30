defmodule Membrane.MoQ.Sink do
  @moduledoc """
  Membrane Sink acting as a MoQ publisher.

  Connects to a MoQ relay server and publishes audio and video tracks as a MoQ broadcast
  with a hang-compatible catalog.

  Add `:video` and `:audio` pads before the pipeline transitions to `:playing`. Each
  pad corresponds to one track in the broadcast. The catalog is published once all
  stream formats have been received.
  """
  use Membrane.Sink
  require Membrane.Logger
  alias Membrane.{H264, H265, AAC, Opus, Pad}
  alias Membrane.MoQ.Native

  # Formats supported by the hang catalog (moq-lite media layer).
  # AV1, VP8, VP9 are also supported by hang but lack Membrane stream format modules.
  def_input_pad(:video,
    availability: :on_request,
    accepted_format: any_of(%H264{}, %H265{})
  )

  def_input_pad(:audio,
    availability: :on_request,
    accepted_format: any_of(%AAC{}, %Opus{})
  )

  def_options(
    url: [
      spec: String.t(),
      description: "URL of the MoQ relay server (e.g. \"https://localhost:4443\")."
    ],
    broadcast: [
      spec: String.t(),
      description: "Broadcast path to publish under (e.g. \"live/camera\")."
    ]
  )

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %{
       url: opts.url,
       broadcast: opts.broadcast,
       resource: nil,
       tracks: %{},
       video_pad: nil,
       audio_pad: nil
     }}
  end

  @impl true
  def handle_setup(_ctx, state) do
    {:ok, resource} = Native.setup_publisher(state.url, state.broadcast, self())
    {[setup: :incomplete], %{state | resource: resource}}
  end

  @impl true
  def handle_info(:moq_connected, _ctx, state) do
    {[setup: :complete], state}
  end

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_pad_added(_pad, ctx, _state) when ctx.playback == :playing do
    raise "Pads can only be added to #{inspect(__MODULE__)} before playback starts"
  end

  @impl true
  def handle_pad_added(Pad.ref(type, _id) = pad, _ctx, state) do
    pad_key = case type do
      :audio -> :audio_pad
      :video -> :video_pad
    end

    {[], Map.update!(state, pad_key, fn
      nil -> pad
      ^pad -> pad
      _other_pad -> raise "#{inspect(__MODULE__)} can only handle at most one #{type} pad"
    end)}
  end

  @impl true
  def handle_stream_format(Pad.ref(:video, _id) = pad, %H264{width: width, height: height, framerate: {fps_num, fps_den}} = fmt, _ctx, state) do

    codec = h264_codec_string(fmt)
    :ok = Native.configure_publisher(state.resource, codec, width, height, fps_num / fps_den)
    {[], %{state | tracks: Map.put(state.tracks, pad, fmt)}}
  end

  @impl true
  def handle_stream_format(Pad.ref(type, _id) = pad, stream_format, _ctx, state) do
    Membrane.Logger.debug("Stream format on #{type} pad: #{inspect(stream_format)}")
    {[], %{state | tracks: Map.put(state.tracks, pad, stream_format)}}
  end

  @impl true
  def handle_playing(_ctx, state) do
    {[], state}
  end

  @impl true
  def handle_buffer(Pad.ref(:video, _id), %Membrane.Buffer{} = buffer, _ctx, state) do
    timestamp_us = Membrane.Time.as_microseconds(buffer.pts, :round)
    keyframe = buffer.metadata[:h264][:key_frame?] || false
    :ok = Native.send_segment(state.resource, timestamp_us, keyframe, buffer.payload)
    {[], state}
  end

  # Produces a WebCodecs / hang codec string for H.264, e.g. "avc1.64001f".
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

  @impl true
  def handle_end_of_stream(_pad, ctx, state) do
    all_done =
      ctx.pads
      |> Map.keys()
      |> Enum.all?(fn pad -> ctx.pads[pad].end_of_stream? end)

    if all_done do
      :ok = Native.stop_publisher(state.resource)
    end

    {[], state}
  end
end
