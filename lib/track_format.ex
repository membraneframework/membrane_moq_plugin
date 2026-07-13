defmodule Membrane.MoQ.TrackFormat do
  @moduledoc false
  # Codec translation between Membrane stream formats
  # and the native MoQ track-format term (`t:ExMoQ.Native.track_format/0`).

  alias ExMoQ.Native

  alias Membrane.{AAC, H264, H265, Opus, RemoteStream}

  @doc """
  Build the native track-format term the Sink publishes from a Membrane stream
  format.
  """
  @spec from_stream_format(Membrane.StreamFormat.t()) :: Native.track_format()
  def from_stream_format(%H264{
        height: height,
        width: width,
        framerate: framerate,
        stream_structure: {tag, dcr}
      }) do
    dcr_parsed = Membrane.H264.DecoderConfigurationRecord.parse(dcr)

    {:h264,
     %{
       params: %Native.VideoTrackParams{
         width: width,
         height: height,
         framerate: framerate_to_float(framerate)
       },
       description: dcr,
       codec: %Native.H264Codec{
         inline:
           case tag do
             :avc1 -> false
             :avc3 -> true
           end,
         profile: dcr_parsed.avc_profile_indication,
         constraints: dcr_parsed.profile_compatibility,
         level: dcr_parsed.avc_level
       }
     }}
  end

  def from_stream_format(%H265{
        height: height,
        width: width,
        framerate: framerate,
        stream_structure: {tag, dcr}
      }) do
    dcr_parsed = Membrane.H265.DecoderConfigurationRecord.parse(dcr)

    {:h265,
     %{
       params: %Native.VideoTrackParams{
         width: width,
         height: height,
         framerate: framerate_to_float(framerate)
       },
       description: dcr,
       codec: %Native.H265Codec{
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
     }}
  end

  def from_stream_format(%AAC{profile: profile, sample_rate: sample_rate, channels: channels}),
    do:
      {:aac,
       %{
         params: %Native.AudioTrackParams{
           sample_rate: sample_rate,
           channels: channels
         },
         codec: %Native.AACCodec{profile: AAC.profile_to_aot_id(profile)}
       }}

  def from_stream_format(%Opus{channels: channels}),
    do: {:opus, %{params: %Native.AudioTrackParams{sample_rate: 48_000, channels: channels}}}

  @doc """
  Media type advertised by a native track-format term.
  """
  @spec media_type(Native.track_format()) :: :video | :audio | :unknown
  def media_type({:h264, _params}), do: :video
  def media_type({:h265, _params}), do: :video
  def media_type({:aac, _params}), do: :audio
  def media_type({:opus, _params}), do: :audio
  def media_type(:unrecognized), do: :unknown

  @doc """
  Reconstruct a Membrane stream format from a native track-format term,
  or `RemoteStream.t()` if it is not recognized.
  """
  @spec to_stream_format(Native.track_format()) ::
          H264.t() | H265.t() | AAC.t() | Opus.t() | RemoteStream.t()
  def to_stream_format(
        {:h264,
         %{
           params: %{width: width, height: height, framerate: framerate},
           description: dcr,
           codec: %{inline: inline} = codec
         }}
      ) do
    %H264{
      width: width,
      height: height,
      framerate: framerate(framerate),
      stream_structure: {if(inline, do: :avc3, else: :avc1), h264_dcr(dcr, codec)}
    }
  end

  def to_stream_format(
        {:h265,
         %{
           params: %{width: width, height: height, framerate: framerate},
           description: dcr,
           codec: %{in_band: in_band}
         }}
      ) do
    %H265{
      width: width,
      height: height,
      framerate: framerate(framerate),
      stream_structure: {if(in_band, do: :hev1, else: :hvc1), dcr}
    }
  end

  def to_stream_format(
        {:aac,
         %{params: %{sample_rate: sample_rate, channels: channels}, codec: %{profile: profile}}}
      ) do
    %AAC{
      sample_rate: sample_rate,
      channels: channels,
      profile: AAC.aot_id_to_profile(profile)
    }
  end

  def to_stream_format({:opus, %{params: %{channels: channels}}}) do
    %Opus{channels: channels, self_delimiting?: false}
  end

  def to_stream_format(:unrecognized), do: %Membrane.RemoteStream{type: :packetized}

  @spec keyframe?(Membrane.Buffer.t()) :: boolean()
  def keyframe?(%Membrane.Buffer{metadata: %{h264: %{key_frame?: kf}}}), do: kf
  def keyframe?(%Membrane.Buffer{metadata: %{h265: %{key_frame?: kf}}}), do: kf
  def keyframe?(%Membrane.Buffer{}), do: true

  @spec buffer_metadata(boolean(), Membrane.StreamFormat.t()) :: Membrane.Buffer.metadata()
  def buffer_metadata(keyframe?, %H264{}), do: %{h264: %{key_frame?: keyframe?}}
  def buffer_metadata(keyframe?, %H265{}), do: %{h265: %{key_frame?: keyframe?}}
  def buffer_metadata(_keyframe?, _audio_format), do: %{}

  @spec h264_dcr(binary(), map()) :: binary()
  # hang allows an in-band (avc3) rendition to omit the catalog description.
  # Membrane's avc3 still requires an avcC, so synthesise one from the codec fields.
  defp h264_dcr(<<>>, %{inline: true, profile: profile, constraints: constraints, level: level}) do
    <<1, profile, constraints, level, 0b111111::6, _length_size_minus_one = 3::2, 0b111::3,
      _num_sps = 0::5, _num_pps = 0::8>>
  end

  defp h264_dcr(dcr, _codec), do: dcr

  @spec framerate_to_float({integer(), integer()} | nil) :: float()
  defp framerate_to_float({num, den}) when is_integer(num) and is_integer(den) and den > 0,
    do: num / den

  defp framerate_to_float(nil), do: 0.0

  @spec framerate(float()) :: {pos_integer(), pos_integer()} | nil
  defp framerate(fps) when is_number(fps) and fps > 0 do
    if fps == Float.floor(fps * 1.0), do: {trunc(fps), 1}, else: {round(fps * 1000), 1000}
  end

  defp framerate(_fps), do: nil
end
