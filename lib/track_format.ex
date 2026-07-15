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
  Default delivery priority for a track format, following hang's convention
  """
  @spec default_priority(Native.track_format()) :: 0..255
  def default_priority(format) do
    case media_type(format) do
      :audio -> 80
      :video -> 60
      :unknown -> 0
    end
  end

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
           codec: %{inline: inline}
         }}
      ) do
    %H264{
      width: dimension(width),
      height: dimension(height),
      framerate: framerate(framerate),
      stream_structure: h264_stream_structure(dcr, inline)
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
      width: dimension(width),
      height: dimension(height),
      framerate: framerate(framerate),
      stream_structure: h265_stream_structure(dcr, in_band)
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

  @spec keyframe?(Membrane.Buffer.t(), Membrane.StreamFormat.t()) :: boolean()
  def keyframe?(%Membrane.Buffer{metadata: %{h264: %{key_frame?: kf}}}, %H264{}), do: kf
  def keyframe?(%Membrane.Buffer{metadata: %{h265: %{key_frame?: kf}}}, %H265{}), do: kf

  def keyframe?(%Membrane.Buffer{metadata: metadata}, %codec{}) when codec in [H264, H265],
    do:
      raise("""
      #{inspect(codec)} buffer carries no key_frame? metadata \
      (metadata keys: #{inspect(Map.keys(metadata))}).
      MoQ groups must start at keyframes.
      """)

  def keyframe?(%Membrane.Buffer{}, _audio_format), do: true

  @spec buffer_metadata(boolean(), Membrane.StreamFormat.t()) :: Membrane.Buffer.metadata()
  def buffer_metadata(keyframe?, %H264{}), do: %{h264: %{key_frame?: keyframe?}}
  def buffer_metadata(keyframe?, %H265{}), do: %{h265: %{key_frame?: keyframe?}}
  def buffer_metadata(_keyframe?, _audio_format), do: %{}

  # Payload framing follows upstream's WebCodecs-style convention: a rendition
  # with a catalog description carries length-prefixed samples, one without
  # carries Annex B with in-band parameter sets (regardless of the avc1/avc3
  # flag, which only describes where the parameter sets live).
  @spec h264_stream_structure(binary(), boolean()) :: H264.stream_structure()
  defp h264_stream_structure(<<>>, _inline), do: :annexb
  defp h264_stream_structure(dcr, inline), do: {if(inline, do: :avc3, else: :avc1), dcr}

  @spec h265_stream_structure(binary(), boolean()) :: H265.stream_structure()
  defp h265_stream_structure(<<>>, _in_band), do: :annexb
  defp h265_stream_structure(dcr, in_band), do: {if(in_band, do: :hev1, else: :hvc1), dcr}

  @spec framerate_to_float({integer(), integer()} | nil) :: float() | nil
  defp framerate_to_float({num, den}) when is_integer(num) and is_integer(den) and den > 0,
    do: num / den

  defp framerate_to_float(nil), do: nil

  @spec dimension(non_neg_integer() | nil) :: pos_integer() | nil
  defp dimension(size) when is_integer(size) and size > 0, do: size
  defp dimension(_absent), do: nil

  @spec framerate(float() | nil) :: {pos_integer(), pos_integer()} | nil
  defp framerate(fps) when is_number(fps) and fps > 0 do
    fps
    |> Ratio.new()
    |> then(&{&1.numerator, &1.denominator})
  end

  defp framerate(_fps), do: nil
end
