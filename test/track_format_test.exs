defmodule Membrane.MoQ.TrackFormatTest do
  @moduledoc """
  Unit tests for the pure codec-translation layer shared by the Sink and Source.
  """
  use ExUnit.Case, async: true

  alias ExMoQ.Native.WebCodecs

  alias Membrane.{AAC, H264, H265, Opus, RemoteStream}
  alias Membrane.MoQ.TrackFormat

  describe "AAC" do
    test "round-trips through from/to stream_format" do
      fmt = %AAC{profile: :LC, sample_rate: 44_100, channels: 2}

      assert {:aac, %{params: params, codec: codec}} =
               native = TrackFormat.from_stream_format(fmt)

      assert %WebCodecs.AudioTrackParams{sample_rate: 44_100, channels: 2} = params
      assert %WebCodecs.AACCodec{profile: AAC.profile_to_aot_id(:LC)} == codec

      assert TrackFormat.to_stream_format(native) == fmt
    end

    test "media_type/1 is :audio" do
      assert TrackFormat.media_type({:aac, %{}}) == :audio
    end
  end

  describe "Opus" do
    test "round-trips through from/to stream_format" do
      fmt = %Opus{channels: 2, self_delimiting?: false}

      assert {:opus, %{params: params}} = native = TrackFormat.from_stream_format(fmt)
      assert %WebCodecs.AudioTrackParams{sample_rate: 48_000, channels: 2} = params

      assert TrackFormat.to_stream_format(native) == fmt
    end

    test "media_type/1 is :audio" do
      assert TrackFormat.media_type({:opus, %{}}) == :audio
    end
  end

  describe "H264" do
    for tag <- [:avc1, :avc3] do
      test "#{tag} round-trips through from/to stream_format" do
        dcr = avcc(_profile = 100, _constraints = 0, _level = 31)

        fmt = %H264{
          width: 1920,
          height: 1080,
          framerate: {30, 1},
          stream_structure: {unquote(tag), dcr}
        }

        native = TrackFormat.from_stream_format(fmt)
        assert {:h264, %{params: params, description: ^dcr, codec: codec}} = native
        assert %WebCodecs.VideoTrackParams{width: 1920, height: 1080, framerate: 30.0} = params
        # avc1 carries parameter sets out-of-band, avc3 in-band.
        assert codec.in_band == (unquote(tag) == :avc3)
        assert codec.profile == 100 and codec.level == 31

        assert TrackFormat.to_stream_format(native) == fmt
      end
    end

    test "a missing framerate maps to nil natively and back to nil" do
      dcr = avcc(100, 0, 31)
      fmt = %H264{width: 640, height: 480, framerate: nil, stream_structure: {:avc1, dcr}}

      assert {:h264, %{params: %{framerate: nil}}} = native = TrackFormat.from_stream_format(fmt)
      assert TrackFormat.to_stream_format(native) == fmt
    end

    # Upstream keys payload framing on description presence (WebCodecs semantics):
    # no description means Annex B on the wire, whatever the in_band flag says.
    for in_band <- [true, false] do
      test "a track without a description (in_band: #{in_band}) maps to Annex B" do
        native =
          {:h264,
           %{
             params: %{width: 1280, height: 720, framerate: 30.0},
             description: <<>>,
             codec: %{in_band: unquote(in_band), profile: 100, constraints: 0, level: 31}
           }}

        assert %H264{stream_structure: :annexb} = TrackFormat.to_stream_format(native)
      end
    end

    # The NIF decodes absent dimensions as nil; a remote catalog may still say 0.
    for absent <- [nil, 0] do
      test "dimensions absent from the catalog as #{inspect(absent)} map to nil" do
        native =
          {:h264,
           %{
             params: %{width: unquote(absent), height: unquote(absent), framerate: 30.0},
             description: avcc(100, 0, 31),
             codec: %{in_band: false, profile: 100, constraints: 0, level: 31}
           }}

        assert %H264{width: nil, height: nil} = TrackFormat.to_stream_format(native)
      end
    end

    test "small fps values map to positive numerator/denominator pairs" do
      native =
        {:h264,
         %{
           params: %{width: 1280, height: 720, framerate: 4.0e-4},
           description: avcc(100, 0, 31),
           codec: %{in_band: false, profile: 100, constraints: 0, level: 31}
         }}

      assert %H264{framerate: {num, den}} = TrackFormat.to_stream_format(native)
      assert num > 0 and den > 0
    end

    test "media_type/1 is :video" do
      assert TrackFormat.media_type({:h264, %{}}) == :video
    end
  end

  describe "H265" do
    for tag <- [:hvc1, :hev1] do
      test "#{tag} round-trips through from/to stream_format" do
        dcr = hvcc()

        fmt = %H265{
          width: 3840,
          height: 2160,
          framerate: {60, 1},
          stream_structure: {unquote(tag), dcr}
        }

        native = TrackFormat.from_stream_format(fmt)
        assert {:h265, %{params: params, description: ^dcr, codec: codec}} = native
        assert %WebCodecs.VideoTrackParams{width: 3840, height: 2160, framerate: 60.0} = params
        # hev1 carries parameter sets in-band, hvc1 out-of-band.
        assert codec.in_band == (unquote(tag) == :hev1)
        assert length(codec.profile_compatibility_flags) == 4
        assert length(codec.constraint_flags) == 6

        assert TrackFormat.to_stream_format(native) == fmt
      end
    end

    test "dimensions omitted from the catalog map to nil" do
      native =
        {:h265,
         %{
           params: %{width: nil, height: nil, framerate: 30.0},
           description: hvcc(),
           codec: %{in_band: false}
         }}

      assert %H265{width: nil, height: nil} = TrackFormat.to_stream_format(native)
    end

    test "a track without a description maps to Annex B" do
      native =
        {:h265,
         %{
           params: %{width: 1280, height: 720, framerate: 30.0},
           description: <<>>,
           codec: %{in_band: true}
         }}

      assert %H265{stream_structure: :annexb} = TrackFormat.to_stream_format(native)
    end

    test "media_type/1 is :video" do
      assert TrackFormat.media_type({:h265, %{}}) == :video
    end
  end

  describe ":unrecognized" do
    test "maps to a RemoteStream and an :unknown media type" do
      assert TrackFormat.to_stream_format(:unrecognized) == %RemoteStream{type: :packetized}
      assert TrackFormat.media_type(:unrecognized) == :unknown
    end
  end

  describe "keyframe?/2" do
    test "reads the parser metadata on video buffers" do
      for {codec_key, fmt} <- [{:h264, %H264{}}, {:h265, %H265{}}], kf <- [true, false] do
        buffer = %Membrane.Buffer{payload: <<>>, metadata: %{codec_key => %{key_frame?: kf}}}
        assert TrackFormat.keyframe?(buffer, fmt) == kf
      end
    end

    test "raises on video buffers without keyframe metadata" do
      for fmt <- [%H264{}, %H265{}] do
        buffer = %Membrane.Buffer{payload: <<>>, metadata: %{}}

        assert_raise RuntimeError, ~r/no key_frame\? metadata/, fn ->
          TrackFormat.keyframe?(buffer, fmt)
        end
      end
    end

    test "treats every audio buffer as a keyframe" do
      buffer = %Membrane.Buffer{payload: <<>>, metadata: %{}}

      for fmt <- [%AAC{}, %Opus{channels: 2}] do
        assert TrackFormat.keyframe?(buffer, fmt)
      end
    end
  end

  # Minimal valid avcC (H264 decoder configuration record) with no SPS/PPS, so
  # `Membrane.H264.DecoderConfigurationRecord.parse/1` recovers profile/level.
  defp avcc(profile, constraints, level) do
    <<1, profile, constraints, level, 0b111111::6, 3::2, 0b111::3, 0::5, 0::8>>
  end

  # Minimal valid hvcC (H265 decoder configuration record) with no NAL arrays.
  defp hvcc do
    <<1::8, _profile_space = 0::2, _tier_flag = 0::1, _profile_idc = 1::5,
      _profile_compatibility_flags = 0x60000000::32,
      _constraint_indicator_flags = 0x900000000000::48, _level_idc = 93::8, 0b1111::4,
      _min_spatial_segmentation_idc = 0::12, 0b111111::6, _parallelism_type = 0::2, 0b111111::6,
      _chroma_format_idc = 1::2, 0b11111::5, _bit_depth_luma_minus8 = 0::3, 0b11111::5,
      _bit_depth_chroma_minus8 = 0::3, _avg_frame_rate = 0::16, _constant_frame_rate = 0::2,
      _num_temporal_layers = 1::3, _temporal_id_nested = 0::1, _length_size_minus_one = 3::2,
      _num_of_arrays = 0::8>>
  end
end
