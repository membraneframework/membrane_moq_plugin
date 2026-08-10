defmodule ExMoQ.Native.WebCodecs do
  @moduledoc """
  Track formats expressed in WebCodecs terms:
  codec-agnostic decoder parameters plus the per-codec fields
  a WebCodecs codec string is built from.
  """

  defmodule VideoTrackParams do
    @moduledoc "Codec-agnostic video parameters, a subset of WebCodecs' `VideoDecoderConfig`"
    @type t :: %__MODULE__{
            width: non_neg_integer() | nil,
            height: non_neg_integer() | nil,
            framerate: float() | nil
          }

    defstruct [:width, :height, :framerate]
  end

  defmodule AudioTrackParams do
    @moduledoc "Codec-agnostic audio parameters, a subset of WebCodecs' `AudioDecoderConfig`"
    @type t :: %__MODULE__{
            sample_rate: pos_integer(),
            channels: pos_integer()
          }
    @enforce_keys [:sample_rate, :channels]
    defstruct @enforce_keys
  end

  defmodule H264Codec do
    @moduledoc """
    Components of the WebCodecs `avc1.PPCCLL` codec string;
    `in_band: true` selects `avc3` (in-band parameter sets) instead.
    """
    @type t :: %__MODULE__{
            in_band: boolean(),
            profile: byte(),
            constraints: byte(),
            level: byte()
          }
    @enforce_keys [:in_band, :profile, :constraints, :level]
    defstruct @enforce_keys
  end

  defmodule H265Codec do
    @moduledoc """
    Components of the WebCodecs `hvc1` codec string;
    `in_band: true` selects `hev1` (in-band parameter sets) instead.
    """
    @type t :: %__MODULE__{
            in_band: boolean(),
            profile_space: byte(),
            profile_idc: byte(),
            profile_compatibility_flags: [byte()],
            tier_flag: boolean(),
            level_idc: byte(),
            constraint_flags: [byte()]
          }
    @enforce_keys [
      :in_band,
      :profile_space,
      :profile_idc,
      :profile_compatibility_flags,
      :tier_flag,
      :level_idc,
      :constraint_flags
    ]
    defstruct @enforce_keys
  end

  defmodule AACCodec do
    @moduledoc "Audio Object Type from the WebCodecs `mp4a.40.<profile>` codec string"
    @type t :: %__MODULE__{profile: byte()}
    @enforce_keys [:profile]
    defstruct @enforce_keys
  end

  defmodule VideoTrackFormat do
    @moduledoc """
    A video track's format: codec-agnostic parameters, the decoder
    configuration record (empty binary when absent) and the per-codec fields.
    """
    @type t :: %__MODULE__{
            params: VideoTrackParams.t(),
            description: binary(),
            codec: H264Codec.t() | H265Codec.t()
          }
    @enforce_keys [:params, :description, :codec]
    defstruct @enforce_keys
  end

  defmodule AudioTrackFormat do
    @moduledoc """
    An audio track's format: codec-agnostic parameters and the per-codec fields
    """
    @type t :: %__MODULE__{
            params: AudioTrackParams.t(),
            codec: AACCodec.t() | :opus
          }
    @enforce_keys [:params, :codec]
    defstruct @enforce_keys
  end
end
