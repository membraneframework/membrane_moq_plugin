defmodule ExMoQ.Native.VideoTrackFormat do
  @moduledoc """
  A video track's format: codec-agnostic parameters, the decoder
  configuration record (empty binary when absent) and the per-codec fields.
  """

  alias ExMoQ.Native.WebCodecs

  @type t :: %__MODULE__{
          params: WebCodecs.VideoTrackParams.t(),
          description: binary(),
          codec: WebCodecs.H264Codec.t() | WebCodecs.H265Codec.t()
        }
  @enforce_keys [:params, :description, :codec]
  defstruct @enforce_keys
end
