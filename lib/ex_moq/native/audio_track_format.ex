defmodule ExMoQ.Native.AudioTrackFormat do
  @moduledoc """
  An audio track's format: codec-agnostic parameters and the per-codec fields
  """

  alias ExMoQ.Native.WebCodecs

  @type t :: %__MODULE__{
          params: WebCodecs.AudioTrackParams.t(),
          codec: WebCodecs.AACCodec.t() | :opus
        }
  @enforce_keys [:params, :codec]
  defstruct @enforce_keys
end
