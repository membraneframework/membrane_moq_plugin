defmodule Membrane.MoQ.Native do
  @moduledoc false
  use Rustler, otp_app: :membrane_moq_plugin, crate: "moq"

  @doc """
  Connects to a MoQ relay and prepares a session.

  Sends `:moq_connected` to `pid` once the QUIC handshake completes,
  or `:moq_disconnected` if the session fails / is closed.
  """
  @spec setup_session(String.t(), pid(), boolean()) ::
          {:ok, session :: reference()} | {:error, reason :: String.t()}
  def setup_session(_url, _pid, _disable_tls_verify?),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down the session. Idempotent.
  """
  @spec close_session(reference()) :: :ok
  def close_session(_session),
    do: :erlang.nif_error(:nif_not_loaded)

  # Broadcast NIFs

  @doc """
  Opens a broadcast on the session and creates its hang + MSF catalog tracks.
  """
  @spec open_broadcast(reference(), String.t()) ::
          {:ok, broadcast_resource :: reference()} | {:error, reason :: String.t()}
  def open_broadcast(_session, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the broadcast, finishing the catalog and aborting in-flight tracks.
  """
  @spec close_broadcast(reference()) :: :ok
  def close_broadcast(_broadcast_resource),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec add_h264_track(reference(), String.t(), VideoTrackParams.t(), String.t(), H264Codec.t()) ::
          {:ok, track_res :: reference()} | {:error, reason :: String.t()}
  def add_h264_track(_broadcast_res, _track, _video_params, _dcr, _codec),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec add_h265_track(reference(), String.t(), VideoTrackParams.t(), String.t(), H265Codec.t()) ::
          {:ok, track_res :: reference()} | {:error, reason :: String.t()}
  def(add_h265_track(_broadcast_res, _track, _video_params, _dcr, _codec),
    do: :erlang.nif_error(:nif_not_loaded)
  )

  @spec add_aac_track(reference(), String.t(), byte(), non_neg_integer(), non_neg_integer()) ::
          {:ok, track_res :: reference()} | {:error, reason :: String.t()}
  def(add_aac_track(_broadcast_res, _track, _profile, _sample_rate, _channels),
    do: :erlang.nif_error(:nif_not_loaded)
  )

  @spec add_opus_track(reference(), String.t(), non_neg_integer(), Membrane.Opus.channels_t()) ::
          {:ok, track_res :: reference()} | {:error, reason :: String.t()}
  def add_opus_track(_broadcast_res, _track, _sample_rate, _channels),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Sends a frame to a track.

  `timestamp_us` is the presentation timestamp in microseconds. `keyframe?`
  must be `true` for IDR frames on video tracks (triggers a new MoQ group);
  for audio tracks pass `true` for every frame.
  """
  @spec send_frame(reference(), integer(), boolean(), binary()) :: :ok # | no_return() __jm__
  def send_frame(_track_res, _timestamp_us, _keyframe?, _data),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the track, removing its rendition from the catalog and finishing the
  underlying moq-lite track. Idempotent.
  """
  @spec remove_track(reference()) :: :ok
  def remove_track(_track_res),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  TODO
  """
  def start_subscriber(_url, _broadcast, _track, _pid),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  TODO
  """
  def stop_subscriber(_resource), do: :erlang.nif_error(:nif_not_loaded)
end

defmodule Membrane.MoQ.Native.VideoTrackParams do
  @moduledoc false
  @type t :: %__MODULE__{width: non_neg_integer(), height: non_neg_integer(), framerate: float()}
  @enforce_keys [:width, :height, :framerate]
  defstruct @enforce_keys
end

defmodule Membrane.MoQ.Native.H264Codec do
  @moduledoc false
  @type t :: %__MODULE__{inline: boolean(), profile: byte(), constraints: byte(), level: byte()}
  @enforce_keys [:inline, :profile, :constraints, :level]
  defstruct @enforce_keys
end

defmodule Membrane.MoQ.Native.H265Codec do
  @moduledoc false
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
