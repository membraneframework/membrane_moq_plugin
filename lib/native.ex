defmodule Membrane.MoQ.Native do
  @moduledoc """
  Elixir bindings to moq-net's native Rust API
  """
  use Rustler, otp_app: :membrane_moq_plugin, crate: "moq"

  @type track :: reference()
  @type broadcast :: reference()
  @type session :: reference()

  defmodule VideoTrackParams do
    @moduledoc "Parameters that describe a `hang` video track."
    @type t :: %__MODULE__{
            width: non_neg_integer(),
            height: non_neg_integer(),
            framerate: float()
          }
    @enforce_keys [:width, :height, :framerate]
    defstruct @enforce_keys
  end

  defmodule H264Codec do
    @moduledoc "H264 parameters required by `hang`"
    @type t :: %__MODULE__{inline: boolean(), profile: byte(), constraints: byte(), level: byte()}
    @enforce_keys [:inline, :profile, :constraints, :level]
    defstruct @enforce_keys
  end

  defmodule H265Codec do
    @moduledoc "H265 parameters required by `hang`"
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

  @doc """
  Connect to a MoQ relay server and prepare the session.

  Builds the origin synchronously so subsequent NIFs can publish broadcasts
  immediately. The QUIC handshake completes asynchronously
  - `:moq_connected` is sent to `pid` once the session is up.
  - `{:moq_setup_failed, reason :: String.t()}` is sent if establishing the connection fails.
  - `{:moq_disconnected, reason :: String.t()}` is sent if the session terminates unexpectedly.
  """
  @spec setup_session(String.t(), pid(), boolean()) ::
          {:ok, session()} | {:error, reason :: String.t()}
  def setup_session(_url, _pid, _disable_tls_verify?),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down the session. Idempotent.
  """
  @spec close_session(session()) :: :ok
  def close_session(_session),
    do: :erlang.nif_error(:nif_not_loaded)

  # Broadcast NIFs

  @doc """
  Opens a broadcast on the session and creates its hang + MSF catalog tracks.
  """
  @spec open_broadcast(session(), String.t()) ::
          {:ok, broadcast()} | {:error, reason :: String.t()}
  def open_broadcast(_session, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the broadcast, finishing the catalog and aborting in-flight tracks.
  """
  @spec close_broadcast(broadcast()) :: :ok
  def close_broadcast(_broadcast_resource),
    do: :erlang.nif_error(:nif_not_loaded)

  @type track_format() ::
          {:h264, %{params: VideoTrackParams.t(), dcr: binary(), codec: H264Codec.t()}}
          | {:h265, %{params: VideoTrackParams.t(), dcr: binary(), codec: H265Codec.t()}}
          | {:aac,
             %{profile: byte(), sample_rate: non_neg_integer(), channels: non_neg_integer()}}
          | {:opus, %{sample_rate: non_neg_integer(), channels: Membrane.Opus.channels_t()}}

  @doc """
  Adds a track of any supported codec to the given broadcast and returns a track
  resource that can be used to send frames.

  The first argument is the target broadcast's resource, the result of calling `open_broadcast/2`.
  The second argument is the name of the track.
  The third is the codec format, see `t:track_format/0`.
  """
  @spec add_track(broadcast(), String.t(), track_format()) ::
          {:ok, track()} | {:error, reason :: String.t()}
  def add_track(_broadcast_res, _track, _format),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Replaces a live track with one carrying `format`, published on a brand-new moq track.

  Returns the new track resource along with the name of the newly generated moq track.
  """
  @spec replace_track(track(), track_format()) ::
          {:ok, track(), name :: String.t()} | {:error, reason :: String.t()}
  def replace_track(_old_track_res, _format),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Sends a frame to a track.

  `timestamp_us` is the presentation timestamp in microseconds. `keyframe?`
  must be `true` for IDR frames on video tracks (triggers a new MoQ group);
  for audio tracks pass `true` for every frame.

  Returns `:ok`, or `{:error, reason}` if the write fails (e.g. track closed).
  """
  @spec send_frame(track(), pos_integer(), boolean(), binary()) ::
          :ok | {:error, reason :: String.t()}
  def send_frame(_track_res, _timestamp_us, _keyframe?, _data),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the track, removing its rendition from the catalog and finishing the
  underlying moq-lite track. Idempotent.
  """
  @spec remove_track(track()) :: :ok
  def remove_track(_track_res),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Connects to a MoQ relay and subscribes to `track` inside `broadcast`.

  Sends the following messages to `pid`:
    * `:moq_connected` once the broadcast is announced and the subscription is open
    * `{:moq_frame, payload :: binary(), timestamp_us :: integer(), keyframe? :: boolean()}`
      for every received frame
    * `{:moq_setup_failed, reason :: String.t()}` if connection or broadcast
      discovery fails
    * `{:moq_disconnected, reason :: String.t()}` when the track or session ends
  """
  @spec start_subscriber(String.t(), String.t(), String.t(), pid(), boolean()) ::
          {:ok, subscriber :: reference()} | {:error, reason :: String.t()}
  def start_subscriber(_url, _broadcast, _track, _pid, _disable_tls_verify?),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down a subscriber. Idempotent.
  """
  @spec stop_subscriber(reference()) :: :ok
  def stop_subscriber(_resource), do: :erlang.nif_error(:nif_not_loaded)
end
