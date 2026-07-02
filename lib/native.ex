defmodule Membrane.MoQ.Native do
  @moduledoc """
  Elixir bindings to moq-net's native Rust API
  """
  use Rustler, otp_app: :membrane_moq_plugin, crate: "moq"

  @type track :: reference()
  @type broadcast :: reference()
  @type session :: reference()

  defmodule VideoTrackParams do
    @moduledoc "Codec-agnostic parameters of a `hang` video track"
    @type t :: %__MODULE__{
            width: non_neg_integer(),
            height: non_neg_integer(),
            framerate: float()
          }
    @enforce_keys [:width, :height, :framerate]
    defstruct @enforce_keys
  end

  defmodule AudioTrackParams do
    @moduledoc "Codec-agnostic parameters of a `hang` audio track"
    @type t :: %__MODULE__{
            sample_rate: non_neg_integer(),
            channels: non_neg_integer()
          }
    @enforce_keys [:sample_rate, :channels]
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

  defmodule AACCodec do
    @moduledoc "AAC parameters required by `hang`"
    @type t :: %__MODULE__{profile: byte()}
    @enforce_keys [:profile]
    defstruct @enforce_keys
  end

  @doc """
  Connect to a MoQ relay server and prepare the session.

  Builds the origin synchronously so subsequent NIFs can publish broadcasts immediately.
  The QUIC handshake completes asynchronously
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

  @typedoc """
  Codec configuration mirroring `hang`'s catalog config.
  """
  @type track_format() ::
          {:h264, %{params: VideoTrackParams.t(), description: binary(), codec: H264Codec.t()}}
          | {:h265, %{params: VideoTrackParams.t(), description: binary(), codec: H265Codec.t()}}
          | {:aac, %{params: AudioTrackParams.t(), codec: AACCodec.t()}}
          | {:opus, %{params: AudioTrackParams.t()}}
          | :unrecognized

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

  `timestamp_us` is the presentation timestamp in microseconds.

  `keyframe?` must be `true` for IDR frames on video tracks (triggers a new MoQ group).
  For audio tracks pass `true` for every frame.

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

  @type subscriber :: reference()

  @doc """
  Connects to a MoQ relay and prepares to subscribe to tracks of `broadcast`.

  A single shared QUIC connection serves every track; subscribe to individual
  tracks with `subscribe_track/3`.

  `latency_ns` is how long each track buffers received frames before emitting
  them, in nanoseconds, trading delay for resilience to jitter and reordering.

  Sends the following messages to `pid`:
    * `:moq_connected`
        once the broadcast is announced
    * `{:moq_setup_failed, reason :: String.t()}`
        if connection or broadcast discovery fails
    * `{:moq_disconnected, reason :: String.t()}`
        when the session terminates unexpectedly
    * `{:moq_track_added, name :: String.t(), format :: track_format()}`
        when the catalog advertises a track
    * `{:moq_track_removed, name :: String.t()}`
        when the catalog drops a track
  """
  @spec start_subscriber(String.t(), String.t(), pid(), boolean(), non_neg_integer()) ::
          {:ok, subscriber()} | {:error, reason :: String.t()}
  def start_subscriber(_url, _broadcast, _pid, _disable_tls_verify?, _latency_ns),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Subscribes to `track` within the broadcast opened by `start_subscriber/4`.

  `token` is a caller-chosen opaque integer echoed back in this track's messages
  so the caller can route them to the originating subscription.
  Sends to the subscriber's `pid`:
    * `{:moq_track_format, token :: integer(), format :: track_format()}` once the catalog
      advertises the track, before any frame
    * `{:moq_frame, token :: integer(), payload :: binary(), timestamp_us :: integer(), keyframe? :: boolean()}`
      for every received frame
    * `{:moq_track_ended, token :: integer(), reason :: String.t()}` when the
      track ends cleanly or errors
  """
  @spec subscribe_track(subscriber(), String.t(), integer()) :: :ok
  def subscribe_track(_subscriber, _track, _token),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Cancels the subscription identified by `token`.
  No `{:moq_track_ended, ...}` is sent for a cancelled track. Idempotent.
  """
  @spec unsubscribe_track(subscriber(), integer()) :: :ok
  def unsubscribe_track(_subscriber, _token),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down a subscriber and all its track subscriptions. Idempotent.
  """
  @spec stop_subscriber(subscriber()) :: :ok
  def stop_subscriber(_subscriber), do: :erlang.nif_error(:nif_not_loaded)
end
