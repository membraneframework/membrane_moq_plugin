defmodule ExMoQ.Native do
  @moduledoc """
  Elixir bindings to moq-net's native Rust API
  """
  use Rustler, otp_app: :membrane_moq_plugin, crate: "ex_moq"

  @type track_resource :: reference()
  @type session :: reference()
  @type broadcast_producer :: reference()
  @type broadcast_consumer :: reference()

  @typedoc """
  Name of a track within a broadcast,
  see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology.
  """
  @type track :: String.t()

  @typedoc "Wire container a published track's frames are encapsulated in."
  @type container :: :legacy | :loc

  @typedoc """
  Wire container of a consumed track's frames,
  as advertised in the broadcast's catalog.
  """
  @type wire_container :: :legacy | :loc | :unrecognized

  @type rendition :: {track_format(), wire_container()}

  defmodule VideoTrackParams do
    @moduledoc "Codec-agnostic parameters of a `hang` video track"
    @type t :: %__MODULE__{
            width: non_neg_integer() | nil,
            height: non_neg_integer() | nil,
            framerate: float() | nil
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
  Connect to a MoQ relay server and prepare a bidirectional session.

  Builds the origins synchronously so subsequent NIFs can
  create broadcast producers and consumers immediately.
  The QUIC handshake completes asynchronously:
  - `:moq_connected` is sent to `pid` once the session is up.
  - `{:moq_setup_failed, reason :: String.t()}` is sent if establishing the
    connection fails, e.g. due to timeout.
  - `{:moq_disconnected, reason :: String.t()}` is sent if the session terminates unexpectedly.
  """
  @spec create_session(String.t(), pid(), boolean()) ::
          {:ok, session()} | {:error, reason :: String.t()}
  def create_session(_url, _pid, _disable_tls_verify?),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down the session. Idempotent.

  Only the session's network task is stopped.
  Don't reuse a closed session.
  """
  @spec close_session(session()) :: :ok
  def close_session(_session),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Opens a broadcast for publishing on the session and creates its hang + MSF catalog tracks.
  """
  @spec create_broadcast_producer(session(), String.t()) ::
          {:ok, broadcast_producer()} | {:error, reason :: String.t()}
  def create_broadcast_producer(_session, _path),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the broadcast, finishing the catalog and aborting in-flight tracks.
  """
  @spec close_broadcast_producer(broadcast_producer()) :: :ok
  def close_broadcast_producer(_broadcast_producer),
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

  Arguments:
    * `broadcast_producer` - the target broadcast's resource,
      the result of calling `create_broadcast_producer/2`.
    * `track` - name of the track.
    * `format` - codec format, see `t:track_format/0`.
    * `priority` - the track's delivery priority:
      under congestion, tracks with a higher value are sent first.
    * `container` - the wire container, see `t:container/0`.
      Stamped into the track's catalog rendition so consumers pick the matching parser.
    * `latency_ns` - how long the track buffers frames
      before writing them to the wire, in nanoseconds (`0` writes each frame immediately).
  """
  @spec add_track(
          broadcast_producer(),
          track(),
          track_format(),
          0..255,
          container(),
          non_neg_integer()
        ) :: {:ok, track_resource()} | {:error, reason :: String.t()}
  def add_track(_broadcast_producer, _track, _format, _priority, _container, _latency_ns),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Swaps a live track's format in place.
  The catalog rendition is replaced under the same track name
  and the underlying moq track keeps flowing,
  so consumers observe the change through a catalog update.

  The track's media kind (audio/video) cannot change.
  """
  @spec update_track(track_resource(), track_format()) :: :ok | {:error, reason :: String.t()}
  def update_track(_track_res, _format),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Sends a frame to a track.

  `timestamp_ns` is the presentation timestamp in nanoseconds.

  `keyframe?` must be `true` for IDR frames on video tracks (triggers a new MoQ group).
  For audio tracks pass `true` for every frame.

  Returns:
     * `:ok` when the write succeeds
     * `:moq_missing_keyframe` when the write failed because
       a MoQ group hasn't opened yet and the frame is not a keyframe.
     * `{:error, reason}` when the write failed for another reason (e.g. track closed).
  """
  @spec send_frame(track_resource(), non_neg_integer(), boolean(), binary()) ::
          :ok | :moq_missing_keyframe | {:error, reason :: String.t()}
  def send_frame(_track_res, _timestamp_ns, _keyframe?, _data),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the track, removing its rendition from the catalog
  and finishing the underlying moq-lite track. Idempotent.

  A track resource that is garbage-collected without an explicit remove
  is retired the same way.
  """
  @spec remove_track(track_resource()) :: :ok
  def remove_track(_track_res),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Prepares to consume tracks of the broadcast at `path` on the session.

  Multiple broadcast consumers may share one session.
  Each one independently waits for its broadcast to be announced.
  Subscribe to individual tracks with `subscribe_track/5`.

  The catalog format is detected from the broadcast name's filename-style suffix:
    * `.msf`  -> MSF
    * `.hang` -> hang
    * default -> hang

  `latency_ns` is how long each track buffers received frames before emitting
  them, in nanoseconds, trading delay for resilience to jitter and reordering.

  Sends the following messages to `pid`:
    * `{:moq_broadcast_ready, path :: String.t()}`
        once the broadcast is announced and its catalog is subscribed
    * `{:moq_broadcast_closed, path :: String.t(), reason :: String.t()}`
        when the broadcast ends, errors, or the session closes underneath it
    * `{:moq_catalog, path :: String.t(),
          renditions :: [{track(), rendition()}]}`
        with the full catalog snapshot, once the broadcast is announced
        and again on every catalog update.
        Diffing consecutive snapshots is the caller's job:
        a rendition replaced in place arrives as a changed entry
        under the same name, and the wire track of a live subscription keeps flowing.
        Unsubscribing on such a change is also the caller's call to make.
  """
  @spec create_broadcast_consumer(session(), String.t(), pid(), non_neg_integer()) ::
          {:ok, broadcast_consumer()}
  def create_broadcast_consumer(_session, _path, _pid, _latency_ns),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Subscribes to `track` within the broadcast consumed by the given consumer.

  `container` selects the parser for the track's frames:
  echo the value advertised for the track in the `:moq_catalog` message.
  An unusable container is rejected synchronously with `{:error, reason}`.

  `token` is a caller-chosen opaque integer echoed back in this track's messages
  so the caller can route them to the originating subscription.
  Keep tokens unique across all broadcast consumers reporting to the same pid.

  The subscription is immediate:
  a track the broadcast does not carry fails asynchronously with `:moq_track_error`.
  Waiting until the catalog advertises a track is the caller's job (watch `:moq_catalog`).

  Sends to the consumer's `pid`:
    * `{:moq_frame, token :: integer(), payload :: binary(), timestamp_ns :: integer(), keyframe? :: boolean()}`
      for every received frame
    * `{:moq_track_ended, token :: integer()}` when the wire track finishes
    * `{:moq_track_error, token :: integer(), reason :: String.t()}`
      when the subscription fails on the native side
      while the track may still be advertised in the catalog
  """
  @spec subscribe_track(broadcast_consumer(), track(), wire_container(), integer(), 0..255) ::
          :ok | {:error, reason :: String.t()}
  def subscribe_track(_broadcast_consumer, _track, _container, _token, _priority),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Cancels the subscription identified by `token`.
  No `{:moq_track_ended, ...}` is sent for a cancelled track. Idempotent.
  """
  @spec unsubscribe_track(broadcast_consumer(), integer()) :: :ok
  def unsubscribe_track(_broadcast_consumer, _token),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down a broadcast consumer and all its track subscriptions. Idempotent.
  """
  @spec close_broadcast_consumer(broadcast_consumer()) :: :ok
  def close_broadcast_consumer(_broadcast_consumer), do: :erlang.nif_error(:nif_not_loaded)
end
