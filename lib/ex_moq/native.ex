defmodule ExMoQ.Native do
  @moduledoc """
  Elixir bindings to moq-net's native Rust API
  """
  use Rustler, otp_app: :membrane_moq_plugin, crate: "ex_moq"

  @type session :: reference()
  @type broadcast_producer :: reference()
  @type broadcast_consumer :: reference()

  @typedoc """
  Name of a track within a broadcast,
  see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology.
  """
  @type track :: String.t()

  @typedoc "Wire container a track's frames are encapsulated in."
  @type container :: :legacy | :loc

  @typedoc """
  Caller-chosen opaque integer identifying a track subscription,
  echoed back in the subscription's messages.
  """
  @type token :: integer()

  @typedoc """
  Format and wire container of a track, as advertised in the broadcast's catalog.
  The container is `nil` when the catalog advertises one this library
  does not recognize.
  """
  @type rendition :: {track_format(), container() | nil}

  @typedoc "Reason a broadcast consumer closed."
  @type close_reason :: :ended | :not_announced | :crashed | {:catalog_error, String.t()}

  defmodule VideoTrackParams do
    @moduledoc "Codec-agnostic parameters of a `hang` video track"
    @type t :: %__MODULE__{
            width: non_neg_integer() | nil,
            height: non_neg_integer() | nil,
            framerate: float() | nil
          }

    defstruct [:width, :height, :framerate]
  end

  defmodule AudioTrackParams do
    @moduledoc "Codec-agnostic parameters of a `hang` audio track"
    @type t :: %__MODULE__{
            sample_rate: pos_integer(),
            channels: pos_integer()
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
  The QUIC handshake completes asynchronously. Sends to `pid`:
    * `:moq_connected`
        once the session is up
    * `{:moq_setup_failed, reason :: String.t()}`
        if establishing the connection fails, e.g. due to timeout
    * `{:moq_disconnected, reason :: String.t()}`
        if the session terminates unexpectedly
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
  Closes the broadcast: flushes every track's buffered frames,
  finishes the catalog, then aborts the broadcast. Idempotent.

  This is the only graceful shutdown path.
  A broadcast producer that is garbage-collected
  without an explicit close is aborted instead.
  Buffered frames are discarded and consumers observe an error rather than a clean end.
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

  @typedoc """
  Error returned by producer NIFs when an earlier call panicked mid-operation,
  leaving the broadcast producer unusable.
  """
  @type poisoned :: :producer_poisoned

  @doc """
  Adds a track of any supported codec to the given broadcast.
  Frames are then sent with `send_frame/5` under the same track name.

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
        ) :: :ok | {:error, :track_already_exists | poisoned() | (reason :: String.t())}
  def add_track(_broadcast_producer, _track, _format, _priority, _container, _latency_ns),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Swaps a live track's format in place.
  The catalog rendition is replaced under the same track name
  and the underlying moq track keeps flowing,
  so consumers observe the change through a catalog update.

  The track's media kind (audio/video) cannot change.
  """
  @spec update_track(broadcast_producer(), track(), track_format()) ::
          :ok | {:error, :unknown_track | :kind_mismatch | poisoned() | (reason :: String.t())}
  def update_track(_broadcast_producer, _track, _format),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Sends a frame to the broadcast's track named `track`.

  `timestamp_ns` is the presentation timestamp in nanoseconds.

  `keyframe?` must be `true` for IDR frames on video tracks (triggers a new MoQ group).
  For audio tracks pass `true` for every frame.
  """
  @spec send_frame(broadcast_producer(), track(), non_neg_integer(), boolean(), binary()) ::
          :ok
          | :missing_keyframe
          | {:error, :unknown_track | poisoned() | (reason :: String.t())}
  def send_frame(_broadcast_producer, _track, _timestamp_ns, _keyframe?, _data),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the broadcast's track named `track`, removing its rendition from the
  catalog and finishing the underlying moq-lite track. Idempotent.
  """
  @spec remove_track(broadcast_producer(), track()) :: :ok | {:error, poisoned()}
  def remove_track(_broadcast_producer, _track),
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

  Sends to `pid`:
    * `{:moq_broadcast_ready, path :: String.t()}`
        once the broadcast is announced and its catalog is subscribed
    * `{:moq_broadcast_closed, path :: String.t(), reason :: close_reason()}`
        when the broadcast ends, errors, or the session closes underneath it
    * `{:moq_catalog, path :: String.t(), renditions :: [{track(), rendition()}]}`
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
  pass the value advertised for the track in the `:moq_catalog` message.

  `token` (see `t:token/0`) lets the caller route this track's messages
  to the originating subscription.
  Keep tokens unique across all broadcast consumers reporting to the same pid.
  Don't reuse tokens.

  The subscription is immediate:
  a track the broadcast does not carry fails asynchronously with `:moq_track_error`.
  Waiting until the catalog advertises a track is the caller's job (watch `:moq_catalog`).

  Sends to the consumer's `pid`:
    * `{:moq_frame, token(), binary(), timestamp_ns :: non_neg_integer(), keyframe? :: boolean()}`
        for every received frame
    * `{:moq_track_finished, token()}`
        when the wire track finishes
    * `{:moq_track_error, token(), reason :: String.t()}`
        when the subscription fails on the native side
        while the track may still be advertised in the catalog
  """
  @spec subscribe_track(broadcast_consumer(), track(), container() | nil, token(), 0..255) ::
          :ok | {:error, :unrecognized_container | :consumer_closed}
  def subscribe_track(_broadcast_consumer, _track, _container, _token, _priority),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Cancels the subscription identified by `token`.
  No `{:moq_track_finished, ...}` is sent for a cancelled track. Idempotent.
  """
  @spec unsubscribe_track(broadcast_consumer(), token()) :: :ok
  def unsubscribe_track(_broadcast_consumer, _token),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Tears down a broadcast consumer and all its track subscriptions. Idempotent.

  A broadcast consumer that is garbage-collected without an explicit close
  is aborted instead, and a final `{:moq_broadcast_closed, path, reason}`
  reports `:crashed` rather than a clean `:ended`.
  """
  @spec close_broadcast_consumer(broadcast_consumer()) :: :ok
  def close_broadcast_consumer(_broadcast_consumer), do: :erlang.nif_error(:nif_not_loaded)
end
