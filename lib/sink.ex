defmodule Membrane.MoQ.Sink do
  @moduledoc """
  Membrane Sink acting as a MoQ publisher.

  Connects to a MoQ relay server and publishes audio and video tracks
  to a single, configured broadcast.

  Pads can be added or removed at any time during the pipeline lifecycle. The
  catalog is republished on every track add/remove.
  """
  use Membrane.Sink

  require Membrane.Logger
  require Membrane.H264
  require Membrane.H265

  alias Membrane.{AAC, H264, H265, Opus}
  alias Membrane.MoQ.Native

  def_input_pad :input,
    availability: :on_request,
    accepted_format:
      any_of(
        AAC,
        Opus,
        %H264{stream_structure: ss} when H264.is_avc(ss),
        %H265{stream_structure: ss} when H265.is_hvc(ss)
      ),
    options: [
      track: [
        spec: String.t(),
        description: """
        Track name for this pad's stream, see `Track` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology.
        This will be the track name advertised by the hang/MSF catalog unless the stream format changes mid-stream.
        On a mid-stream format change, the current track is removed,
        and a new track is added with a different track name to avoid races with the catalog track.
        """
      ]
    ]

  def_options url: [
                spec: String.t(),
                description: "URL to the MoQ relay server."
              ],
              broadcast: [
                spec: String.t(),
                description:
                  "Broadcast path, see `Broadcast` at https://doc.moq.dev/concept/layer/moq-lite.html#terminology"
              ],
              container: [
                spec: :legacy,
                default: :legacy,
                description:
                  "Container format for media frames. Only :legacy is supported for now."
              ],
              disable_tls_verify?: [
                spec: boolean(),
                default: false,
                description:
                  "Whether to disable TLS verification when connecting to the relay. Useful for local development."
              ]

  defmodule State do
    @moduledoc false

    @type pad_state :: %{
            track: String.t(),
            track_resource: Native.track() | nil
          }

    @type t :: %__MODULE__{
            url: String.t(),
            container: :legacy,
            disable_tls_verify?: boolean(),
            session: Native.session(),
            broadcast: String.t(),
            broadcast_resource: Native.broadcast(),
            pads: %{Membrane.Pad.ref() => pad_state()}
          }

    @enforce_keys [:url, :broadcast, :container, :disable_tls_verify?]
    defstruct @enforce_keys ++
                [
                  session: nil,
                  pads: %{},
                  broadcast_resource: nil
                ]
  end

  @impl true
  def handle_init(_ctx, opts) do
    {[],
     %State{
       url: opts.url,
       broadcast: opts.broadcast,
       container: opts.container,
       disable_tls_verify?: opts.disable_tls_verify?
     }}
  end

  @impl true
  def handle_setup(ctx, %State{url: url, disable_tls_verify?: disable_tls_verify?} = state) do
    {:ok, session} = Native.setup_session(url, self(), disable_tls_verify?)

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.close_session(session)
    end)

    {[setup: :incomplete], %{state | session: session}}
  end

  @impl true
  def handle_info(:moq_connected, ctx, %State{session: session, broadcast: broadcast} = state) do
    {:ok, resource} = Native.open_broadcast(session, broadcast)

    Membrane.ResourceGuard.register(ctx.resource_guard, fn ->
      Native.close_broadcast(resource)
    end)

    {[setup: :complete], %{state | broadcast_resource: resource}}
  end

  @impl true
  def handle_info({:moq_setup_failed, reason}, _ctx, _state) do
    raise "MoQ session setup failed with reason: #{inspect(reason)}"
  end

  @impl true
  def handle_info({:moq_disconnected, reason}, _ctx, _state) do
    raise "MoQ session closed with reason: #{inspect(reason)}"
  end

  @impl true
  def handle_info(msg, _ctx, state) do
    Membrane.Logger.warning("Unknown message received: #{inspect(msg)}")
    {[], state}
  end

  @impl true
  def handle_pad_added(pad, %{pad_options: %{track: track}} = _ctx, state) do
    pad_state = %{
      track: track,
      track_resource: nil
    }

    {[], put_in(state.pads[pad], pad_state)}
  end

  @impl true
  def handle_pad_removed(pad, _ctx, state), do: {[], close_pad(pad, state)}

  @impl true
  def handle_stream_format(pad, fmt, _ctx, %State{broadcast_resource: broadcast_res} = state) do
    pad_state = Map.fetch!(state.pads, pad)

    track_fmt = track_format(fmt)

    track_res =
      pad_state.track_resource
      |> case do
        nil -> Native.add_track(broadcast_res, pad_state.track, track_fmt)
        existing -> Native.replace_track(existing, track_fmt)
      end
      |> case do
        {:ok, track_res} ->
          track_res

        {:ok, track_res, _new_track_name} ->
          track_res

        {:error, reason} ->
          raise "Failed to update pad's stream format, reason: #{inspect(reason)}"
      end

    {[], put_in(state.pads[pad], %{pad_state | track_resource: track_res})}
  end

  @impl true
  def handle_buffer(pad, %Membrane.Buffer{} = buffer, _ctx, state) do
    pad_state = Map.fetch!(state.pads, pad)
    timestamp_us = Membrane.Time.as_microseconds(buffer.pts, :round)

    if timestamp_us < 0 do
      raise "Received buffer with negative timestamp"
    end

    case Native.send_frame(
           pad_state.track_resource,
           timestamp_us,
           keyframe?(buffer),
           buffer.payload
         ) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "Failed to send frame to track #{inspect(pad_state.track)}: #{reason}"
    end

    {[], state}
  end

  @impl true
  def handle_end_of_stream(pad, _ctx, state) do
    state = close_pad(pad, state)
    {[], state}
  end

  @spec close_pad(Membrane.Pad.ref(), State.t()) :: State.t()
  defp close_pad(pad, state) do
    case Map.pop(state.pads, pad) do
      {%{track_resource: track_resource}, pads} ->
        :ok = Native.remove_track(track_resource)
        %{state | pads: pads}

      {nil, _pads} ->
        state
    end
  end

  @spec track_format(Membrane.StreamFormat.t()) :: Native.track_format()
  defp track_format(%H264{
         height: height,
         width: width,
         framerate: framerate,
         stream_structure: {tag, dcr}
       }) do
    dcr_parsed = Membrane.H264.DecoderConfigurationRecord.parse(dcr)

    {:h264,
     %{
       params: %Membrane.MoQ.Native.VideoTrackParams{
         width: width,
         height: height,
         framerate: framerate_to_float(framerate)
       },
       dcr: dcr,
       codec: %Membrane.MoQ.Native.H264Codec{
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

  defp track_format(%H265{
         height: height,
         width: width,
         framerate: framerate,
         stream_structure: {tag, dcr}
       }) do
    dcr_parsed = Membrane.H265.DecoderConfigurationRecord.parse(dcr)

    {:h265,
     %{
       params: %Membrane.MoQ.Native.VideoTrackParams{
         width: width,
         height: height,
         framerate: framerate_to_float(framerate)
       },
       dcr: dcr,
       codec: %Membrane.MoQ.Native.H265Codec{
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

  defp track_format(%AAC{profile: profile, sample_rate: sample_rate, channels: channels}),
    do:
      {:aac,
       %{profile: AAC.profile_to_aot_id(profile), sample_rate: sample_rate, channels: channels}}

  defp track_format(%Opus{channels: channels}),
    do: {:opus, %{sample_rate: 48_000, channels: channels}}

  @spec framerate_to_float({integer(), integer()} | nil) :: float()
  defp framerate_to_float({num, den}) when is_integer(num) and is_integer(den) and den > 0,
    do: num / den

  defp framerate_to_float(nil), do: 0.0

  @spec keyframe?(Membrane.Buffer.t()) :: boolean()
  defp keyframe?(%Membrane.Buffer{metadata: %{h264: %{key_frame?: kf}}}), do: kf
  defp keyframe?(%Membrane.Buffer{metadata: %{h265: %{key_frame?: kf}}}), do: kf
  defp keyframe?(%Membrane.Buffer{}), do: true
end
