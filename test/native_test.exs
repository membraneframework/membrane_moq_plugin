defmodule ExMoQ.NativeTest do
  @moduledoc """
  Membrane-agnostic tests of the raw `ExMoQ.Native` API against a real relay.
  """

  use ExUnit.Case, async: true

  alias ExMoQ.Native
  alias Membrane.MoQ.Test.Relay

  @moduletag :integration

  @track "video"

  setup_all do
    [relay: Relay.ensure!()]
  end

  setup do
    [broadcast: "membrane/native-#{System.unique_integer([:positive])}"]
  end

  test "send_frame errors after the broadcast producer is closed", %{
    broadcast: broadcast,
    relay: relay
  } do
    {:ok, session} = Native.create_session(relay.url, self(), relay.disable_tls_verify?)
    assert_receive :moq_connected, 10_000

    {:ok, producer} = Native.create_broadcast_producer(session, broadcast)

    {:ok, track} = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)
    assert :ok = Native.send_frame(track, 0, true, "frame")

    :ok = Native.close_broadcast_producer(producer)
    assert {:error, _reason} = Native.send_frame(track, 40_000_000, true, "frame")

    :ok = Native.close_session(session)
  end

  test "unsubscribe_track stops a subscription made before the track was advertised", %{
    broadcast: broadcast,
    relay: relay
  } do
    {:ok, pub_session} = Native.create_session(relay.url, self(), relay.disable_tls_verify?)
    {:ok, sub_session} = Native.create_session(relay.url, self(), relay.disable_tls_verify?)
    assert_receive :moq_connected, 10_000
    assert_receive :moq_connected, 10_000

    {:ok, producer} = Native.create_broadcast_producer(pub_session, broadcast)
    {:ok, consumer} = Native.create_broadcast_consumer(sub_session, broadcast, self(), 0)
    assert_receive {:moq_broadcast_ready, ^broadcast}, 10_000

    # Subscribe before the track exists in the catalog, so the subscription
    # parks in the consumer's pending set and its pump is spawned only once
    # the catalog advertises the track.
    early_token = 1
    :ok = Native.subscribe_track(consumer, @track, early_token, 60)

    {:ok, track} = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)

    assert_receive {:moq_track_format, ^early_token, {:h264, _config}}, 10_000
    :ok = Native.send_frame(track, 0, true, "before")
    assert_receive {:moq_frame, ^early_token, "before", _timestamp, true}, 10_000

    :ok = Native.unsubscribe_track(consumer, early_token)

    # Consumer commands are processed in order, so once this later
    # subscription is acknowledged the unsubscribe has been handled too.
    late_token = 2
    :ok = Native.subscribe_track(consumer, @track, late_token, 60)
    assert_receive {:moq_track_format, ^late_token, {:h264, _config}}, 10_000

    :ok = Native.send_frame(track, 40_000_000, true, "after")

    # The frame reaching the live subscription proves it made the round trip;
    # the unsubscribed one must stay silent.
    assert_receive {:moq_frame, ^late_token, "after", _timestamp, true}, 10_000
    refute_receive {:moq_frame, ^early_token, _payload, _timestamp, _keyframe?}, 500

    :ok = Native.close_broadcast_consumer(consumer)
    :ok = Native.close_broadcast_producer(producer)
    :ok = Native.close_session(sub_session)
    :ok = Native.close_session(pub_session)
  end

  defp h264_format() do
    {:h264,
     %{
       params: %Native.VideoTrackParams{width: 1280, height: 720, framerate: 30.0},
       description: <<>>,
       codec: %Native.H264Codec{inline: true, profile: 66, constraints: 0, level: 30}
     }}
  end
end
