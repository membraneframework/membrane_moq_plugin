defmodule ExMoQ.NativeTest do
  @moduledoc """
  Membrane-agnostic tests of the raw `ExMoQ.Native` API against a real relay.
  """

  use ExUnit.Case, async: true

  alias ExMoQ.Native
  alias ExMoQ.Native.{VideoTrackFormat, WebCodecs}
  alias Membrane.MoQ.Test.Relay

  @moduletag :integration

  @track "video"

  setup_all do
    [relay: Relay.start_supervised!()]
  end

  setup do
    [broadcast: "membrane/native-#{System.unique_integer([:positive])}"]
  end

  test "subscribing to a track the broadcast does not carry fails with :moq_track_error", %{
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

    ghost_token = 1
    :ok = Native.subscribe_track(consumer, "ghost", ghost_token, 60)
    assert_receive {:moq_track_error, ^ghost_token, _reason}, 10_000

    :ok = Native.close_broadcast_consumer(consumer)
    :ok = Native.close_broadcast_producer(producer)
    :ok = Native.close_session(sub_session)
    :ok = Native.close_session(pub_session)
  end

  test "send_frame before the first keyframe reports :missing_keyframe", %{
    broadcast: broadcast,
    relay: relay
  } do
    {:ok, session} = Native.create_session(relay.url, self(), relay.disable_tls_verify?)
    assert_receive :moq_connected, 10_000

    {:ok, producer} = Native.create_broadcast_producer(session, broadcast)
    :ok = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)

    assert :missing_keyframe =
             Native.send_frame(producer, @track, 0, false, "delta before any group")

    assert :ok = Native.send_frame(producer, @track, 0, true, "keyframe opens a group")
    assert :ok = Native.send_frame(producer, @track, 40_000_000, false, "delta within the group")

    :ok = Native.close_broadcast_producer(producer)
    :ok = Native.close_session(session)
  end

  test "update_track on a removed track errors instead of touching the catalog", %{
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

    :ok = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)
    await_renditions(broadcast, &match?([{@track, %VideoTrackFormat{}}], &1))

    # A name the broadcast already carries cannot be added again.
    assert {:error, _reason} = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)

    :ok = Native.remove_track(producer, @track)
    await_renditions(broadcast, &(&1 == []))

    # An update under the removed name must not resurrect its rendition.
    assert {:error, _reason} = Native.update_track(producer, @track, h264_format())
    refute_receive {:moq_catalog, _path, _renditions}, 500

    # The name is free for reuse, and updates then target the successor track.
    :ok = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)
    await_renditions(broadcast, &match?([{@track, %VideoTrackFormat{}}], &1))

    assert :ok = Native.update_track(producer, @track, h264_format(1920))

    await_renditions(
      broadcast,
      &match?([{@track, %VideoTrackFormat{params: %{width: 1920}}}], &1)
    )

    :ok = Native.close_broadcast_consumer(consumer)
    :ok = Native.close_broadcast_producer(producer)
    :ok = Native.close_session(sub_session)
    :ok = Native.close_session(pub_session)
  end

  test "send_frame errors after the broadcast producer is closed", %{
    broadcast: broadcast,
    relay: relay
  } do
    {:ok, session} = Native.create_session(relay.url, self(), relay.disable_tls_verify?)
    assert_receive :moq_connected, 10_000

    {:ok, producer} = Native.create_broadcast_producer(session, broadcast)

    :ok = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)
    assert :ok = Native.send_frame(producer, @track, 0, true, "frame")

    :ok = Native.close_broadcast_producer(producer)
    assert {:error, _reason} = Native.send_frame(producer, @track, 40_000_000, true, "frame")

    :ok = Native.close_session(session)
  end

  test "unsubscribe_track stops a live subscription", %{
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

    :ok = Native.add_track(producer, @track, h264_format(), 60, :legacy, 0)

    await_renditions(broadcast, &match?([{@track, %VideoTrackFormat{}}], &1))

    early_token = 1
    :ok = Native.subscribe_track(consumer, @track, early_token, 60)

    :ok = Native.send_frame(producer, @track, 0, true, "before")
    assert_receive {:moq_frame, ^early_token, "before", _timestamp, true}, 10_000

    :ok = Native.unsubscribe_track(consumer, early_token)

    # Consumer commands are processed in order, so once a frame reaches this
    # later subscription the unsubscribe has been handled too.
    late_token = 2
    :ok = Native.subscribe_track(consumer, @track, late_token, 60)

    :ok = Native.send_frame(producer, @track, 40_000_000, true, "after")

    # The frame reaching the live subscription proves it made the round trip;
    # the unsubscribed one must stay silent.
    assert_receive {:moq_frame, ^late_token, "after", _timestamp, true}, 10_000
    refute_receive {:moq_frame, ^early_token, _payload, _timestamp, _keyframe?}, 500

    :ok = Native.close_broadcast_consumer(consumer)
    :ok = Native.close_broadcast_producer(producer)
    :ok = Native.close_session(sub_session)
    :ok = Native.close_session(pub_session)
  end

  # Receives :moq_catalog snapshots in arrival order until one satisfies
  # `matcher`, returning it. Intermediate snapshots are consumed on the way.
  defp await_renditions(broadcast, matcher) do
    assert_receive {:moq_catalog, ^broadcast, renditions}, 10_000

    if matcher.(renditions) do
      renditions
    else
      await_renditions(broadcast, matcher)
    end
  end

  defp h264_format(width \\ 1280) do
    %VideoTrackFormat{
      params: %WebCodecs.VideoTrackParams{width: width, height: 720, framerate: 30.0},
      description: <<>>,
      codec: %WebCodecs.H264Codec{in_band: true, profile: 66, constraints: 0, level: 30}
    }
  end
end
