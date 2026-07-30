# Rust bindings for MoQ publishing/subscribing

## Conceptual model

Each MoQ track needs to be assigned to a broadcast.
A broadcast is a collection of tracks, and a session is a collection of broadcasts.
For details, see https://doc.moq.dev/concept/layer/moq-lite.html#terminology

## Publishing example

```elixir
alias ExMoQ.Native

{:ok, session} = Native.create_session(url, self(), false)

receive do
  :moq_connected -> :ok
after
  2_000 -> raise "timeout"
end

{:ok, broadcast} = Native.create_broadcast_producer(session, "my_broadcast")

format =
  {:h264,
   %{
     params: %Native.VideoTrackParams{width: 1280, height: 720, framerate: 30.0},
     description: avc_decoder_config_record,
     codec: %Native.H264Codec{inline: false, profile: 100, constraints: 0, level: 31}
   }}

{:ok, video_track} =
  Native.add_track(broadcast, "my_video_track", format, _priority = 60, :legacy, _latency_ns = 0)

IO.puts("MoQ setup successful, you can start streaming frames to PID #{inspect(self())}")

Stream.repeatedly(fn ->
  receive do
    {:video, buf, timestamp_ns, keyframe?} ->
      Native.send_frame(video_track, timestamp_ns, keyframe?, buf)
  end
end)
|> Stream.run()
```

## Subscribing example

```elixir
alias ExMoQ.Native

{:ok, session} = Native.create_session(url, self(), false)

receive do
  :moq_connected -> :ok
after
  2_000 -> raise "timeout"
end

{:ok, consumer} = Native.create_broadcast_consumer(session, "my_broadcast", self(), latency_ns)

{format, container} =
  receive do
    {:moq_catalog, "my_broadcast", renditions} ->
      {"my_video_track", rendition} = List.keyfind!(renditions, "my_video_track", 0)
      rendition
  after
    10_000 -> raise "broadcast was not announced"
  end

# `token` is any integer you choose; it tags this subscription's messages.
:ok = Native.subscribe_track(consumer, "my_video_track", container, _token = 1, _priority = 60)

Stream.repeatedly(fn ->
  receive do
    {:moq_frame, 1, payload, timestamp_ns, keyframe?} ->
      handle_frame(payload, timestamp_ns, keyframe?)

    {:moq_track_finished, 1} ->
      IO.puts("track finished")

    {:moq_track_error, 1, reason} ->
      raise "subscription failed: #{reason}"

    {:moq_broadcast_closed, "my_broadcast", reason} ->
      raise "broadcast closed: #{reason}"
  end
end)
|> Stream.run()
```
