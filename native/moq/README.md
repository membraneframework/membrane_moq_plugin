# Rust bindings for MoQ publishing/subscribing

## Conceptual model

Each MoQ track needs to be assigned to a broadcast.
A broadcast is a collection of tracks, and a session is a collection of broadcasts.

Here's a simple usage example:
```elixir
alias Membrane.MoQ.Native

{:ok, session} = Native.setup_session(url, self(), false)

receive do
  :moq_connected -> :ok
after
  2_000 -> raise "timeout"
end

{:ok, broadcast} = Native.open_broadcast(session, "my_broadcast")

{:ok, audio_track} = Native.add_aac_track(broadcast, "my_audio_track", profile, sample_rate, channels)
{:ok, video_track} = Native.add_h264_track(broadcast, "my_video_track", params, dcr, codec)

IO.puts("MoQ setup successful, you can start streaming frames to PID #{inspect(self())}")

Stream.repeatedly(fn ->
  receive do
    {:audio, buf, timestamp_us} -> Native.send_frame(audio_track, timestamp_us, true, buf)
    {:video, buf, timestamp_us, keyframe?} -> Native.send_frame(video_track, timestamp_us, keyframe?, buf)
  end
end) |> Stream.run()
```

For details, see https://doc.moq.dev/concept/layer/moq-lite.html#terminology

## Thread model

A global multi-threaded tokio runtime is shared across all sessions.

```
Caller thread         Tokio runtime
─────────────         ─────────────
setup_session() ───→  [Session task] — owns QUIC connection, waits for
                                        shutdown signal or disconnect

open_broadcast()      (synchronous, no background task)

add_*_track()  ────→  [Track task]   — receives frames via channel,
                                        writes them to the MoQ transport

send_frame()          (enqueues frame to the track task's channel)
```

- **Session task** — one per connection. Owns the QUIC session and runs until
  `close_session()` is called or the relay disconnects. Sends
  `moq_connected` / `moq_disconnected` notifications to the caller.
- **Track task** — one per track. Receives `Frame` commands from an unbounded
  channel and writes them into the MoQ container format. Exits on `Stop` or
  when the channel closes.

`send_frame()` never blocks on I/O — it copies the payload into an unbounded
channel and returns immediately. Back-pressure is not applied; the caller is
responsible for rate-limiting frame submission.

