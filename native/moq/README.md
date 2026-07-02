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

