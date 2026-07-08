# Membrane MoQ Plugin

[![Hex.pm](https://img.shields.io/hexpm/v/membrane_moq_plugin.svg)](https://hex.pm/packages/membrane_moq_plugin)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/membrane_moq_plugin)

Membrane plugin for [Media over QUIC](https://moq.dev) (MoQ) streams:

* `Membrane.MoQ.Sink` publishes tracks to a broadcast on a MoQ relay.
* `Membrane.MoQ.Source` subscribes to a broadcast's tracks
  and emits their frames, notifying its parent as tracks come and go.

The MoQ session, catalog and wire handling are implemented natively
on top of the [moq](https://github.com/moq-dev/moq) Rust stack
(`moq-native`, `moq-mux`, `hang`), bound via Rustler NIFs.

Broadcasts use the [hang](https://doc.moq.dev/concept/layer/hang.html) catalog,
so they interoperate with the `moq` CLI, moq-gst and the JS `@moq/hang` player.

Publishing encapsulates frames in the `:legacy` or `:loc` wire container.
Consuming selects each track's container from the catalog automatically.

It is a part of [Membrane Multimedia Framework](https://membrane.stream).

## Installation

The package can be installed by adding `membrane_moq_plugin` to your list of
dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:membrane_moq_plugin, "~> 0.1.0"}
  ]
end
```

Building requires a Rust toolchain (the NIF is compiled by [rustler](https://hex.pm/packages/rustler))

## Usage

Both elements talk to a MoQ relay. For local development run one with
anonymous auth, e.g. [`moq-relay`](https://github.com/moq-dev/moq) (`cargo install moq-relay`).
The examples assume `https://localhost:4443` with a self-signed certificate.

Publishing an H.264 track:

```elixir
child(:source, %Membrane.File.Source{location: "video.h264"})
|> child(:parser, %Membrane.H264.Parser{
  output_stream_structure: :avc3,
  generate_best_effort_timestamps: %{framerate: {30, 1}}
})
|> child(:realtimer, Membrane.Realtimer)
|> via_in(Pad.ref(:input, :video), options: [track: "video"])
|> child(:sink, %Membrane.MoQ.Sink{
  url: "https://localhost:4443",
  broadcast: "demo.hang",
  disable_tls_verify?: true
})
```

Subscribing to it:

```elixir
child(:source, %Membrane.MoQ.Source{
  url: "https://localhost:4443",
  broadcast: "demo.hang",
  disable_tls_verify?: true
})
|> via_out(Pad.ref(:output, :video), options: [track: "video"])
|> child(:parser, %Membrane.H264.Parser{
  generate_best_effort_timestamps: %{framerate: {30, 1}}
})
```

The `examples/` directory has runnable scripts covering the common setups:
- loopback publish+play (`publish_and_play.exs`)
- multi-track A/V from an MP4 (`publish_mp4.exs`), H.265 (`publish_h265.exs`)
- endless looped publishing (`publish_h264_loop.exs`)
- mid-stream format changes (`publish_format_change.exs`)
- notification-driven subscribing (`dynamic_subscriber.exs`).

## Testing

`mix test` runs the unit suite.
Integration tests exercise a real relay and are opt-in:

```shell
mix test --include integration
```

## Copyright and License

Copyright 2026, [Software Mansion](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_moq_plugin)

[![Software Mansion](https://logo.swmansion.com/logo?color=white&variant=desktop&width=200&tag=membrane-github)](https://swmansion.com/?utm_source=git&utm_medium=readme&utm_campaign=membrane_moq_plugin)

Licensed under the [Apache License, Version 2.0](LICENSE)
