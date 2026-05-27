# MoQ plugin: test design informed by HLS and SRT references

## Context

`membrane_moq_plugin` is meant to expose a `Membrane.MoQ.Sink` (publisher) and
`Membrane.MoQ.Source` (subscriber) for moq-lite + hang. The Sink in
`lib/sink.ex` already accepts H.264, H.265, AAC, Opus on `:on_request` pads,
manages broadcasts, and republishes the catalog on track add/remove. The
Source in `lib/source.ex` is a thin pass-through that emits raw frame payloads
on a `Membrane.RemoteStream` output. There is one integration test in
`test/integration_test.exs` doing a loopback (Sink → relay → Source) that
checks byte equality of `bbb.ts`.

The user wants:

1. A conceptual map of the two reference plugins
   (`.reference/membrane_http_adaptive_stream_plugin`,
   `.reference/membrane_srt_plugin`) framed for someone who knows MoQ but not
   HLS or SRT, so the differences in *what they ship* explain the differences
   in *how they're tested*.
2. A concrete proposal for what to test on the MoQ Sink and Source, drawing
   from those references.
3. A list of scenarios that won't be automated cheaply but should be exercised
   manually during development (OBS with the moq-obs plugin, browser hang
   watch player, the standalone subscriber in `standalone/`).

This document is the deliverable; no code changes are proposed here.

---

## 1. What the reference plugins actually are

### HLS (`membrane_http_adaptive_stream_plugin`)

HLS is a **pull, file-based** streaming protocol. The sender writes
`.m3u8` playlists and `.m4s` / `.ts` segment files into a *storage*; clients
fetch them over HTTP. There is no live socket, no session, no relay. The
plugin therefore ships:

- `Membrane.HTTPAdaptiveStream.Sink` — low-level CMAF muxer that writes
  segments + manifests via a pluggable `Storage` behaviour.
- `Membrane.HTTPAdaptiveStream.SinkBin` — full encoder→parser→muxer→Sink
  bundle, the thing users actually instantiate.
- `Membrane.HTTPAdaptiveStream.Source` — HLS *playback* element: fetches an
  `.m3u8`, demuxes fragmented MP4 or MPEG-TS, emits H.264/H.265/AAC.
- Storage backends: `FileStorage`, `GenServerStorage`, `SendStorage` (test
  mock that turns every store/remove into a message).

Mental mapping for a MoQ person:

| HLS concept | MoQ analogue |
|---|---|
| storage backend | the relay (writes go to the wire instead of disk) |
| `.m3u8` master/media playlist | the hang catalog |
| `.m4s` / `.ts` segment | a MoQ group (one per IDR for video) |
| init segment / fMP4 header | the per-track init you put in the catalog |
| LL-HLS partial segment | not really mirrored; closest analogue is MoQ frame granularity inside a group |

Implication for tests: HLS produces *durable artefacts* (manifests, segments)
that you can compare against committed golden files. That is its dominant
test idiom.

### SRT (`membrane_srt_plugin`)

SRT is a **live, point-to-point UDP-based** transport: one peer listens, one
peer connects, packets flow until somebody disconnects. There is no
manifest, no segmentation at the protocol level. The payload is whatever you
put in (typically MPEG-TS so audio + video can share the link). The plugin
ships only:

- `Membrane.SRT.Source` — listener/server side, receives bytes.
- `Membrane.SRT.Sink` — caller/client side, sends bytes.
- A small MPEG-TS muxer/demuxer pair used to make multi-track SRT useful.

Mental mapping for a MoQ person:

| SRT concept | MoQ analogue |
|---|---|
| listener port | a relay-side broadcast path |
| caller / publisher | the Sink connecting to the relay |
| stream id + passphrase | the broadcast path (and any future auth on the relay) |
| MPEG-TS over SRT | how SRT bolts multi-track on top of an opaque byte pipe; MoQ does this natively with one track per rendition |

Implication for tests: SRT's "artefact" is a **stream of bytes** that
arrives at the receiver. Verification is by byte equality of the input file
against the output file, plus a couple of negative-path tests (auth
mismatch). There are no manifests to diff, so the suite stays small.

### Why their test layouts differ

| | HLS | SRT |
|---|---|---|
| Output shape | Files on disk | Live byte stream |
| Stateful | very (window, persist mode, deltas) | barely |
| Verifiable offline | yes — golden manifests + segments | no — must run end-to-end |
| Test split | unit (Sink + `SendStorage`) **and** integration (`SinkBin` + `FileStorage` vs golden fixtures) | integration only, one MPEG-TS muxer unit test |
| Network | none | real loopback sockets, ephemeral ports |
| External dep | none | optional external `ExLibSRT.Server` for one test |
| Failure tests | cleanup + dynamic pad add/remove | one auth-mismatch crash |

HLS gets *deep* unit tests because its core Sink is a pure state machine
over storage operations — easy to mock, easy to diff. SRT skips that layer
because its Sink is essentially `socket.send/2`; there is nothing
interesting between the API and the wire to assert without actually moving
bytes.

MoQ sits between the two: it has hang-level state (catalog, tracks,
broadcasts, group boundaries on keyframes) like HLS, but the artefact lives
on a remote relay rather than on disk like SRT. So the test design borrows
from both.

---

## 2. Proposed automated tests for `Membrane.MoQ.Sink` and `Membrane.MoQ.Source`

All integration tests are gated behind `RELAY_URL` (already the convention
in `test/integration_test.exs:16`). When unset, they're skipped. The cheapest
way to get reliable CI later is to launch `moq-relay` with `muontrap` (the
HLS plugin pulls it in for similar reasons) but that's a follow-up — for now
keep the env-var gate.

### 2a. Sink — pure unit tests (no relay)

The HLS plugin proves that you can get real coverage of a Sink without ever
opening a socket, by mocking the boundary. For MoQ that boundary is
`Membrane.MoQ.Native`. Approach:

- Introduce a thin behaviour (or `Mox`-style mock) the Sink calls instead of
  `Native` directly. In tests, swap in an implementation that records every
  `setup_session/3`, `open_broadcast/2`, `add_*_track/*`, `send_frame/4`,
  `remove_track/1`, `close_broadcast/1`, `close_session/1` call as a message
  to the test process — exactly what `SendStorage` does for HLS.
- Drive the Sink with a `Membrane.Testing.Pipeline` and a trivial source
  that emits buffers with the right stream format.

Scenarios worth covering, copied largely from
`test/membrane_http_adaptive_stream/sink_test.exs`:

- `single H.264 track` — open broadcast, add track, send N frames, expect
  catalog-track sequence; verify keyframe flag is forwarded faithfully
  (group boundary semantics).
- `single AAC track` — every frame marked keyframe in the helper; verify the
  Native call sequence.
- `Opus + H.265 track` — covers both other codecs in one go.
- `multiple tracks in one broadcast` — add audio + video, verify catalog is
  republished after each add (matches the `@moduledoc` claim in `lib/sink.ex:18`).
- `multiple broadcasts on one Sink` — two pads with different `:broadcast`
  pad options, one Sink-level default; verify
  `ensure_broadcast/2` opens each path exactly once.
- `dynamic pad add then remove` — close on unused broadcast collapses it
  (`maybe_close_broadcast/2` in `lib/sink.ex:267`).
- `EOS on all pads closes the session` — `handle_end_of_stream/3` in
  `lib/sink.ex:237`.
- `pad with no broadcast configured raises` — current behaviour at
  `lib/sink.ex:124`.

Codec-string regression tests deserve a separate `_test.exs` (pure
functions, no pipeline): for H.264, exercise both branches of
`h264_codec_string/1` (DCR-bearing AVC, profile-only fallback) against
known-good WebCodecs strings, and the AAC profile byte mapping. These
correspond to nothing in the SRT plugin but parallel
`bandwidth_calculator_test.exs` from HLS in spirit.

### 2b. Source — pure unit tests (no relay)

`lib/source.ex` is small but worth a Mox-style test of its own:

- On `handle_playing/2`, `Native.start_subscriber/4` is called with the right
  args and registered with the resource guard.
- An incoming `{:moq_frame, payload}` message becomes a buffer on `:output`.
- An incoming `:moq_disconnected` message produces `end_of_stream`.
- Unknown messages log a warning and don't crash.

These are easy to write because the Source's external surface is just three
messages.

### 2c. Loopback integration (Sink ↔ relay ↔ Source)

This is the SRT pattern, and it's what `test/integration_test.exs` already
does. Build it out into a small matrix:

- `passthrough loopback` — the existing test (legacy container, raw bytes
  in, raw bytes out, byte equality).
- `H.264 publish + subscribe` — File → H264 parser → Sink; Source → file;
  compare output H.264 bytes (this requires the Source to actually demux
  hang frames; if it stays passthrough for now, gate this scenario behind a
  TODO).
- `multi-track broadcast` — audio + video pads on one Sink, two Sources
  subscribed to different tracks; verify both arrive.
- `dynamic add/remove during streaming` — add a second pad mid-stream, drop
  it, ensure the surviving pad keeps producing.
- `subscribe to nonexistent broadcast` — the existing graceful-disconnect
  test; keep it.
- `relay disappears mid-stream` — kill `moq-relay` between assertions;
  expect EOS on Source and a warning on Sink (mirrors SRT's "connection
  fails" deterministic-crash test). Useful but flaky if the relay isn't
  managed by the test; defer until we wrap relay startup.
- `CMAF container path` — same loopback but with `container: :cmaf` on the
  Sink; right now no automated check beyond "doesn't crash" because the
  Source is opaque, so flag this as the natural extension once the Source
  understands containers.

### 2d. Fixtures

`test/fixtures/` already has the SRT plugin's `bbb.*` files. Reuse them.
The HLS-style "golden manifest diff" doesn't directly transfer — there is
no manifest on disk — but a future test could `curl` the relay's catalog
endpoint and compare against a committed JSON snapshot. Worth keeping in
mind, not worth building now.

---

## 3. Manual / interop scenarios (developer loop, not CI)

These are the things the reference plugins also don't automate, and they're
where MoQ pays for being a young protocol. Keep these in `examples/` or in
a `MANUAL_TESTING.md` so they don't bit-rot.

- **OBS with `moq-obs` plugin as consumer.** Run `examples/publish_h264.exs`
  (already in the repo) and `examples/publish_cmaf.exs`, point an OBS Browser
  Source or the moq-obs plugin at the broadcast, eyeball A/V sync, verify
  audio + video tracks both render. OBS only handles the legacy container,
  so this is the *only* automatable-by-eye check that legacy mode works.
- **hang browser watch player.** Same publish, point the player at the
  broadcast URL. Catches CMAF init / codec-string mistakes (the H.264 one in
  `lib/sink.ex:295` is fragile; the H.265 one at `:319` is admittedly a
  hard-coded guess).
- **`standalone/` subscriber.** The Rust binary in `standalone/src/main.rs`
  is the closest thing to a Membrane-free Source. Run Sink → relay →
  standalone subscriber and check it produces sensible bytes; useful when
  Source-side bugs are suspected.
- **`Membrane.MoQ.Sink` ← someone else's publisher → `Membrane.MoQ.Source`.**
  Inverse of the loopback: publish from `moq-cli` or `moq-obs`, subscribe
  with our Source. Catches assumptions in the Source about frame framing
  that the Sink would never violate because it produced the bytes itself.
- **Catalog inspection by hand.** `curl https://relay/<broadcast>/catalog`
  (per the question already noted in `memory/moq_questions.md`) and confirm
  renditions appear/disappear as pads are added/removed.
- **Long-running soak.** Publish for 10+ minutes, watch RSS in `top` for
  the BEAM and the relay; confirm `Native.send_frame/4` doesn't leak, the
  ring/looping buffer queues in `lib/ring_buffer.ex` /
  `lib/looping_buffer_queue.ex` stay bounded.

---

## 4. Critical files

- `lib/sink.ex` — Sink module under test; especially
  `handle_pad_added/3` (`:120`), `handle_stream_format/4` clauses (`:153`+),
  `handle_buffer/4` (`:229`), `handle_end_of_stream/3` (`:237`),
  `ensure_broadcast/2` (`:254`), `maybe_close_broadcast/2` (`:267`),
  `h264_codec_string/1` (`:295`).
- `lib/source.ex` — Source module under test.
- `lib/native.ex` — boundary to mock for unit tests; the function names here
  define the recorded-call vocabulary.
- `test/integration_test.exs` — existing loopback; the new integration
  matrix should grow here or in sibling files.
- `test/fixtures/bbb.*` — already present, copied from SRT.
- `.reference/membrane_http_adaptive_stream_plugin/test/membrane_http_adaptive_stream/sink_test.exs`
  — pattern to copy for unit-testing the Sink against a recorded Native mock.
- `.reference/membrane_http_adaptive_stream_plugin/lib/membrane_http_adaptive_stream/storages/send_storage.ex`
  — concrete reference for the message-recording mock.
- `.reference/membrane_srt_plugin/test/integration_test.exs` — pattern to
  copy for sender↔receiver loopback variants and the negative-path test.

## 5. Verification

This deliverable is documentation, not code, so "verification" is just:
the user reads the overview, agrees with the scoping (or redirects), and
then a follow-up task implements the unit-test scaffold (Native mock +
`sink_test.exs`-equivalent) before extending the integration matrix.
