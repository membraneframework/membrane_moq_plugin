# Extending the plugin to support `container: "cmaf"`

This plugin currently publishes/subscribes only with the **legacy** hang container. This document captures everything we learned about adding CMAF support so a future contributor can pick it up without re-doing the investigation.

References below cite paths inside the upstream reference checkout at `.reference/moq/`. Pin to the same commit you're targeting; the catalog wire format for CMAF is in flux upstream (see PR #1341).

---

## Why we shipped legacy first

The native CMAF *encoding* path in upstream MoQ is unfinished. Concretely:

- `doc/concept/standard/interop.md:82` says verbatim: *"We support two possible containers, but currently `cmaf` is experimental"*.
- `rs/moq-mux/src/cmaf/container.rs::encode` (lines 66-130) is `pub(crate)`, has zero unit tests, and **has zero callers anywhere in the upstream workspace**. It's effectively dead code that any external consumer (us) would be the first to exercise.
- That `encode` function builds `mp4_atom::TrunEntry` with only `size` and `flags` (lines 85-91) and `mp4_atom::Tfhd` with just `track_id` (lines 98-101). Both `TrunEntry.duration` and `Tfhd.default_sample_duration` are left at `None`.
- The JS decoder (`js/hang/src/container/cmaf/decode.ts:270-284` on `main`) reads `sample.sampleDuration ?? defaultDuration`. With the encoder above, both are 0, and the decoder throws `Invalid sample duration 0 for sample 0 in trun`. Result: the watch component goes Live but no frames render.
- The only working CMAF path upstream is **passthrough**: ffmpeg writes valid moof+mdat, and `import::Fmp4` with `passthrough: true` forwards bytes unchanged (`rs/moq-mux/src/import/fmp4.rs:638-647`). `cmaf::encode` is never called.

This means routing per-frame data through `moq_mux::ordered::Producer<Container::Cmaf>` (the natural Rust API) produces invalid bytes. Legacy avoids the problem entirely because its encoder lives in `Container::Legacy::write` and writes the moq-lite native frame format (timestamp + payload), which the browser path handles correctly.

## Upstream direction (snapshot, may have moved)

The maintainer's stated direction is to remove the native CMAF encoder and only support passthrough. Tracking issues / PRs:

- **Issue [#676 "Native CMAF support"](https://github.com/moq-dev/moq/issues/676)** — closed, but only the passthrough subset shipped.
- **PR [#1341 "Refactor media producers and simplify fMP4 CMAF passthrough"](https://github.com/moq-dev/moq/pull/1341)** — open, by the project maintainer. *"Removes `Fmp4Config`, makes CMAF passthrough the only mode."* Reshapes `Container::Cmaf` from `{ timescale, track_id }` to `{ init_data: Bytes }` (raw ftyp+moov), so subscribers can read `trex` defaults from the init segment.
- **PR [#1057](https://github.com/moq-dev/moq/pull/1057)** — closed, attempted to fix `trun → tfhd → trex` duration fallback in the JS decoder. Deferred to issue [#1059 "Init tracks for CMAF"](https://github.com/moq-dev/moq/issues/1059) (still open) which is the architectural redesign for moving init segments off the catalog onto their own track.
- **PR [#1164](https://github.com/moq-dev/moq/pull/1164)** — merged on the dev branch. Removes the JS decoder's `duration=0` validation entirely with the rationale *"duration is unused in the `Sample` type"*. So on `dev` the exception above no longer fires, but the underlying timing is still wrong.

When approaching CMAF support, **first check whether #1341 has merged and whether the catalog format has changed**. The `Container::Cmaf { init_data: Bytes }` shape is a wire-breaking change.

## The three implementation paths

In order of preference:

### Path A — bytes-in / bytes-out passthrough (recommended)

Upstream-blessed. The Membrane side does the CMAF muxing (e.g. `Membrane.MP4.CMAF.Muxer` from `membrane_mp4_plugin`), and the Rust NIF treats fragments as opaque bytes:

1. Receive a `%Membrane.CMAF.Track{}` stream format on the pad. Pass `header` (the ftyp+moov init segment) and `content_type` over the FFI.
2. On the Rust side, parse the moov **once** to populate the catalog, then forward each subsequent moof+mdat as a single `moq_lite::Frame`.
3. New group on every keyframe-aligned fragment, mirroring `import/fmp4.rs:629-636`.

There is no public Rust helper that takes a raw init segment and returns `(VideoConfig | AudioConfig)`. The conversion lives privately inside `import::Fmp4::init_video()` (line 252) and `init_audio()` (line 412). Two ways to reuse it:

- **Drive `import::Fmp4` directly** — feed the init segment + each fragment into `Fmp4::decode()` (line 139) with `Fmp4Config { passthrough: true }`. Catalog auto-populates from the moov, fragments forward unchanged. The cost is one extra serialize/parse round trip on bytes Membrane has already parsed once. Smallest code change.
- **Pull the helpers into our crate** — copy the `Codec::Avc1`/`Hvc1`/`Hev1`/`Mp4a`/`Opus` arms out of `init_video`/`init_audio` (~150 lines, mechanical) and expose `pub fn video_config_from_trak(trak: &Trak) -> VideoConfig`. More upkeep but no double-parse.

Start with the first option. Profile before optimizing.

### Path B — fix the encoder locally

If we ever need true per-frame CMAF emission (no upstream muxer in front), we'd have to fix `cmaf::encode` ourselves:

- Populate `TrunEntry.duration` per sample from frame timestamps, **or** set `Tfhd.default_sample_duration` and ensure all samples share that duration.
- The duration unit is **timescale ticks**, not microseconds. For `timescale: 90000`, a 30 fps frame = 3000 ticks.
- `mp4_atom` should auto-set the corresponding `tr_flags` / `tf_flags` bits (`0x000100` sample-duration-present, `0x000008` default-sample-duration-present) when the field is `Some`.
- Verify with `mp4dump` (Bento4) or `mp4box -info` (GPAC) that the trun entries have non-zero duration.

Expect the maintainer to redirect any upstream PR to #1341 / #1059. Locally, this is fine as a stopgap.

### Path C — track upstream `dev`

The dev branch (per PR #1164) silently accepts duration=0 on the JS decoder, masking the symptom. The underlying timing is still broken — buffered ranges and sync will be off — so this is not actually a fix, just a way to make the player stop throwing. Don't rely on it.

---

## Membrane-side considerations

### `%Membrane.CMAF.Track{}` mapping

```elixir
%Membrane.CMAF.Track{
  content_type: :audio | :video | [:audio, :video],
  header: binary(),                  # ftyp+moov bytes — the init segment
  resolution: {w, h} | nil,
  codecs: %{...}                     # informational; MoQ parses the moov directly
}
```

For Path A, the `header` field maps directly to what the Rust side feeds into `Fmp4::decode` first. The `codecs` map is informational — we don't need to translate it field-for-field; the moov parser will rebuild it from the binary header.

### One pad per track, not muxed

CMAF spec recommends per-track segments and `Membrane.MP4.CMAF.Muxer` follows that. The MoQ wire model also wants one moq-lite track per media track (separate group streams give independent priority/buffering). So: **one Membrane pad per track, one moq-lite track per pad**.

If `content_type` ever arrives as `[:audio, :video]` (a multi-trak fragment), reject it or split it before muxing — see the warning in §3 of the linked session log: feeding multi-trak moofs into the upstream importer in passthrough mode duplicates bytes onto wrong tracks.

### Codecs

The catalog supports both AAC and Opus on the Rust side:

- AAC: `mp4_atom::Codec::Mp4a` → `AudioCodec::AAC` (`fmp4.rs:423-449`). Requires `description` = AudioSpecificConfig from `esds`. The importer builds it via `build_aac_audio_specific_config`.
- Opus: `mp4_atom::Codec::Opus` → `AudioCodec::Opus` (`fmp4.rs:450-460`). `description: None`.

Caveat: **`mp4a` is the ISO BMFF FourCC for AAC specifically** (and a couple of legacy MPEG-2 modes). Opus uses its own sample entry box (`Opus`). The Membrane struct's `codecs` map only spells `mp4a`/`avc1`/`hvc1` — for Opus you'd add `:opus => %{...}`. Whether `Membrane.MP4.CMAF.Muxer` actually emits Opus today is a Membrane-side question; verify against `membrane_mp4_plugin` source before committing.

### Plugin API surface

Per `docs/TODO.md`, current plan: a Sink-scoped option `container: :legacy | :cmaf`. Implementation hint:

- For `:legacy`, keep what we have — `moq_mux::ordered::Producer<Container::Legacy>` with frames carrying `(timestamp, payload, keyframe)`.
- For `:cmaf` Path A, route bytes through `import::Fmp4` (or a thin equivalent) and store the broadcast/catalog handle the same way. The Sink interface stays the same; only the input stream format and the inner producer differ.
- OBS interop is legacy-only as of this writing (`docs/TODO.md`), so default to `:legacy` and surface CMAF as opt-in.

---

## Subscriber (Source) side

The browser watcher *can* consume CMAF; the decoder dispatch in `js/watch/src/{video,audio}/{decoder,mse}.ts` branches on `config.container.kind`. There's no equivalent decoder-readiness concern for our Source — we're reading raw moq-lite frames either way. For CMAF subscribe, we receive whole moof+mdat fragments per frame and have to:

1. Read the catalog's `Container::Cmaf { ... }` entry to get the init segment / timescale / track_id (whichever the catalog format of the day uses).
2. Reconstruct a CMAF byte stream (init segment, then fragments) and feed it back into Membrane as a `%Membrane.CMAF.Track{}` for downstream demuxing.

The subscribe path has no `cmaf::encode` problem — it's pure decode/reassemble. The thing to watch out for is jitter: per `js/watch/src/video/decoder.ts:381`, the CMAF subscriber path lacks the latency-control consumer wrapper that legacy has. Out-of-order delivery will starve playback. If we hit that, mirror the legacy `Consumer` reorder buffer for our Source.

---

## Diagnostic toolkit

Bookmark these for the day someone hits a "Live but no media" stall:

- **`mp4dump` / `mp4box -info`** on a captured fragment to verify trun entries have non-zero `duration` and that `tfhd` / `trex` defaults are sane.
- The JS watcher has `[hang-watch ...]` console diagnostics added to the local checkout at `.reference/moq/js/watch/src/{video,audio}/{decoder,mse}.ts` — they print dispatch decisions, init-segment construction, first frame received, and decode errors. Useful for distinguishing "wrong container kind in catalog" from "valid CMAF that fails to parse" from "no frames arriving at all".
- `moq-cli ... fmp4 --passthrough` is the known-good baseline. If your own publisher is broken, tee bytes from both publishers and `diff` the moof boxes.

---

## TL;DR for a future contributor

1. Read PR #1341 first — if it's merged, the catalog shape has changed and these instructions need updating.
2. Use Path A (passthrough). The Membrane CMAF muxer produces correct fragments; just forward them.
3. The native Rust `cmaf::encode` is broken and unused. Don't route per-frame data through `moq_mux::ordered::Producer<Container::Cmaf>` and expect it to work.
4. Default `container:` to `:legacy` for OBS interop. Surface `:cmaf` as opt-in.
5. One pad per track. AAC needs `description` from `esds`; Opus doesn't.
