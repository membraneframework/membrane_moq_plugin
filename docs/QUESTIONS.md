  - What is a track and a broadcast from the publisher/relay's point of view?
    - A broadcast groups tracks. A track is a collection of groups (delivered out-of-order until expired). A group is a sequence-numbered collection of frames. `moq-lite/src/model/broadcast.rs`, `track.rs`, `group.rs`.
  - Why does moq-lite have an Origin concept that moq-transport is missing? What is it, and how does it relate to Sessions and Broadcasts?
    - Per `doc/concept/layer/moq-lite.md`: **Origin = "a collection of broadcasts, used to scope what is available to a session."** It has no moq-transport equivalent because moq-transport expresses scoping purely via namespace prefix matching at the wire level; moq-lite makes it a first-class object.
    - Concretely: the relay holds one root `OriginProducer` (a path trie of all broadcasts in the cluster). Each session gets a scoped view — `with_root(&token.root).consume_only(&token.subscribe)` — that scoped `OriginConsumer` is the session's Origin. One session → one Origin view; multiple sessions can share the same Origin if their auth tokens have identical scope.
    - The `Origin` struct's 62-bit random ID (`Origin::random()`) is a separate, implementation-level detail used for relay **loop detection** and **shortest-path preference**: each relay appends its own ID to `Broadcast.hops` when forwarding; if it sees its own ID already in the list it drops the broadcast (loop). When the same broadcast arrives via two cluster paths, the shorter `hops.len()` wins. This is not what "Origin" means to an API consumer — it's internal relay routing machinery. `moq-lite/src/model/origin.rs`, `moq-relay/src/cluster.rs`.
  - What formats does MoQ support? In what context would a single broadcast have multiple video and audio tracks?
    - `hang` stores multiple **renditions** per media type in `BTreeMap<String, VideoConfig>` / `BTreeMap<String, AudioConfig>`. Renditions are named (e.g. "1080p", "720p") and share the same type. There is one `Video` and one `Audio` catalog entry, each containing N renditions. The subscriber picks which rendition(s) to subscribe to by name. `hang/src/catalog/root.rs`, `video/mod.rs`, `audio/mod.rs`.
  - How are auxiliary tracks like subtitles announced via the catalog?
    - Not supported by `hang` at all — `moq-mux/src/import/fmp4.rs:198` explicitly `bail!`s on `b"sbtl"` handler boxes. The `hang` catalog only has `video`, `audio`, optional `user` (broadcaster metadata), optional `chat`, and optional `preview` fields.
  - Are there currently alternatives to hang?
    - Yes, but they're developing (MSF, LOC). `moq-mux::catalog` publishes **both** a hang catalog and an MSF catalog on every mutation. You could try rolling your own if there's a use case.
  - What are the major differences between the hang catalog and MSF?
    - Both are JSON over the same MoQ track transport. Key differences:
      - **Track name**: hang uses `catalog.json`; MSF uses `catalog`
      - **Schema**: hang has a top-level object `{ video: { renditions: { "name": {...} } }, audio: { renditions: {...} }, user?, chat?, preview? }`; MSF uses `{ version: 1, tracks: [ { name, ... }, ... ] }` (flat array)
      - **Renditions**: hang uses a `BTreeMap` keyed by name (so JSON Merge Patch works for incremental updates); MSF uses a plain array
      - **Container/packaging field**: hang calls it `container` with values `"legacy"` / `"cmaf"`; MSF calls it `packaging` and adds `"loc"`, `"media_timeline"`, `"event_timeline"` variants
      - **Codec representation**: hang uses typed structs (`VideoCodec` enum with profile/level fields); MSF uses a WebCodecs codec string (e.g. `"avc3.64001f"`)
      - **Auxiliary tracks**: MSF supports `role` values for captions, subtitles, sign language, audio description; hang supports none of those
      - **Synchronization metadata**: MSF has `render_group` (synchronized playback) and `alt_group` (quality switching); hang has neither
      - **Conversion**: `moq-mux/src/msf.rs` has `to_msf()` which converts a hang `Catalog` → MSF on the fly
  - Is the OBS plugin hang or MSF compliant, or its own thing?
    - Confirmed hang. It calls `moq_consume_catalog()` and parses the result via `moq_consume_video_config()` which returns hang's JSON schema. Frame payloads are raw codec bitstreams with `frame_data.timestamp_us` (microseconds) — i.e. `container: "legacy"`. `moq-obs/src/moq-source.cpp:311,557,834`.
  - Does the OBS plugin subscribe to the hang catalog track or the MSF one?
    - Hang. The track name it subscribes to is the hang catalog default (`catalog.json`). MSF uses a different track name (`catalog`) and a different JSON schema — OBS never touches it.
  - What does the "legacy" container mean? Is it a custom container made for prototyping that isn't widely used otherwise? What does "raw codec payload + microsecond timestamp" mean exactly?
    - Yes, it was for prototyping and probably shouldn't be targeted by `membrane_moq_plugin`. Concretely: each frame is a QUIC VarInt (1–8 bytes) encoding a microsecond timestamp (`Timescale<1_000_000>`), followed immediately by the raw codec bitstream payload. No container headers. `hang/src/container/frame.rs`, `hang/src/catalog/container.rs`.
  - Why would the OBS consumer care about the container used if it mixes audio/video from separate tracks manually?
    - Confirmed: the timestamps from the `legacy` container (or the moof decode time for `cmaf`) are exactly what the consumer uses to sync the two tracks. `cmaf` keeps timestamps in the moof box rather than a separate header.
  - What is root in the relay connection logs? How do you set it in Rust? Where is it documented?
    - `root` is a URL path **prefix** stored in the JWT `AuthToken` (`moq-relay/src/auth.rs`). When the relay accepts a connection it strips the token's `root` from the connection URL path, and uses the remaining suffix as the broadcast name. If the path doesn't have that prefix, the connection is rejected with `AuthError::IncorrectRoot`. Set it when minting the token; it's documented only in the relay source.
  - How do you fetch the catalog with `curl`?
    - You can't. It's a bytestream like the other tracks and you receive it the same way.
  - What is `moq_mux::import` doing if simply sending buffers to `OrderedProducer` works fine?
    - `moq-mux/src/catalog.rs` (`CatalogProducer`): (1) creates and inserts both a `hang` catalog track and an MSF catalog track into the broadcast; (2) re-publishes the full serialized catalog as a new group on **every mutation** (track added/removed). It also parses the moov/trak boxes from incoming fMP4 to build the hang `VideoConfig`/`AudioConfig` with correct codec strings, timescales, and dimensions. Without it you'd have to do all of that manually.
  - What is the overall structure of `moq_mux`? What do the submodules do?
    - `moq_mux` is a **demux/mux bridge library** — it converts between existing media formats and MoQ. It does NOT need to be used at all if you're publishing raw demuxed frames.
      - `import/` — demuxers: reads fMP4, HLS, AAC, Opus, H.264 Annex-B, H.265, AV1 byte-streams and produces MoQ track data. This is what `moq-cli` uses when you pipe `ffmpeg` output to it.
      - `catalog` — `CatalogProducer`: builds and re-publishes both hang + MSF catalog JSON whenever tracks are added/removed. Used internally by `import/`.
      - `ordered/` — generic `Producer<C: Container>` / `Consumer<C>`: wraps a `moq_lite::TrackProducer` and handles group boundaries at keyframes, optional latency buffering, and frame encoding/decoding for any `Container` impl.
      - `msf` — `to_msf()`: converts a hang `Catalog` struct into MSF JSON and publishes it.
      - `cmaf/` — CMAF container adapter (behind `mp4` feature flag).
      - `hang/` — hang `Legacy` container encoder/decoder (VarInt timestamp + raw payload).
  - My audio/video are already demuxed (e.g. H.264 NAL buffers with PTS). Do I need to wrap them in fMP4/CMAF, or is there a simpler API?
    - No fMP4 needed. For `container: "legacy"` (simplest): use `hang::container::OrderedProducer` directly:
      ```rust
      producer.write(Frame { timestamp: Timestamp::from_micros(pts_us), payload: bytes })?;
      producer.keyframe()?; // call before each keyframe
      ```
      This is legacy-only — it always encodes as `VarInt(timestamp_µs) ++ raw_payload`. `hang/src/container/producer.rs`.
    - For `container: "cmaf"`: use `moq_mux::ordered::Producer<hang::catalog::Container>` instead. `hang::catalog::Container` implements the `moq_mux::container::Container` trait and dispatches to real `moof+mdat` construction for the `Cmaf` variant (`moq-mux/src/hang/container.rs:49-78`, requires the `mp4` feature). Use `with_latency()` to batch multiple input frames into one fragment:
      ```rust
      let container = hang::catalog::Container::Cmaf { timescale: 1_000_000, track_id: 1 };
      let mut producer = moq_mux::ordered::Producer::new(track, container)
          .with_latency(Duration::from_millis(100));
      producer.write(moq_mux::container::Frame { timestamp, payload, keyframe: true })?;
      ```
      Without `with_latency`, every input frame becomes its own `moof+mdat` fragment.
    - Either way you still need to publish the hang catalog. `moq_mux::catalog::CatalogProducer` is the right tool: call `catalog.lock()`, set the correct `VideoConfig`/`AudioConfig` (including the `container` field), and both the hang and MSF tracks are published on drop of the guard. `moq-mux/src/catalog.rs`.
  - Does streaming CMAF piped from `ffmpeg` to `moq-cli` produce separate audio and video tracks, or a single multiplexed track?
    - It does produce separate tracks, with `container: "cmaf"` encapsulation metadata. Confirmed by the fmp4 importer: one `TrackProducer` per `trak` box in moov.
  - Who is responsible for synchronizing audio and video — the sink or the client?
    - The sink/publisher populates the timestamp. For `legacy`: the publisher writes a VarInt microsecond PTS before every frame payload (`hang/src/container/producer.rs`). For non-passthrough `cmaf`: the fMP4 importer extracts PTS from the trun box (`dts + cts`) and encodes it the same way. For passthrough `cmaf`: timestamps stay embedded in the moof box and the client must parse moof to recover them — no separate frame header is written.
  - With `passthrough: true` in `Fmp4Config`, how can separate tracks still be created if the fMP4 is forwarded as-is without demuxing?
    - Separate tracks are created from the **moov** atom, which is always parsed: each `trak` box yields one `TrackProducer` in the broadcast. Once moof/mdat fragments arrive, passthrough mode writes the entire moof+mdat as a single frame per track (keyed by `track_id` from the traf inside the moof) — so the fragment is never demuxed into individual samples, but routing to the correct track still happens at the fragment level. `moq-mux/src/import/fmp4.rs:559-635`.
