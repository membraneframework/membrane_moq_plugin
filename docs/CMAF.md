❯ Can you give me an overview of how CMAF works (what moof, mdat, trun, tfdt mean)? Give an external resource if possible

⏺ CMAF (Common Media Application Format, ISO 23000-19) is a profile of fragmented MP4 (fMP4, ISO 14496-12) designed for low-latency
  streaming. Unlike a regular MP4 where all samples are in one big mdat, fMP4 splits the file into small self-contained fragments that
  can be streamed independently.

  Box hierarchy

  A CMAF stream is a sequence of fragments, each structured as:

  moof  ← movie fragment header (metadata for this fragment)
    mfhd  ← sequence number (monotonically increasing, for ordering/error detection)
    traf  ← track fragment (one per track in the fragment)
      tfhd  ← track fragment header (track ID, default sample flags)
      tfdt  ← track fragment decode time (base DTS for this fragment)
      trun  ← track run (per-sample table: size, duration, flags, CTS offset)
  mdat  ← actual sample bytes, laid out in the order trun describes

  What each box means

  moof — "movie fragment". The metadata envelope for one fragment. Contains no media bytes itself — it just describes what's in the mdat
  that immediately follows it.

  mfhd — sequence number. Starts at 1, increments per fragment. Lets receivers detect gaps.

  traf — "track fragment". One traf per track per fragment (so a muxed audio+video fragment would have two trafs; with separate MoQ
  tracks there's always exactly one).

  tfhd — the track ID and default per-sample values (size, duration, flags) so trun entries can omit them if they're constant.

  tfdt — "track fragment decode time". Gives the DTS (decode timestamp) of the first sample in this fragment, in the track's timescale
  units. This is what lets a receiver seeking into the middle of a stream know when this fragment starts without parsing everything
  before it.

  trun — "track run". The per-sample table. Each entry can carry:
  - size — byte length of this sample in mdat
  - duration — duration in timescale units
  - flags — whether it's a keyframe (the depends_on_no_other bits)
  - cts — CTS offset (composition time offset): PTS = DTS + CTS. For codecs with B-frames (H.264 with B-frames, HEVC), DTS ≠ PTS — CTS
  carries that difference. For audio and I/P-only video, CTS is 0.

  mdat — raw sample bytes, concatenated in the same order as trun entries. The data_offset field in trun gives the byte offset from the
  start of moof to the start of the sample data in mdat.

  Timescale

  Timestamps in tfdt/trun are in timescale units, not microseconds. The timescale (ticks/second) is declared in the mdhd box inside the
  static moov initialization segment that precedes all fragments. Common values: 90000 (video), 44100/48000 (audio), or 1000000
  (microseconds — what moq-mux/src/cmaf/container.rs uses when encoding from the hang Timescale<1_000_000>).

  The static moov init segment

  Before any fragments there's a regular moov box (no moof) that declares codec parameters, timescale, and track IDs. In MoQ hang this is
   sent as a separate description/init_data field in the catalog, not as a track frame.

  External resources

  The authoritative spec is ISO 14496-12, but that's paywalled. Better practical references:
  - Bento4 MP4 explained — good box-by-box walkthrough
  - MDN: Media Source Extensions / fMP4 — browser-centric but clear on fragment structure
  - Carsten Griwodz's fMP4 cheat sheet — interactive: drop any fMP4 and it renders the box tree
  - ISO BMFF byte stream spec (W3C)](https://www.w3.org/TR/mse-byte-stream-format-isobmff/) — free, short, describes exactly the
  moof+mdat streaming pattern

  The GPAC mp4box -info tool (or mp4dump from Bento4) is the fastest way to inspect real fragments if you're debugging — it prints the
  full box tree with field values.


