- Should the Sink correspond to a single moq-lite Session, spawning a single Origin, but accepting multiple tracks scoped to independent broadcasts?
  - A: Decided on configuring the broadcast per-pad, this is the most configurable. TODO: remove the default fallback broadcast and track names, require configuring broadcast for each added pad explicitly.
- OBS only supports `container: "legacy"`. I'd add it as a Sink-scoped option whether to use `"legacy"` or `"cmaf"`. Create a `moq_mux::container::Container` conditionally based on the value.
  - A: Scraped for now, see `./EXTENDING_TO_CMAF.md` for rationale.
- How many Rustler async runtimes should be running? Need a threading model design!!! Is one async runtime per pad too fine-grained? Remember that we're sharing the session, origin and CatalogProducer!
  - TODO: review generated code. The runtimes created should represend thread pools, which is flexible enough.

- Does the relay have persistence logic or does it flush unconditionally? Pipelines without a realtimer are not testable manually. Should subscribers like `web` or `OBS` respect the legacy container's timestamp encapsulation? Is it payloaded properly by `Membrane.MoQ.Sink`?
- Test the MSF cataloguing - it's supposed to be superseding hang's catalog.js track.

- Monitoring/inspection scripts for looking up current state of the catalog meta-tracks, currently `./membrane_moq_plugin/standalone` does a good job.
- Manual example tests: not very flexible now, should test more formats like Opus and H265 manually, maybe add a control button to enable playback so the tracks can be inspected in the catalogs, and also inspect termination conditions more thoroughly.
- Automated tests: configurable relay, mocking the rust layer was a good idea by the LLM
- Add TLS support!!
