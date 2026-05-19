/// Subscribe to a MoQ broadcast, print its hang catalog, then stream the first track.
///
/// Usage:
///   cargo run -- https://localhost:4443 anon/bbb

use anyhow::Context;
use std::time::Duration;
use url::Url;

/// Set to `true` to skip TLS certificate verification.
///
/// Useful when developing against a local moq-rs relay with a self-signed
/// cert (e.g. `https://localhost:4443`). Must stay `false` for any public
/// relay — otherwise the connection is vulnerable to MITM.
const DISABLE_TLS_VERIFY: bool = true;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let url: Url = args
        .next()
        .unwrap_or_else(|| "https://localhost:4443".into())
        .parse()
        .context("invalid URL")?;
    let broadcast_name = args.next().unwrap_or_else(|| "anon/bbb".into());

    let config = {
        let mut tls = moq_native::ClientTls::default();
        if DISABLE_TLS_VERIFY {
            tls.disable_verify = Some(true);
        }
        let mut config = moq_native::ClientConfig::default();
        config.tls = tls;
        config
    };

    // with_consume: relay pushes announced broadcasts into our OriginProducer (subscriber role)
    let origin = moq_lite::OriginProducer::new();
    let mut announced = origin.consume();

    let client = config.init()?.with_consume(origin);
    let _session = client.connect(url).await.context("failed to connect")?;

    println!("connected, waiting for broadcast {:?}...", broadcast_name);

    // Wait until the relay announces the broadcast we want.
    let broadcast = loop {
        let (path, broadcast) = announced
            .announced()
            .await
            .context("origin closed before broadcast appeared")?;

        println!("announced: {} (active={})", path, broadcast.is_some());

        if path.as_str() == broadcast_name {
            match broadcast {
                Some(b) => break b,
                None => println!("broadcast unannounced, still waiting..."),
            }
        }
    };

    // -------------------------------------------------------------------------
    // Catalog
    // -------------------------------------------------------------------------

    let catalog_track = broadcast
        .subscribe_track(&hang::Catalog::default_track())
        .context("failed to subscribe to catalog track")?;

    let mut catalog_consumer = hang::CatalogConsumer::new(catalog_track);

    println!("subscribed to catalog track, waiting for catalog...");

    let catalog = catalog_consumer
        .next()
        .await?
        .context("catalog track closed with no data")?;

    println!("--- catalog ---");
    println!("{}", catalog.to_string()?);
    println!("---------------");

    // -------------------------------------------------------------------------
    // First available track
    // -------------------------------------------------------------------------

    // Prefer the first video rendition, fall back to first audio rendition.
    let track_name = catalog
        .video
        .renditions
        .keys()
        .next()
        .or_else(|| catalog.audio.renditions.keys().next())
        .context("catalog has no video or audio renditions")?
        .clone();

    println!("subscribing to track {:?}...", track_name);

    let track_consumer = broadcast
        .subscribe_track(&moq_lite::Track {
            name: track_name.clone(),
            priority: 0,
        })
        .context("failed to subscribe to track")?;

    let mut consumer = hang::container::OrderedConsumer::new(track_consumer, Duration::from_secs(5));

    println!("streaming frames from {:?}:", track_name);

    let mut frame_count: u64 = 0;
    while let Some(frame) = consumer.read().await? {
        let ts_ms = frame.timestamp.as_micros() / 1000;
        let keyframe = if frame.is_keyframe() { " [keyframe]" } else { "" };
        println!(
            "  frame {:>5}  pts={:>8}ms  size={:>6} bytes  group={}{keyframe}",
            frame_count,
            ts_ms,
            frame.payload.num_bytes(),
            frame.group,
        );
        frame_count += 1;
    }

    println!("track ended after {} frames", frame_count);

    Ok(())
}
