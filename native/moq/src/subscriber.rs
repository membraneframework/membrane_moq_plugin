use std::collections::HashMap;
use std::time::Duration;

use url::Url;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use rustler::{Atom, LocalPid, NifResult, Resource, ResourceArc};

use hang::moq_net;
use moq_native::ClientConfig;

use crate::messages::{self, Token};
use crate::track_format::{audio_params, video_params, TrackEntry, TrackParams};
use crate::{atoms, runtime};

enum Command {
    Subscribe { track: String, token: Token },
    Unsubscribe { token: Token },
}

pub(crate) struct SubscriberResource {
    commands: mpsc::UnboundedSender<Command>,
    shutdown: mpsc::UnboundedSender<()>,
}

impl Resource for SubscriberResource {}

#[rustler::nif]
pub(crate) fn start_subscriber(
    url: String,
    broadcast: String,
    pid: LocalPid,
    disable_tls_verify: bool,
    latency_ns: u64,
) -> NifResult<(Atom, ResourceArc<SubscriberResource>)> {
    let url = Url::parse(&url).map_err(|e| crate::nif_error!("invalid url: {e}"))?;
    let latency = Duration::from_nanos(latency_ns);

    let (commands_tx, commands_rx) = mpsc::unbounded_channel::<Command>();
    let (shutdown_tx, shutdown_rx) = mpsc::unbounded_channel::<()>();

    runtime().spawn(async move {
        let config = crate::session::client_config(disable_tls_verify);

        let result = run_session(
            url,
            broadcast,
            &pid,
            config,
            latency,
            commands_rx,
            shutdown_rx,
        )
        .await;

        if let Err(e) = result {
            messages::send_setup_failed(&pid, e.to_string());
        }
    });

    Ok((
        atoms::ok(),
        ResourceArc::new(SubscriberResource {
            commands: commands_tx,
            shutdown: shutdown_tx,
        }),
    ))
}

#[rustler::nif]
pub(crate) fn subscribe_track(
    subscriber: ResourceArc<SubscriberResource>,
    track: String,
    token: Token,
) -> Atom {
    let _ = subscriber
        .commands
        .send(Command::Subscribe { track, token });
    atoms::ok()
}

#[rustler::nif]
pub(crate) fn unsubscribe_track(subscriber: ResourceArc<SubscriberResource>, token: Token) -> Atom {
    let _ = subscriber.commands.send(Command::Unsubscribe { token });
    atoms::ok()
}

#[rustler::nif]
pub(crate) fn stop_subscriber(subscriber: ResourceArc<SubscriberResource>) -> Atom {
    let _ = subscriber.shutdown.send(());
    atoms::ok()
}

async fn run_session(
    url: Url,
    broadcast_name: String,
    pid: &LocalPid,
    config: ClientConfig,
    latency: Duration,
    mut commands_rx: mpsc::UnboundedReceiver<Command>,
    mut shutdown_rx: mpsc::UnboundedReceiver<()>,
) -> anyhow::Result<()> {
    let origin = moq_net::Origin::random().produce();
    let mut origin_consumer = origin.consume();

    let client = config.init()?.with_consume(origin);
    let session = client.connect(url).await?;

    let broadcast = tokio::select! {
        broadcast = await_broadcast(&mut origin_consumer, &broadcast_name) => {
            broadcast.ok_or_else(|| anyhow::anyhow!(
                "broadcast {broadcast_name:?} was not announced before origin closed"
            ))?
        }
        _ = shutdown_rx.recv() => return Ok(()),
    };

    messages::send_connected(pid);

    let mut catalog = subscribe_catalog(&broadcast)?;

    let mut pumps: JoinSet<()> = JoinSet::new();
    let mut cancels: HashMap<Token, watch::Sender<bool>> = HashMap::new();
    // Latest codec params per track name, from the most recent catalog snapshot.
    let mut params: HashMap<String, TrackParams> = HashMap::new();
    // Subscribe commands whose track has not yet appeared in the catalog. Held
    // here (with their cancel receiver) until a snapshot advertises the track.
    let mut pending: HashMap<Token, (String, watch::Receiver<bool>)> = HashMap::new();

    let disconnect_reason = loop {
        tokio::select! {
            command = commands_rx.recv() => match command {
                Some(Command::Subscribe { track, token }) => {
                    let (cancel_tx, cancel_rx) = watch::channel(false);
                    cancels.insert(token, cancel_tx);
                    // Only subscribe to the track once the catalog advertises it:
                    // publishers create tracks lazily on the first stream_format,
                    // so subscribing earlier races the announcement and the relay
                    // answers NotFound. If we already have params, start now;
                    // otherwise park the command until a snapshot resolves it.
                    match params.get(&track) {
                        Some(track_params) => {
                            messages::send_track_format(pid, token, track_params);
                            pumps.spawn(run_pump(
                                broadcast.clone(), track, token, *pid, latency, cancel_rx,
                            ));
                        }
                        None => {
                            pending.insert(token, (track, cancel_rx));
                        }
                    }
                }
                Some(Command::Unsubscribe { token }) => {
                    if let Some(cancel) = cancels.remove(&token) {
                        let _ = cancel.send(true);
                    }
                    pending.remove(&token);
                }
                // The resource was dropped without a stop; treat as shutdown.
                None => return Ok(()),
            },
            _ = shutdown_rx.recv() => return Ok(()),
            _ = pumps.join_next(), if !pumps.is_empty() => {}
            snapshot = catalog.next() => {
                let snapshot = match snapshot {
                    Ok(Some(snapshot)) => snapshot,
                    Ok(None) => break "broadcast ended".to_string(),
                    Err(e) => break format!("catalog error: {e}"),
                };

                params = catalog_params(&snapshot);

                let entries: Vec<TrackEntry> = params
                    .iter()
                    .map(|(name, params)| TrackEntry {
                        name: name.clone(),
                        params: params.clone(),
                    })
                    .collect();
                if messages::send_tracks(pid, &entries).is_err() {
                    break "subscriber pid is dead".to_string();
                }

                // Start pumps for any parked commands the snapshot just resolved.
                let ready: Vec<Token> = pending
                    .iter()
                    .filter(|(_, (track, _))| params.contains_key(track))
                    .map(|(token, _)| *token)
                    .collect();
                for token in ready {
                    let (track, cancel_rx) = pending.remove(&token).unwrap();
                    messages::send_track_format(pid, token, &params[&track]);
                    pumps.spawn(run_pump(
                        broadcast.clone(), track, token, *pid, latency, cancel_rx,
                    ));
                }
            }
            result = session.closed() => break match result {
                Ok(()) => "session closed gracefully".to_string(),
                Err(e) => format!("session error: {e}"),
            },
        }
    };

    messages::send_disconnected(pid, disconnect_reason);
    Ok(())
}

async fn run_pump(
    broadcast: moq_net::BroadcastConsumer,
    track_name: String,
    token: Token,
    pid: LocalPid,
    latency: Duration,
    mut cancel: watch::Receiver<bool>,
) {
    let reason = tokio::select! {
        _ = cancel.changed() => return,
        result = pump_track(&broadcast, &track_name, token, &pid, latency) => match result {
            Ok(()) => "track ended".to_string(),
            Err(e) => format!("track error: {e}"),
        }
    };

    messages::send_track_ended(&pid, token, reason);
}

async fn pump_track(
    broadcast: &moq_net::BroadcastConsumer,
    track_name: &str,
    token: Token,
    pid: &LocalPid,
    latency: Duration,
) -> anyhow::Result<()> {
    // The caller only spawns a pump once the catalog advertises this track, so
    // the subscribe below no longer races the announcement, and the stream
    // format has already been sent to Elixir from the catalog params.
    let track_ref = moq_net::Track {
        name: track_name.to_string(),
        priority: 0,
    };
    let track_consumer = broadcast
        .subscribe_track(&track_ref)
        .map_err(|e| anyhow::anyhow!("subscribe_track({track_name}) failed: {e}"))?;

    let mut consumer =
        moq_mux::container::Consumer::new(track_consumer, moq_mux::container::legacy::Wire)
            .with_latency(latency);

    pump_frames(&mut consumer, token, pid).await
}

async fn await_broadcast(
    consumer: &mut moq_net::OriginConsumer,
    name: &str,
) -> Option<moq_net::BroadcastConsumer> {
    while let Some((path, broadcast)) = consumer.announced().await {
        if path.as_str() == name {
            if let Some(broadcast) = broadcast {
                return Some(broadcast);
            }
        }
    }
    None
}

fn subscribe_catalog(
    broadcast: &moq_net::BroadcastConsumer,
) -> anyhow::Result<moq_mux::catalog::hang::Consumer<()>> {
    let catalog_track = broadcast
        .subscribe_track(&hang::Catalog::default_track())
        .map_err(|e| anyhow::anyhow!("subscribe_track(catalog) failed: {e}"))?;
    Ok(moq_mux::catalog::hang::Consumer::<()>::new(catalog_track))
}

/// Flatten a catalog snapshot into per-track codec params. Unrecognized codecs
/// still get an entry (as [`TrackParams::Unrecognized`]) so the track list Elixir
/// sees matches what the broadcast advertises.
fn catalog_params(snapshot: &moq_mux::catalog::hang::Catalog) -> HashMap<String, TrackParams> {
    let mut params = HashMap::new();
    for (name, config) in &snapshot.video.renditions {
        params.insert(name.clone(), video_params(config));
    }
    for (name, config) in &snapshot.audio.renditions {
        params.insert(name.clone(), audio_params(config));
    }
    params
}

async fn pump_frames(
    consumer: &mut moq_mux::container::Consumer<moq_mux::container::legacy::Wire>,
    token: Token,
    pid: &LocalPid,
) -> anyhow::Result<()> {
    while let Some(frame) = consumer.read().await? {
        // u128 → i64: real-world frame timestamps fit. If a stream somehow
        // overflows i64 microseconds (~292,000 years), the cast wraps and the
        // Elixir side sees a meaningless value, but no UB.
        let timestamp_us = frame.timestamp.as_micros() as i64;

        messages::send_frame(pid, token, &frame.payload, timestamp_us, frame.keyframe)
            .map_err(|_| anyhow::anyhow!("subscriber pid is dead"))?;
    }
    Ok(())
}
