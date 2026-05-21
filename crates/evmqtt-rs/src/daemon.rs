use crate::config::{HassConfig, MqttConfig};
use crate::coordinator::Coordinator;
use crate::db::Database;
use crate::mqtt::{graceful_shutdown, spawn as spawn_mqtt, wait_for_permanent_failure};
use crate::topics::{
    PAYLOAD_AVAILABLE, PAYLOAD_NOT_AVAILABLE, availability_topic, device_enabled_wildcard,
};
use crate::watcher::run_watcher;
use anyhow::Result;
use std::future::Future;
use std::path::PathBuf;
use std::process;
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

pub async fn run(mqtt_cfg: MqttConfig, hass_cfg: HassConfig, db_path: PathBuf) -> Result<()> {
    run_with_shutdown(mqtt_cfg, hass_cfg, db_path, wait_for_shutdown()).await
}

/// Same as [`run`] but with an injected shutdown future. Tests use
/// this to drive a controlled exit without sending real signals.
pub async fn run_with_shutdown<F>(
    mqtt_cfg: MqttConfig,
    hass_cfg: HassConfig,
    db_path: PathBuf,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let db = Database::load(&db_path)?;
    info!(
        db = %db_path.display(),
        devices = db.devices.len(),
        "loaded device database",
    );

    let availability = availability_topic(&mqtt_cfg.topic_prefix);
    let client_id = build_client_id(&mqtt_cfg.client_id_prefix);
    let runtime = spawn_mqtt(&mqtt_cfg, client_id, availability.clone());

    // Wait for CONNACK (or fail fast on a permanent error like
    // bad credentials). 30 s is generous so the systemd service can
    // come up before the network is fully online without flapping.
    runtime
        .wait_ready(Duration::from_secs(30))
        .await
        .map_err(|e| e.context("daemon connecting to MQTT broker"))?;

    let handle = runtime.handle.clone();
    let mqtt_incoming = runtime.incoming;
    let mqtt_event_task = runtime.event_task;
    let mqtt_shutdown_flag = runtime.shutdown_flag;
    let mqtt_conn_state = runtime.conn_state;

    if let Err(e) = handle
        .publish_str(&availability, PAYLOAD_AVAILABLE, true)
        .await
    {
        warn!(error = %e, "failed to publish online announcement");
    }

    // Subscribe to enabled-state commands before doing the initial
    // sweep so retained enabled-topic messages are delivered first.
    let wildcard = device_enabled_wildcard(&mqtt_cfg.topic_prefix);
    if let Err(e) = handle.subscribe(&wildcard).await {
        warn!(error = %e, "failed to subscribe to enabled wildcard");
    }
    // Hand the broker enough time to deliver retained messages before
    // the watcher starts firing DeviceConnected events; otherwise an
    // outstanding "remove" (empty retained on enabled topic) might
    // race with a fresh sighting of the same device.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (coord_tx, coord_rx) = mpsc::unbounded_channel();
    let coordinator = Coordinator::new(
        db,
        db_path,
        handle.clone(),
        mqtt_cfg.clone(),
        hass_cfg.clone(),
        coord_tx.clone(),
    );
    let coord_task = tokio::spawn(async move {
        coordinator.run(coord_rx, mqtt_incoming).await;
    });

    // Watcher shutdown is a separate oneshot so we can hand the same
    // application shutdown future into both watcher and "after this
    // resolves, stop everything".
    let (watcher_shutdown_tx, watcher_shutdown_rx) = oneshot::channel::<()>();
    let watcher_handle = tokio::spawn(run_watcher(coord_tx.clone(), async move {
        let _ = watcher_shutdown_rx.await;
    }));

    info!("daemon ready");
    // Either the externally-provided shutdown future fires (SIGINT/
    // SIGTERM in production, test harness in tests) or the eventloop
    // transitions to PermanentFailure (broker keeps rejecting us
    // after we were once connected). Both paths drop into the same
    // shutdown sequence below.
    tokio::pin!(shutdown);
    tokio::select! {
        _ = &mut shutdown => info!("daemon shutdown signal received"),
        _ = wait_for_permanent_failure(mqtt_conn_state) => {
            error!("MQTT connection permanently failed; daemon exiting");
        }
    }

    let _ = watcher_shutdown_tx.send(());
    // Drop the coordinator's tx clone so the channel closes once the
    // watcher exits.
    drop(coord_tx);
    if let Err(e) = watcher_handle.await {
        warn!(error = %e, "watcher join error");
    }

    if let Err(e) = handle
        .publish_str(&availability, PAYLOAD_NOT_AVAILABLE, true)
        .await
    {
        warn!(error = %e, "failed to publish offline announcement");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Disconnect cleanly: flip the eventloop's shutdown flag (so it
    // ignores the "Connection closed by peer abruptly" that follows
    // our own DISCONNECT), send DISCONNECT, wait briefly for the
    // eventloop task to exit.
    graceful_shutdown(handle, mqtt_event_task, mqtt_shutdown_flag).await;
    coord_task.abort();
    let _ = coord_task.await;
    Ok(())
}

fn build_client_id(prefix: &str) -> String {
    let host = hostname().unwrap_or_else(|| "unknown".to_string());
    format!("{prefix}-{host}-{}", process::id())
}

fn hostname() -> Option<String> {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .and_then(|s| {
            let t = s.trim();
            if t.is_empty() {
                None
            } else {
                Some(t.to_string())
            }
        })
        .or_else(|| std::env::var("HOSTNAME").ok())
}

async fn wait_for_shutdown() {
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "could not install SIGTERM handler; will only handle ctrl-c");
            tokio::signal::ctrl_c().await.ok();
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm.recv() => {},
    }
}
