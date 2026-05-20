use crate::config::Config;
use crate::discovery::write_detect_snippets;
use crate::mqtt::{announce_offline, announce_online, spawn as spawn_mqtt};
use crate::topics::availability_topic;
use crate::watcher::run_watcher;
use anyhow::Result;
use std::future::Future;
use std::io::{self, Write};
use std::process;
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{info, warn};

pub async fn run(config: Config) -> Result<()> {
    run_with_shutdown(config, wait_for_shutdown()).await
}

/// Same as [`run`] but with an injected shutdown future. Tests use this
/// to drive a controlled exit without sending real signals.
pub async fn run_with_shutdown<F>(config: Config, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send,
{
    if config.devices.is_empty() {
        return run_empty_config_detect();
    }

    let availability = availability_topic(&config.mqtt.topic_prefix);
    let client_id = build_client_id(&config.mqtt.client_id_prefix);

    let mqtt = spawn_mqtt(&config.mqtt, client_id, availability.clone());
    let handle = mqtt.handle.clone();

    // Give the eventloop a moment to CONNECT before publishing the first
    // retained "online" announcement.
    tokio::time::sleep(Duration::from_millis(200)).await;
    if let Err(e) = announce_online(&handle, &availability).await {
        warn!(error = %e, "failed to publish online announcement");
    }

    info!(
        configured = config.devices.len(),
        "starting hotplug device watcher",
    );

    let watcher_result = run_watcher(
        config.devices.clone(),
        config.hass.clone(),
        config.mqtt.topic_prefix.clone(),
        handle.clone(),
        shutdown,
    )
    .await;

    if let Err(e) = announce_offline(&handle, &availability).await {
        warn!(error = %e, "failed to publish offline announcement");
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    if let Err(e) = handle.disconnect().await {
        warn!(error = %e, "disconnect error");
    }
    mqtt.event_task.abort();

    watcher_result
}

/// First-run hint when `[[device]]` is empty: print a notice and the
/// detected-device TOML snippets, then exit successfully. This is intended
/// as a setup aid — the user pastes the snippets into config and re-runs.
fn run_empty_config_detect() -> Result<()> {
    eprintln!(
        "no devices configured in [[device]]. Detected devices follow; \
         copy the matching snippets into your config and re-run."
    );
    eprintln!();
    let stderr = io::stderr();
    let mut lock = stderr.lock();
    let count = write_detect_snippets(&mut lock)?;
    lock.flush().ok();
    drop(lock);
    if count == 0 {
        eprintln!();
        eprintln!(
            "no input devices were visible to this process. Check that the \
             user has read access to /dev/input/event* (usually via the \
             `input` group)."
        );
    }
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
