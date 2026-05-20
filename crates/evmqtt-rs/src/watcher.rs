use crate::config::{DeviceConfig, HassConfig};
use crate::discovery::{DeviceIdentity, enumerate_identities, open_identity};
use crate::monitor::run_device;
use crate::mqtt::MqttHandle;
use futures_util::StreamExt;
use inotify::{Inotify, WatchMask};
use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

const INPUT_DIR: &str = "/dev/input";

/// Drive the hotplug-aware device lifecycle. Existing `/dev/input/event*`
/// devices are picked up on entry; new ones are picked up when inotify
/// fires on `/dev/input/`. When a device disappears its monitor task
/// returns and its config slot becomes available for the next reconnect.
///
/// `shutdown` is awaited concurrently — when it resolves the watcher
/// aborts all live monitor tasks and returns.
pub async fn run_watcher<F>(
    devices: Vec<DeviceConfig>,
    hass: HassConfig,
    topic_prefix: String,
    handle: MqttHandle,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send,
{
    if devices.is_empty() {
        // app::run is responsible for the empty-config detect-and-exit path;
        // reaching here with no devices is a programmer error.
        return Err(anyhow::anyhow!(
            "run_watcher called with no configured devices"
        ));
    }

    let inotify = Inotify::init()?;
    inotify
        .watches()
        .add(INPUT_DIR, WatchMask::CREATE | WatchMask::ATTRIB)?;

    let mut attached: Vec<bool> = vec![false; devices.len()];
    let mut tasks: JoinSet<usize> = JoinSet::new();

    // Initial sweep so devices that were already present at startup get
    // attached without waiting for an inotify event.
    for identity in enumerate_identities() {
        try_attach(
            &devices,
            &hass,
            &topic_prefix,
            &handle,
            &mut attached,
            &mut tasks,
            identity,
        );
    }

    let mut buffer = [0u8; 4096];
    let mut events = inotify.into_event_stream(&mut buffer[..])?;
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; stopping device watcher");
                break;
            }
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(ev)) => {
                        if let Some(path) = event_path(ev.name.as_deref()) {
                            // Open with a tiny retry to ride out the
                            // udev CREATE-then-chown race; if the device
                            // is still unreadable we'll see another ATTRIB.
                            if let Some(identity) = open_with_retry(&path).await {
                                try_attach(
                                    &devices,
                                    &hass,
                                    &topic_prefix,
                                    &handle,
                                    &mut attached,
                                    &mut tasks,
                                    identity,
                                );
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!(error = %e, "inotify read error");
                    }
                    None => {
                        warn!("inotify stream ended unexpectedly");
                        break;
                    }
                }
            }
            Some(joined) = tasks.join_next(), if !tasks.is_empty() => {
                match joined {
                    Ok(cfg_idx) => {
                        attached[cfg_idx] = false;
                        debug!(cfg_idx, "monitor task finished; slot is free again");
                    }
                    Err(e) if e.is_cancelled() => {
                        debug!("monitor task cancelled");
                    }
                    Err(e) => {
                        warn!(error = %e, "monitor task join error");
                    }
                }
            }
        }
    }

    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

fn try_attach(
    devices: &[DeviceConfig],
    hass: &HassConfig,
    topic_prefix: &str,
    handle: &MqttHandle,
    attached: &mut [bool],
    tasks: &mut JoinSet<usize>,
    identity: DeviceIdentity,
) {
    let Some(cfg_idx) = devices.iter().position(|d| identity.matches(&d.matcher)) else {
        info!(
            path = %identity.path.display(),
            name = %identity.name,
            "no configured device matches; skipping",
        );
        return;
    };
    if attached[cfg_idx] {
        debug!(
            path = %identity.path.display(),
            cfg_name = %devices[cfg_idx].name,
            "configured device is already attached; ignoring duplicate match",
        );
        return;
    }
    info!(
        path = %identity.path.display(),
        name = %identity.name,
        cfg_name = %devices[cfg_idx].name,
        mqtt_path = %devices[cfg_idx].resolved_mqtt_path(),
        "attaching device",
    );
    attached[cfg_idx] = true;
    let cfg = devices[cfg_idx].clone();
    let hass = hass.clone();
    let topic_prefix = topic_prefix.to_string();
    let handle = handle.clone();
    tasks.spawn(async move {
        run_device(handle, topic_prefix, hass, cfg, identity).await;
        cfg_idx
    });
}

async fn open_with_retry(path: &Path) -> Option<DeviceIdentity> {
    if let Some(id) = open_identity(path) {
        return Some(id);
    }
    // Single short retry to ride out the CREATE → chown race on hotplug.
    tokio::time::sleep(Duration::from_millis(50)).await;
    open_identity(path)
}

/// Map an inotify event's `name` field (a basename inside /dev/input)
/// to a full path, but only when it looks like an `event*` node.
fn event_path(name: Option<&OsStr>) -> Option<PathBuf> {
    let raw = name?.to_str()?;
    if is_event_node(raw) {
        Some(PathBuf::from(INPUT_DIR).join(raw))
    } else {
        None
    }
}

fn is_event_node(name: &str) -> bool {
    name.strip_prefix("event")
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn attached_indices(attached: &[bool]) -> HashSet<usize> {
        attached
            .iter()
            .enumerate()
            .filter_map(|(i, &a)| if a { Some(i) } else { None })
            .collect()
    }

    #[test]
    fn is_event_node_accepts_event_digits() {
        assert!(is_event_node("event0"));
        assert!(is_event_node("event42"));
    }

    #[test]
    fn is_event_node_rejects_others() {
        assert!(!is_event_node("event"));
        assert!(!is_event_node("eventX"));
        assert!(!is_event_node("mice"));
        assert!(!is_event_node("js0"));
        assert!(!is_event_node("by-id"));
    }

    #[test]
    fn event_path_builds_full_path() {
        assert_eq!(
            event_path(Some(OsStr::new("event3"))),
            Some(PathBuf::from("/dev/input/event3"))
        );
    }

    #[test]
    fn event_path_skips_non_event_nodes() {
        assert_eq!(event_path(Some(OsStr::new("mice"))), None);
        assert_eq!(event_path(None), None);
    }

    #[test]
    fn attached_indices_reports_busy_slots() {
        let attached = vec![false, true, false, true];
        let busy = attached_indices(&attached);
        assert!(busy.contains(&1));
        assert!(busy.contains(&3));
        assert_eq!(busy.len(), 2);
    }
}
