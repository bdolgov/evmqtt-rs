use crate::coordinator::{CoordinatorMsg, CoordinatorTx};
use crate::discovery::DeviceIdentity;
use crate::mqtt::MqttHandle;
use crate::slug::key_slug;
use crate::topics::Action;
use evdev::{Device, EventSummary, KeyCode};
use std::io;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, trace, warn};

/// Coordinator-held handle to a running monitor task.
pub struct MonitorHandle {
    pub join: JoinHandle<()>,
    pub shutdown: oneshot::Sender<()>,
}

/// Spawn a monitor task for one device. Returns the handle the
/// coordinator stores to either await the task or ask it to exit.
pub fn start(
    handle: MqttHandle,
    action_topic: Arc<str>,
    slug: String,
    identity: DeviceIdentity,
    coordinator: CoordinatorTx,
) -> MonitorHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        run_device(handle, action_topic, slug, identity, coordinator, shutdown_rx).await;
    });
    MonitorHandle {
        join,
        shutdown: shutdown_tx,
    }
}

/// Drive one device's event stream. Returns when:
/// - the device disappears (ENODEV / stream end) -- sends
///   `DeviceDisconnected` first.
/// - the `shutdown` oneshot fires -- does NOT send disconnect; the
///   coordinator already knows.
pub async fn run_device(
    handle: MqttHandle,
    action_topic: Arc<str>,
    slug: String,
    identity: DeviceIdentity,
    coordinator: CoordinatorTx,
    shutdown: oneshot::Receiver<()>,
) {
    let path = identity.path.clone();
    let name = identity.name.clone();

    let mut device = match Device::open(&path) {
        Ok(d) => d,
        Err(e) => {
            error!(path = %path.display(), error = %e, "could not open device");
            let _ = coordinator.send(CoordinatorMsg::DeviceDisconnected { slug });
            return;
        }
    };

    if let Err(e) = device.grab() {
        warn!(
            path = %path.display(),
            error = %e,
            "failed to grab device exclusively; events may also reach the console",
        );
    } else {
        info!(path = %path.display(), name = %name, %slug, "grabbed device for exclusive use");
    }

    let mut stream = match device.into_event_stream() {
        Ok(s) => s,
        Err(e) => {
            error!(path = %path.display(), error = %e, "could not enter async event stream");
            let _ = coordinator.send(CoordinatorMsg::DeviceDisconnected { slug });
            return;
        }
    };

    info!(name = %name, path = %path.display(), %slug, "monitoring device for key events");

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!(%slug, "monitor received shutdown");
                return;
            }
            event_result = stream.next() => {
                match event_result {
                    Some(Ok(ev)) => {
                        match ev.destructure() {
                            EventSummary::Key(_, key, value) => {
                                trace!(?key, value, "key event");
                                handle_key_event(
                                    &handle,
                                    &action_topic,
                                    &slug,
                                    &coordinator,
                                    key,
                                    value,
                                ).await;
                            }
                            _ => trace!(event = ?ev, "non-key event"),
                        }
                    }
                    Some(Err(e)) => {
                        if is_disconnect(&e) {
                            info!(path = %path.display(), %name, "device disconnected; monitor exiting");
                        } else {
                            error!(path = %path.display(), error = %e, "error reading event; stopping monitor");
                        }
                        let _ = coordinator.send(CoordinatorMsg::DeviceDisconnected { slug });
                        return;
                    }
                    None => {
                        info!(%name, path = %path.display(), "device stream ended; monitor exiting");
                        let _ = coordinator.send(CoordinatorMsg::DeviceDisconnected { slug });
                        return;
                    }
                }
            }
        }
    }
}

fn is_disconnect(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc_enodev::ENODEV) | Some(libc_enodev::ENXIO)
    )
}

mod libc_enodev {
    pub const ENODEV: i32 = 19;
    pub const ENXIO: i32 = 6;
}

/// Classify an evdev key event's `value` field.
///
/// evdev encodes a key event's phase as: `0` = release, `1` = press,
/// `2` = autorepeat. HA device triggers are momentary events with no
/// concept of "still held", so autorepeat is ignored.
pub fn evdev_value_to_action(value: i32) -> Option<Action> {
    match value {
        1 => Some(Action::Press),
        0 => Some(Action::Release),
        _ => None,
    }
}

async fn handle_key_event(
    handle: &MqttHandle,
    action_topic: &str,
    slug: &str,
    coordinator: &CoordinatorTx,
    key: KeyCode,
    value: i32,
) {
    let Some(action) = evdev_value_to_action(value) else {
        return;
    };
    let key_name = format!("{:?}", key);
    let kslug = key_slug(&key_name);
    let payload = format!("{kslug}_{}", action.as_str());

    if let Err(e) = handle.publish_str(action_topic, &payload, false).await {
        error!(error = %e, topic = %action_topic, %payload, "publish failed");
    } else {
        debug!(topic = %action_topic, %payload, "trigger published");
    }
    // Tell the coordinator regardless of publish success -- it dedupes
    // and persists the observed-keys list.
    let _ = coordinator.send(CoordinatorMsg::KeyObserved {
        slug: slug.to_string(),
        code: key.code(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn press_release_classified() {
        assert_eq!(evdev_value_to_action(1), Some(Action::Press));
        assert_eq!(evdev_value_to_action(0), Some(Action::Release));
    }

    #[test]
    fn autorepeat_ignored() {
        assert_eq!(evdev_value_to_action(2), None);
        assert_eq!(evdev_value_to_action(42), None);
    }

    #[test]
    fn enodev_is_classified_as_disconnect() {
        let e = io::Error::from_raw_os_error(libc_enodev::ENODEV);
        assert!(is_disconnect(&e));
        let e = io::Error::from_raw_os_error(libc_enodev::ENXIO);
        assert!(is_disconnect(&e));
        let e = io::Error::from_raw_os_error(1); // EPERM
        assert!(!is_disconnect(&e));
    }
}
