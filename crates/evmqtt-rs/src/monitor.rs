use crate::config::{DeviceConfig, HassConfig};
use crate::discovery::DeviceIdentity;
use crate::mqtt::Publisher;
use crate::slug::key_slug;
use crate::topics::{
    Action, DeviceInfo, OriginInfo, TriggerDiscovery, TriggerTopics, device_identifier,
    trigger_topics,
};
use evdev::{Device, EventSummary, KeyCode};
use std::collections::HashSet;
use std::io;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, trace, warn};

/// Drive a single device's event stream until the device disappears or the
/// task is cancelled. Returns when the underlying `/dev/input/eventN` ends
/// the stream (normal disconnect) or errors out (unexpected read failure).
pub async fn run_device<P: Publisher>(
    handle: P,
    topic_prefix: String,
    hass: HassConfig,
    cfg: DeviceConfig,
    identity: DeviceIdentity,
) {
    let path = identity.path.clone();
    let name = identity.name.clone();
    let device_path_slug = cfg.resolved_mqtt_path();

    let mut device = match Device::open(&path) {
        Ok(d) => d,
        Err(e) => {
            error!(path = %path.display(), error = %e, "could not open device");
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
        info!(path = %path.display(), name = %name, mqtt_path = %device_path_slug, "grabbed device for exclusive use");
    }

    let mut stream = match device.into_event_stream() {
        Ok(s) => s,
        Err(e) => {
            error!(path = %path.display(), error = %e, "could not enter async event stream");
            return;
        }
    };

    let mut announced: HashSet<(KeyCode, Action)> = HashSet::new();

    info!(
        name = %name,
        path = %path.display(),
        mqtt_path = %device_path_slug,
        "monitoring device for key events",
    );

    while let Some(event_result) = stream.next().await {
        let event = match event_result {
            Ok(e) => e,
            Err(e) => {
                if is_disconnect(&e) {
                    info!(
                        path = %path.display(),
                        name = %name,
                        "device disconnected; monitor exiting",
                    );
                } else {
                    error!(
                        path = %path.display(),
                        error = %e,
                        "error reading event; stopping monitor",
                    );
                }
                return;
            }
        };

        match event.destructure() {
            EventSummary::Key(_, key, value) => {
                trace!(?key, value, "key event");
                handle_key_event(
                    &handle,
                    &topic_prefix,
                    &hass,
                    &cfg,
                    &identity,
                    &mut announced,
                    key,
                    value,
                )
                .await;
            }
            _ => {
                trace!(event = ?event, "non-key event");
            }
        }
    }

    info!(
        name = %name,
        path = %path.display(),
        "device stream ended; monitor exiting",
    );
}

fn is_disconnect(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc_enodev::ENODEV) | Some(libc_enodev::ENXIO)
    )
}

mod libc_enodev {
    // Avoid pulling in libc just for these constants.
    pub const ENODEV: i32 = 19;
    pub const ENXIO: i32 = 6;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_key_event<P: Publisher>(
    handle: &P,
    topic_prefix: &str,
    hass: &HassConfig,
    cfg: &DeviceConfig,
    identity: &DeviceIdentity,
    announced: &mut HashSet<(KeyCode, Action)>,
    key: KeyCode,
    value: i32,
) {
    let action = match value {
        1 => Action::Press,
        0 => Action::Release,
        // Autorepeat — HA device_triggers are momentary events; ignore.
        _ => return,
    };

    let key_name = format!("{:?}", key);
    let kslug = key_slug(&key_name);
    let device_slug = cfg.resolved_mqtt_path();
    let topics = trigger_topics(
        &hass.discovery_prefix,
        topic_prefix,
        &device_slug,
        &kslug,
        action,
    );

    if hass.enabled && !announced.contains(&(key, action)) {
        debug!(
            device = %device_slug,
            key = %key_name,
            action = %action.as_str(),
            unique_id = %topics.unique_id,
            "registering new device_trigger",
        );
        if let Err(e) = publish_discovery(
            handle,
            topic_prefix,
            hass,
            cfg,
            identity,
            &kslug,
            action,
            &topics,
        )
        .await
        {
            error!(error = %e, key = %key_name, "failed to publish discovery payload");
            return;
        }
        announced.insert((key, action));
    }

    if let Err(e) = handle
        .publish_str(&topics.action, &topics.payload, false)
        .await
    {
        error!(error = %e, topic = %topics.action, payload = %topics.payload, "publish failed");
    } else {
        debug!(topic = %topics.action, payload = %topics.payload, "trigger published");
    }
}

#[allow(clippy::too_many_arguments)]
async fn publish_discovery<P: Publisher>(
    handle: &P,
    topic_prefix: &str,
    hass: &HassConfig,
    cfg: &DeviceConfig,
    identity: &DeviceIdentity,
    key_slug: &str,
    action: Action,
    topics: &TriggerTopics,
) -> anyhow::Result<()> {
    let device_friendly_name = format!("{} - {}", hass.name, cfg.name);
    // The HA device's stable identifier is derived from
    // (topic_prefix, cfg.resolved_mqtt_path()) — not from /dev/input
    // path or evdev name — so a reconnect under a different
    // /dev/input/eventN still maps to the same HA device, and two
    // evmqtt-rs instances on the same broker (distinct topic_prefix)
    // produce distinct HA devices for the same device_slug.
    let identifier = device_identifier(topic_prefix, &cfg.resolved_mqtt_path());
    let model = format!("Input Device ({})", identity.name);
    let payload = TriggerDiscovery {
        automation_type: "trigger",
        type_: action.ha_type(),
        subtype: key_slug,
        topic: &topics.action,
        payload: &topics.payload,
        qos: None,
        device: DeviceInfo {
            identifiers: vec![identifier],
            name: device_friendly_name,
            manufacturer: "evmqtt-rs",
            model: &model,
            sw_version: env!("CARGO_PKG_VERSION"),
        },
        origin: OriginInfo {
            name: "evmqtt-rs",
            sw_version: env!("CARGO_PKG_VERSION"),
            support_url: "https://github.com/bdolgov/evmqtt-rs",
        },
    };
    let json = serde_json::to_vec(&payload).expect("serialise discovery");
    handle.publish_bytes(&topics.config, json, true).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DeviceMatcher;
    use std::path::PathBuf;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakePublisher {
        publishes: Mutex<Vec<(String, Vec<u8>, bool)>>,
    }

    impl FakePublisher {
        fn action_topics(&self) -> Vec<String> {
            self.publishes
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, _, retain)| !*retain)
                .map(|(t, _, _)| t.clone())
                .collect()
        }
    }

    impl Publisher for FakePublisher {
        async fn publish_str(
            &self,
            topic: &str,
            payload: &str,
            retain: bool,
        ) -> anyhow::Result<()> {
            self.publishes.lock().unwrap().push((
                topic.to_string(),
                payload.as_bytes().to_vec(),
                retain,
            ));
            Ok(())
        }

        async fn publish_bytes(
            &self,
            topic: &str,
            payload: Vec<u8>,
            retain: bool,
        ) -> anyhow::Result<()> {
            self.publishes
                .lock()
                .unwrap()
                .push((topic.to_string(), payload, retain));
            Ok(())
        }
    }

    fn identity_with_path_and_uniq(path: &str, uniq: &str) -> DeviceIdentity {
        DeviceIdentity {
            path: PathBuf::from(path),
            name: "USB Keyboard".to_string(),
            unique_id: Some(uniq.to_string()),
            bus: 0x0003,
            vendor: 0x046d,
            product: 0xc52b,
            version: 0x0111,
            physical_path: None,
            has_keys: true,
        }
    }

    fn hass() -> HassConfig {
        HassConfig::default()
    }

    /// The end-to-end shape we care about: a device configured by
    /// `unique_id` keeps the *same* MQTT action topic across a disconnect
    /// and a reconnect under a different `/dev/input/eventN` path.
    #[tokio::test]
    async fn same_unique_id_keeps_topic_across_path_change() {
        let cfg = DeviceConfig {
            matcher: DeviceMatcher::UniqueId("abc".into()),
            name: "Living Room".into(),
            mqtt_path: None,
        };
        let publisher = FakePublisher::default();
        let mut announced = HashSet::new();

        // First connection at event3.
        let id1 = identity_with_path_and_uniq("/dev/input/event3", "abc");
        handle_key_event(
            &publisher,
            "evmqtt",
            &hass(),
            &cfg,
            &id1,
            &mut announced,
            KeyCode::KEY_A,
            1, // press
        )
        .await;

        // Disconnect, then reconnect at a different path with the same uniq.
        let id2 = identity_with_path_and_uniq("/dev/input/event7", "abc");
        handle_key_event(
            &publisher,
            "evmqtt",
            &hass(),
            &cfg,
            &id2,
            &mut announced,
            KeyCode::KEY_A,
            0, // release
        )
        .await;

        let topics = publisher.action_topics();
        assert_eq!(topics.len(), 2, "expected one action per call");
        // Topic derives from cfg.resolved_mqtt_path() = slugify("Living Room"),
        // not from /dev/input/eventN, so both publishes target the same topic.
        assert_eq!(topics[0], "evmqtt/living-room/action");
        assert_eq!(topics[1], "evmqtt/living-room/action");
    }

    #[tokio::test]
    async fn explicit_mqtt_path_overrides_slugified_name() {
        let cfg = DeviceConfig {
            matcher: DeviceMatcher::Name("USB Keyboard".into()),
            name: "Office Keyboard".into(),
            mqtt_path: Some("office-kbd".into()),
        };
        let publisher = FakePublisher::default();
        let mut announced = HashSet::new();
        let id = identity_with_path_and_uniq("/dev/input/event4", "x");
        handle_key_event(
            &publisher,
            "evmqtt",
            &hass(),
            &cfg,
            &id,
            &mut announced,
            KeyCode::KEY_A,
            1,
        )
        .await;
        let topics = publisher.action_topics();
        assert_eq!(topics, vec!["evmqtt/office-kbd/action"]);
    }

    /// `hass.enabled = false` ⇒ no retained discovery publish on first
    /// keypress, but the action publish still happens.
    #[tokio::test]
    async fn hass_disabled_skips_discovery_but_still_publishes_actions() {
        let cfg = DeviceConfig {
            matcher: DeviceMatcher::UniqueId("abc".into()),
            name: "Living Room".into(),
            mqtt_path: None,
        };
        let publisher = FakePublisher::default();
        let mut announced = HashSet::new();
        let id = identity_with_path_and_uniq("/dev/input/event3", "abc");
        let hass = HassConfig {
            enabled: false,
            ..HassConfig::default()
        };
        handle_key_event(
            &publisher,
            "evmqtt",
            &hass,
            &cfg,
            &id,
            &mut announced,
            KeyCode::KEY_A,
            1,
        )
        .await;

        let publishes = publisher.publishes.lock().unwrap();
        let retained: Vec<_> = publishes.iter().filter(|(_, _, r)| *r).collect();
        let non_retained: Vec<_> = publishes.iter().filter(|(_, _, r)| !*r).collect();
        assert!(retained.is_empty(), "discovery should be suppressed");
        assert_eq!(non_retained.len(), 1, "action should still publish");
        assert_eq!(non_retained[0].0, "evmqtt/living-room/action");
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
