use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Action {
    Press,
    Release,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Press => "press",
            Action::Release => "release",
        }
    }

    /// HA "type" string used in the device_automation discovery payload.
    pub fn ha_type(self) -> &'static str {
        match self {
            Action::Press => "button_short_press",
            Action::Release => "button_short_release",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerTopics {
    /// `<discovery_prefix>/device_automation/<unique_id>/config` (retained)
    pub config: String,
    /// Shared per-device topic on which action payloads are published.
    pub action: String,
    /// Stable HA unique id / object id for this (device, key, action) triple.
    pub unique_id: String,
    /// Payload that HA matches against to fire this trigger.
    pub payload: String,
}

/// Per-device shared action topic.
pub fn action_topic(topic_prefix: &str, device_slug: &str) -> String {
    format!("{topic_prefix}/{device_slug}/action")
}

pub fn trigger_topics(
    discovery_prefix: &str,
    topic_prefix: &str,
    device_slug: &str,
    key_slug: &str,
    action: Action,
) -> TriggerTopics {
    // unique_id is namespaced by topic_prefix so two evmqtt-rs instances
    // sharing a broker (different topic_prefix on each) don't trample
    // each other's HA discovery for the same device_slug.
    let unique_id = format!(
        "{topic_prefix}_{device_slug}_{key_slug}_{}",
        action.as_str()
    );
    TriggerTopics {
        config: format!("{discovery_prefix}/device_automation/{unique_id}/config"),
        action: action_topic(topic_prefix, device_slug),
        payload: format!("{key_slug}_{}", action.as_str()),
        unique_id,
    }
}

/// Stable HA device-level identifier — the value used in
/// `device.identifiers[]`. Namespaced by `topic_prefix` for the same
/// reason as `unique_id`.
pub fn device_identifier(topic_prefix: &str, device_slug: &str) -> String {
    format!("{topic_prefix}_{device_slug}")
}

/// MQTT availability topic for the whole gateway (LWT).
pub fn availability_topic(topic_prefix: &str) -> String {
    format!("{topic_prefix}/status")
}

#[derive(Debug, Serialize)]
pub struct DeviceInfo<'a> {
    pub identifiers: Vec<String>,
    pub name: String,
    pub manufacturer: &'a str,
    pub model: &'a str,
    pub sw_version: &'a str,
}

#[derive(Debug, Serialize)]
pub struct OriginInfo<'a> {
    pub name: &'a str,
    pub sw_version: &'a str,
    pub support_url: &'a str,
}

/// HA MQTT device-trigger discovery payload.
///
/// Per <https://www.home-assistant.io/integrations/device_trigger.mqtt/>.
#[derive(Debug, Serialize)]
pub struct TriggerDiscovery<'a> {
    pub automation_type: &'static str, // always "trigger"
    #[serde(rename = "type")]
    pub type_: &'a str,
    pub subtype: &'a str,
    pub topic: &'a str,
    pub payload: &'a str,
    pub device: DeviceInfo<'a>,
    pub origin: OriginInfo<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qos: Option<u8>,
}

pub const PAYLOAD_AVAILABLE: &str = "online";
pub const PAYLOAD_NOT_AVAILABLE: &str = "offline";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_press_topics() {
        let t = trigger_topics(
            "homeassistant",
            "evmqtt",
            "usb-keyboard",
            "volumeup",
            Action::Press,
        );
        assert_eq!(t.unique_id, "evmqtt_usb-keyboard_volumeup_press");
        assert_eq!(
            t.config,
            "homeassistant/device_automation/evmqtt_usb-keyboard_volumeup_press/config",
        );
        assert_eq!(t.action, "evmqtt/usb-keyboard/action");
        assert_eq!(t.payload, "volumeup_press");
    }

    #[test]
    fn unique_id_namespaced_by_topic_prefix() {
        // Two evmqtt-rs instances configured with distinct topic_prefixes
        // produce distinct HA unique_ids even for the same device_slug,
        // so they don't fight over the same discovery config topic.
        let one = trigger_topics("homeassistant", "host-a", "remote", "a", Action::Press);
        let two = trigger_topics("homeassistant", "host-b", "remote", "a", Action::Press);
        assert_eq!(one.unique_id, "host-a_remote_a_press");
        assert_eq!(two.unique_id, "host-b_remote_a_press");
        assert_ne!(one.unique_id, two.unique_id);
        assert_ne!(one.config, two.config);
    }

    #[test]
    fn device_identifier_namespaced_by_topic_prefix() {
        assert_eq!(device_identifier("evmqtt", "remote"), "evmqtt_remote");
        assert_eq!(device_identifier("host-a", "remote"), "host-a_remote");
    }

    #[test]
    fn builds_release_topics() {
        let t = trigger_topics(
            "homeassistant",
            "evmqtt",
            "usb-keyboard",
            "volumeup",
            Action::Release,
        );
        assert_eq!(t.unique_id, "evmqtt_usb-keyboard_volumeup_release");
        assert_eq!(t.payload, "volumeup_release");
        assert_eq!(t.action, "evmqtt/usb-keyboard/action");
    }

    #[test]
    fn press_release_share_action_topic() {
        let p = trigger_topics("homeassistant", "evmqtt", "kbd", "a", Action::Press);
        let r = trigger_topics("homeassistant", "evmqtt", "kbd", "a", Action::Release);
        assert_eq!(p.action, r.action);
        assert_ne!(p.unique_id, r.unique_id);
        assert_ne!(p.payload, r.payload);
    }

    #[test]
    fn action_string_mapping() {
        assert_eq!(Action::Press.as_str(), "press");
        assert_eq!(Action::Release.as_str(), "release");
        assert_eq!(Action::Press.ha_type(), "button_short_press");
        assert_eq!(Action::Release.ha_type(), "button_short_release");
    }

    #[test]
    fn availability_topic_format() {
        assert_eq!(availability_topic("evmqtt"), "evmqtt/status");
    }

    #[test]
    fn discovery_payload_serializes() {
        let t = trigger_topics(
            "homeassistant",
            "evmqtt",
            "gpio-ir-recv",
            "volumeup",
            Action::Press,
        );
        let d = TriggerDiscovery {
            automation_type: "trigger",
            type_: Action::Press.ha_type(),
            subtype: "volumeup",
            topic: &t.action,
            payload: &t.payload,
            qos: None,
            device: DeviceInfo {
                identifiers: vec!["evmqtt_gpio-ir-recv".into()],
                name: "evmqtt - gpio_ir_recv".into(),
                manufacturer: "evmqtt-rs",
                model: "Input Device",
                sw_version: env!("CARGO_PKG_VERSION"),
            },
            origin: OriginInfo {
                name: "evmqtt-rs",
                sw_version: env!("CARGO_PKG_VERSION"),
                support_url: "https://github.com/odtgit/evmqtt",
            },
        };
        let json = serde_json::to_value(&d).unwrap();
        assert_eq!(json["automation_type"], "trigger");
        assert_eq!(json["type"], "button_short_press");
        assert_eq!(json["subtype"], "volumeup");
        assert_eq!(json["topic"], t.action.as_str());
        assert_eq!(json["payload"], "volumeup_press");
        assert_eq!(json["device"]["identifiers"][0], "evmqtt_gpio-ir-recv");
        // qos is None so it should be omitted from serialized output
        assert!(json.get("qos").is_none());
    }
}
