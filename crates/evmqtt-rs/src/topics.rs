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

/// Per-device shared action topic.
pub fn action_topic(topic_prefix: &str, slug: &str) -> String {
    format!("{topic_prefix}/{slug}/action")
}

/// MQTT availability topic for the whole gateway (LWT).
pub fn availability_topic(topic_prefix: &str) -> String {
    format!("{topic_prefix}/status")
}

/// Retained JSON describing one known device.
pub fn device_info_topic(topic_prefix: &str, slug: &str) -> String {
    format!("{topic_prefix}/_devices/{slug}")
}

/// Retained `on`/`off`. Reading reports current state; writing is a
/// command to the daemon.
pub fn device_enabled_topic(topic_prefix: &str, slug: &str) -> String {
    format!("{topic_prefix}/_devices/{slug}/enabled")
}

/// MQTT wildcard the daemon subscribes to in order to receive enable
/// and disable commands.
pub fn device_enabled_wildcard(topic_prefix: &str) -> String {
    format!("{topic_prefix}/_devices/+/enabled")
}

/// Try to split `{prefix}/_devices/{slug}/enabled` back into its slug.
/// Returns `None` for topics that don't match the shape.
pub fn parse_enabled_topic<'a>(topic_prefix: &str, topic: &'a str) -> Option<&'a str> {
    let prefix = format!("{topic_prefix}/_devices/");
    let rest = topic.strip_prefix(&prefix)?;
    let slug = rest.strip_suffix("/enabled")?;
    if slug.is_empty() || slug.contains('/') {
        return None;
    }
    Some(slug)
}

/// HA device-level identifier — the value used in `device.identifiers`.
/// Namespaced by `topic_prefix` so two evmqtt-rs instances on the same
/// broker don't collide.
pub fn device_identifier(topic_prefix: &str, slug: &str) -> String {
    format!("{topic_prefix}_{slug}")
}

/// HA per-device discovery topic for the device-based bundle format.
pub fn device_discovery_topic(discovery_prefix: &str, identifier: &str) -> String {
    format!("{discovery_prefix}/device/{identifier}/config")
}

pub const PAYLOAD_AVAILABLE: &str = "online";
pub const PAYLOAD_NOT_AVAILABLE: &str = "offline";
pub const PAYLOAD_ENABLED: &str = "on";
pub const PAYLOAD_DISABLED: &str = "off";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_topic_shape() {
        assert_eq!(action_topic("evmqtt", "kbd"), "evmqtt/kbd/action");
    }

    #[test]
    fn availability_topic_shape() {
        assert_eq!(availability_topic("evmqtt"), "evmqtt/status");
    }

    #[test]
    fn device_info_topic_shape() {
        assert_eq!(device_info_topic("evmqtt", "kbd"), "evmqtt/_devices/kbd");
    }

    #[test]
    fn device_enabled_topic_shape() {
        assert_eq!(
            device_enabled_topic("evmqtt", "kbd"),
            "evmqtt/_devices/kbd/enabled"
        );
    }

    #[test]
    fn enabled_wildcard_shape() {
        assert_eq!(
            device_enabled_wildcard("evmqtt"),
            "evmqtt/_devices/+/enabled"
        );
    }

    #[test]
    fn parse_enabled_topic_extracts_slug() {
        assert_eq!(
            parse_enabled_topic("evmqtt", "evmqtt/_devices/kbd/enabled"),
            Some("kbd")
        );
    }

    #[test]
    fn parse_enabled_topic_rejects_unrelated() {
        assert_eq!(
            parse_enabled_topic("evmqtt", "evmqtt/_devices/kbd"),
            None,
            "missing /enabled suffix",
        );
        assert_eq!(
            parse_enabled_topic("evmqtt", "homeassistant/foo"),
            None,
            "wrong prefix",
        );
        assert_eq!(
            parse_enabled_topic("evmqtt", "evmqtt/_devices//enabled"),
            None,
            "empty slug",
        );
        assert_eq!(
            parse_enabled_topic("evmqtt", "evmqtt/_devices/a/b/enabled"),
            None,
            "slug with slash",
        );
    }

    #[test]
    fn device_identifier_shape() {
        assert_eq!(device_identifier("evmqtt", "kbd"), "evmqtt_kbd");
        assert_eq!(device_identifier("host-a", "kbd"), "host-a_kbd");
    }

    #[test]
    fn device_discovery_topic_shape() {
        assert_eq!(
            device_discovery_topic("homeassistant", "evmqtt_kbd"),
            "homeassistant/device/evmqtt_kbd/config",
        );
    }
}
