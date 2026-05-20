use crate::slug::slugify;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub mqtt: MqttConfig,
    #[serde(default)]
    pub hass: HassConfig,
    #[serde(default, rename = "device")]
    pub devices: Vec<DeviceConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MqttConfig {
    pub host: String,
    #[serde(default = "default_mqtt_port")]
    pub port: u16,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Base of every published topic — `<topic_prefix>/<mqtt_path>/action`
    /// for events and `<topic_prefix>/status` for the LWT availability
    /// topic. Belongs to MQTT because it's the gateway's prefix on the
    /// broker side, independent of whether HA discovery is in use.
    #[serde(default = "default_topic_prefix")]
    pub topic_prefix: String,
    #[serde(default = "default_client_id_prefix")]
    pub client_id_prefix: String,
    #[serde(default = "default_keepalive_secs")]
    pub keepalive_secs: u16,
}

fn default_mqtt_port() -> u16 {
    1883
}
fn default_client_id_prefix() -> String {
    "evmqtt-rs".into()
}
fn default_keepalive_secs() -> u16 {
    30
}
fn default_topic_prefix() -> String {
    "evmqtt".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HassConfig {
    /// When `false`, evmqtt-rs publishes action events but skips the
    /// retained `homeassistant/device_automation/.../config` discovery
    /// payloads — useful when you want a generic key→MQTT gateway
    /// without HA wiring.
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_discovery_prefix")]
    pub discovery_prefix: String,
    #[serde(default = "default_gateway_name")]
    pub name: String,
}

impl Default for HassConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            discovery_prefix: default_discovery_prefix(),
            name: default_gateway_name(),
        }
    }
}

fn default_discovery_prefix() -> String {
    "homeassistant".into()
}
fn default_gateway_name() -> String {
    "evmqtt".into()
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceConfig {
    pub matcher: DeviceMatcher,
    pub name: String,
    #[serde(default)]
    pub mqtt_path: Option<String>,
}

impl DeviceConfig {
    pub fn resolved_mqtt_path(&self) -> String {
        self.mqtt_path
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| slugify(&self.name))
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum DeviceMatcher {
    UniqueId(String),
    BusVendorProductVersion(u16, u16, u16, u16),
    Name(String),
}

impl Config {
    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        let cfg: Config = toml::from_str(s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
            path: path.display().to_string(),
            source: e,
        })?;
        Self::parse(&text)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.mqtt.host.trim().is_empty() {
            return Err(ConfigError::Invalid("mqtt.host must not be empty".into()));
        }
        if self.mqtt.port == 0 {
            return Err(ConfigError::Invalid("mqtt.port must be non-zero".into()));
        }
        if self.mqtt.topic_prefix.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "mqtt.topic_prefix must not be empty".into(),
            ));
        }
        if self.hass.discovery_prefix.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "hass.discovery_prefix must not be empty".into(),
            ));
        }
        let mut seen_paths: HashSet<String> = HashSet::new();
        for (i, d) in self.devices.iter().enumerate() {
            if d.name.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "device[{i}].name must not be empty"
                )));
            }
            let p = d.resolved_mqtt_path();
            if p.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "device[{i}] (`{}`) resolves to an empty mqtt_path",
                    d.name
                )));
            }
            if !seen_paths.insert(p.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate mqtt_path `{p}` across devices; set [[device]].mqtt_path explicitly",
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let toml = r#"
[mqtt]
host = "192.168.1.10"
"#;
        let cfg = Config::parse(toml).unwrap();
        assert_eq!(cfg.mqtt.host, "192.168.1.10");
        assert_eq!(cfg.mqtt.port, 1883);
        assert_eq!(cfg.mqtt.topic_prefix, "evmqtt");
        assert_eq!(cfg.hass.discovery_prefix, "homeassistant");
        assert!(cfg.hass.enabled);
        assert!(cfg.devices.is_empty());
    }

    #[test]
    fn parses_hass_disabled() {
        let toml = r#"
[mqtt]
host = "x"

[hass]
enabled = false
"#;
        let cfg = Config::parse(toml).unwrap();
        assert!(!cfg.hass.enabled);
    }

    #[test]
    fn parses_topic_prefix_under_mqtt() {
        let toml = r#"
[mqtt]
host = "x"
topic_prefix = "kbd"
"#;
        let cfg = Config::parse(toml).unwrap();
        assert_eq!(cfg.mqtt.topic_prefix, "kbd");
    }

    #[test]
    fn parses_unique_id_matcher() {
        let toml = r#"
[mqtt]
host = "x"

[[device]]
matcher = { unique_id = "abc-123" }
name    = "Living Room"
"#;
        let cfg = Config::parse(toml).unwrap();
        assert_eq!(cfg.devices.len(), 1);
        assert_eq!(
            cfg.devices[0].matcher,
            DeviceMatcher::UniqueId("abc-123".into())
        );
        assert_eq!(cfg.devices[0].resolved_mqtt_path(), "living-room");
    }

    #[test]
    fn parses_bvp_matcher_with_hex_literals() {
        let toml = r#"
[mqtt]
host = "x"

[[device]]
matcher = { bus_vendor_product_version = [0x0003, 0x046d, 0xc52b, 0x0111] }
name    = "Backup Remote"
"#;
        let cfg = Config::parse(toml).unwrap();
        assert_eq!(
            cfg.devices[0].matcher,
            DeviceMatcher::BusVendorProductVersion(0x0003, 0x046d, 0xc52b, 0x0111)
        );
    }

    #[test]
    fn parses_name_matcher_and_explicit_mqtt_path() {
        let toml = r#"
[mqtt]
host = "x"

[[device]]
matcher   = { name = "USB Keyboard" }
name      = "Office Keyboard"
mqtt_path = "office-kbd"
"#;
        let cfg = Config::parse(toml).unwrap();
        assert_eq!(
            cfg.devices[0].matcher,
            DeviceMatcher::Name("USB Keyboard".into())
        );
        assert_eq!(cfg.devices[0].resolved_mqtt_path(), "office-kbd");
    }

    #[test]
    fn rejects_empty_host() {
        let toml = r#"
[mqtt]
host = ""
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn rejects_zero_port() {
        let toml = r#"
[mqtt]
host = "x"
port = 0
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn rejects_duplicate_mqtt_path() {
        let toml = r#"
[mqtt]
host = "x"

[[device]]
matcher = { unique_id = "a" }
name    = "Remote"

[[device]]
matcher = { unique_id = "b" }
name    = "Remote"
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn rejects_empty_name() {
        let toml = r#"
[mqtt]
host = "x"

[[device]]
matcher = { unique_id = "a" }
name    = ""
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Invalid(_))));
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let toml = r#"
[mqtt]
host = "x"

[unknown_section]
foo = "bar"
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Parse(_))));
    }

    #[test]
    fn rejects_unknown_mqtt_field() {
        let toml = r#"
[mqtt]
host = "x"
typo_field = "oops"
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Parse(_))));
    }

    #[test]
    fn rejects_unknown_hass_field() {
        let toml = r#"
[mqtt]
host = "x"

[hass]
# `topic_prefix` used to live here; it's now under [mqtt]. A stale
# config with it set in [hass] should fail loudly rather than silently
# losing the override.
topic_prefix = "old-place"
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Parse(_))));
    }

    #[test]
    fn rejects_unknown_device_field() {
        let toml = r#"
[mqtt]
host = "x"

[[device]]
matcher = { unique_id = "a" }
name    = "Remote"
typo    = "oops"
"#;
        assert!(matches!(Config::parse(toml), Err(ConfigError::Parse(_))));
    }

    #[test]
    fn accepts_empty_device_list() {
        // Empty [[device]] list is valid; app.rs uses it to trigger detect-and-exit.
        let toml = r#"
[mqtt]
host = "x"
"#;
        let cfg = Config::parse(toml).unwrap();
        assert!(cfg.devices.is_empty());
    }
}
