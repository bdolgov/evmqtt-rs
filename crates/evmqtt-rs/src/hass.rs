use crate::config::{HassConfig, MqttConfig};
use crate::db::DeviceRecord;
use crate::slug::key_slug;
use crate::topics::{
    Action, PAYLOAD_DISABLED, PAYLOAD_ENABLED, action_topic, availability_topic,
    device_enabled_topic, device_identifier,
};
use evdev::KeyCode;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const ORIGIN_NAME: &str = "evmqtt-rs";
const ORIGIN_URL: &str = "https://github.com/bdolgov/evmqtt-rs";
const MANUFACTURER: &str = "evmqtt-rs";

/// Resolve a u16 evdev code to its symbolic name (`KEY_VOLUMEUP`),
/// falling back to a decimal form for unknown codes.
pub fn key_name(code: u16) -> String {
    let formatted = format!("{:?}", KeyCode::new(code));
    // KeyCode::Debug yields either "KEY_FOO" or, for unknowns, something
    // like "KeyCode(123)" — keep the former, normalise the latter.
    if formatted.starts_with("KEY_") || formatted.starts_with("BTN_") {
        formatted
    } else {
        format!("KEY_{code}")
    }
}

#[derive(Debug, Serialize)]
struct OriginBlock<'a> {
    name: &'a str,
    sw: &'a str,
    url: &'a str,
}

#[derive(Debug, Serialize)]
struct DeviceBlock<'a> {
    ids: Vec<String>,
    name: String,
    mf: &'a str,
    mdl: String,
    sw: &'a str,
}

#[derive(Debug, Serialize)]
struct DiscoveryPayload<'a> {
    dev: DeviceBlock<'a>,
    o: OriginBlock<'a>,
    availability_topic: String,
    qos: u8,
    cmps: BTreeMap<String, Value>,
}

/// Build the retained device-info JSON that lives at
/// `{prefix}/_devices/{slug}`. Carries identifying fields only; the
/// list of observed keys does not appear here. Observed keys surface
/// as HA discovery triggers, and republishing the info topic on
/// every new key would just duplicate that information.
pub fn device_info_payload(record: &DeviceRecord) -> Vec<u8> {
    let mut v = json!({
        "slug": record.slug,
        "name": record.display_name(),
        "bus": record.bus,
        "vendor": record.vendor,
        "product": record.product,
        "version": record.version,
    });
    if let Some(uniq) = record.unique_id.as_deref() {
        v["unique_id"] = Value::String(uniq.to_string());
    }
    serde_json::to_vec(&v).expect("serialise info payload")
}

/// Build the retained HA per-device discovery payload that lives at
/// `{hass.discovery_prefix}/device/{identifier}/config`.
///
/// Always includes the "Enabled" switch. Adds a (press, release)
/// trigger pair for every key in `record.observed_keys`.
pub fn discovery_payload(record: &DeviceRecord, mqtt: &MqttConfig, hass: &HassConfig) -> Vec<u8> {
    let identifier = device_identifier(&mqtt.topic_prefix, &record.slug);
    let enabled_topic = device_enabled_topic(&mqtt.topic_prefix, &record.slug);
    let action_topic_str = action_topic(&mqtt.topic_prefix, &record.slug);

    let mut cmps: BTreeMap<String, Value> = BTreeMap::new();
    cmps.insert(
        "enabled".to_string(),
        json!({
            "platform": "switch",
            "name": "Enabled",
            "unique_id": format!("{identifier}_enabled"),
            "command_topic": enabled_topic,
            "state_topic":   enabled_topic,
            "payload_on":  PAYLOAD_ENABLED,
            "payload_off": PAYLOAD_DISABLED,
            "state_on":    PAYLOAD_ENABLED,
            "state_off":   PAYLOAD_DISABLED,
            "retain": true,
            "icon": "mdi:keyboard",
        }),
    );

    for code in record.observed_keys.iter().copied() {
        let kname = key_name(code);
        let kslug = key_slug(&kname);
        for action in [Action::Press, Action::Release] {
            let cmp_key = format!("{kslug}_{}", action.as_str());
            cmps.insert(
                cmp_key,
                json!({
                    "platform": "device_automation",
                    "automation_type": "trigger",
                    "type": action.ha_type(),
                    "subtype": kslug,
                    "topic": action_topic_str,
                    "payload": format!("{kslug}_{}", action.as_str()),
                }),
            );
        }
    }

    let display_name = record.display_name();
    let payload = DiscoveryPayload {
        dev: DeviceBlock {
            ids: vec![identifier],
            name: format!("{} - {}", hass.name, display_name),
            mf: MANUFACTURER,
            mdl: format!("Input Device ({display_name})"),
            sw: env!("CARGO_PKG_VERSION"),
        },
        o: OriginBlock {
            name: ORIGIN_NAME,
            sw: env!("CARGO_PKG_VERSION"),
            url: ORIGIN_URL,
        },
        availability_topic: availability_topic(&mqtt.topic_prefix),
        qos: 1,
        cmps,
    };
    serde_json::to_vec(&payload).expect("serialise discovery payload")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mqtt() -> MqttConfig {
        MqttConfig {
            host: "x".into(),
            port: 1883,
            username: None,
            password: None,
            topic_prefix: "evmqtt".into(),
            client_id_prefix: "evmqtt-rs".into(),
            keepalive_secs: 30,
        }
    }

    fn hass() -> HassConfig {
        HassConfig {
            enabled: true,
            discovery_prefix: "homeassistant".into(),
            name: "evmqtt".into(),
        }
    }

    fn record_with_keys(keys: Vec<u16>) -> DeviceRecord {
        DeviceRecord {
            slug: "usb-keyboard".into(),
            name: "USB Keyboard".into(),
            unique_id: Some("abc-123".into()),
            bus: 3,
            vendor: 0x046d,
            product: 0xc52b,
            version: 0x0111,
            physical_path: None,
            capability_fingerprint: None,
            capability_tag: None,
            enabled: false,
            observed_keys: keys,
        }
    }

    #[test]
    fn key_name_known_codes() {
        assert_eq!(key_name(KeyCode::KEY_A.code()), "KEY_A");
        assert_eq!(key_name(KeyCode::KEY_VOLUMEUP.code()), "KEY_VOLUMEUP");
    }

    fn parse(bytes: Vec<u8>) -> Value {
        serde_json::from_slice(&bytes).expect("valid JSON")
    }

    #[test]
    fn discovery_always_includes_enabled_switch() {
        let rec = record_with_keys(vec![]);
        let payload = parse(discovery_payload(&rec, &mqtt(), &hass()));
        let cmps = payload["cmps"].as_object().unwrap();
        let sw = &cmps["enabled"];
        assert_eq!(sw["platform"], "switch");
        assert_eq!(sw["command_topic"], "evmqtt/_devices/usb-keyboard/enabled");
        assert_eq!(sw["state_topic"], "evmqtt/_devices/usb-keyboard/enabled");
        assert_eq!(sw["payload_on"], "on");
        assert_eq!(sw["unique_id"], "evmqtt_usb-keyboard_enabled");
        // Without observed keys, the switch is the only component.
        assert_eq!(cmps.len(), 1);
    }

    #[test]
    fn discovery_adds_trigger_pair_per_observed_key() {
        let rec = record_with_keys(vec![KeyCode::KEY_VOLUMEUP.code()]);
        let payload = parse(discovery_payload(&rec, &mqtt(), &hass()));
        let cmps = payload["cmps"].as_object().unwrap();
        assert_eq!(cmps.len(), 1 /* switch */ + 2 /* press,release */);
        let press = &cmps["volumeup_press"];
        assert_eq!(press["platform"], "device_automation");
        assert_eq!(press["type"], "button_short_press");
        assert_eq!(press["subtype"], "volumeup");
        assert_eq!(press["topic"], "evmqtt/usb-keyboard/action");
        assert_eq!(press["payload"], "volumeup_press");
        let release = &cmps["volumeup_release"];
        assert_eq!(release["type"], "button_short_release");
        assert_eq!(release["payload"], "volumeup_release");
    }

    #[test]
    fn discovery_carries_device_identifier_and_origin() {
        let rec = record_with_keys(vec![]);
        let payload = parse(discovery_payload(&rec, &mqtt(), &hass()));
        assert_eq!(payload["dev"]["ids"][0], "evmqtt_usb-keyboard");
        assert_eq!(payload["dev"]["name"], "evmqtt - USB Keyboard");
        assert_eq!(payload["o"]["name"], "evmqtt-rs");
        assert_eq!(payload["availability_topic"], "evmqtt/status");
    }

    #[test]
    fn info_payload_carries_identifiers_only() {
        let rec = record_with_keys(vec![KeyCode::KEY_A.code(), KeyCode::KEY_VOLUMEUP.code()]);
        let info = parse(device_info_payload(&rec));
        assert_eq!(info["slug"], "usb-keyboard");
        assert_eq!(info["name"], "USB Keyboard");
        assert_eq!(info["unique_id"], "abc-123");
        assert_eq!(info["bus"], 3);
        assert_eq!(info["vendor"], 0x046d);
        assert_eq!(info["product"], 0xc52b);
        assert_eq!(info["version"], 0x0111);
        // Observed keys do NOT appear in the info topic -- they
        // surface as HA discovery triggers instead.
        assert!(info.get("observed_keys").is_none());
    }

    #[test]
    fn info_payload_omits_unique_id_when_absent() {
        let mut rec = record_with_keys(vec![]);
        rec.unique_id = None;
        let info = parse(device_info_payload(&rec));
        assert!(info.get("unique_id").is_none());
    }
}
