//! Library API for inspecting and managing a running daemon over MQTT.
//!
//! The CLI subcommands (`--list-devices`, `--enable-device`, …) are
//! thin wrappers around this. Third-party Rust code can embed the
//! crate and call into [`Client`] directly.

use crate::config::MqttConfig;
use crate::mqtt::{MqttRuntime, graceful_shutdown, spawn as spawn_mqtt};
use crate::topics::{
    PAYLOAD_DISABLED, PAYLOAD_ENABLED, availability_topic, device_enabled_topic,
    device_enabled_wildcard,
};
use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use std::process;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceSnapshot {
    pub slug: String,
    pub name: String,
    pub unique_id: Option<String>,
    pub bus: u16,
    pub vendor: u16,
    pub product: u16,
    pub version: u16,
    /// `None` if no retained `enabled` topic was seen (device exists
    /// only as an info entry, or was cleared).
    pub enabled: Option<bool>,
}

pub struct Client {
    runtime: MqttRuntime,
    topic_prefix: String,
}

impl Client {
    /// Connect to the broker. Does not subscribe to anything yet.
    ///
    /// Waits for the broker's CONNACK (up to 5 s) before returning;
    /// fails fast if the broker rejects credentials, the TLS
    /// handshake fails, or the host can't be reached within the
    /// budget.
    pub async fn connect(mqtt: &MqttConfig) -> Result<Self> {
        let client_id = format!("{}-cli-{}", mqtt.client_id_prefix, process::id());
        // The CLI does not own the availability topic -- supply a
        // throwaway value as the LWT topic; the daemon's LWT remains
        // authoritative.
        let availability = availability_topic(&mqtt.topic_prefix);
        let runtime = spawn_mqtt(mqtt, client_id, format!("{availability}/cli-lwt"));
        runtime
            .wait_ready(Duration::from_secs(5))
            .await
            .context("connecting to MQTT broker")?;
        Ok(Self {
            runtime,
            topic_prefix: mqtt.topic_prefix.clone(),
        })
    }

    /// Subscribe to the device-info and enabled-mirror topics, collect
    /// retained messages, and return a per-slug snapshot.
    ///
    /// The wait policy is: read every message; whenever a message
    /// arrives, reset the idle timer; return when the timer expires
    /// (default 500 ms of silence). Overall hard cap of 5 s.
    pub async fn list_devices(&mut self) -> Result<Vec<DeviceSnapshot>> {
        let info_wildcard = format!("{}/_devices/+", self.topic_prefix);
        let enabled_wildcard = device_enabled_wildcard(&self.topic_prefix);
        self.runtime
            .handle
            .subscribe(&info_wildcard)
            .await
            .context("subscribe info wildcard")?;
        self.runtime
            .handle
            .subscribe(&enabled_wildcard)
            .await
            .context("subscribe enabled wildcard")?;

        let mut infos: BTreeMap<String, Value> = BTreeMap::new();
        let mut enabled: BTreeMap<String, bool> = BTreeMap::new();

        let idle = Duration::from_millis(500);
        let hard_cap = Duration::from_secs(5);
        let start = std::time::Instant::now();
        let incoming = &mut self.runtime.incoming;
        loop {
            if start.elapsed() >= hard_cap {
                break;
            }
            match timeout(idle, incoming.recv()).await {
                Err(_) => break,   // idle reached
                Ok(None) => break, // channel closed
                Ok(Some(msg)) => {
                    if let Some(slug) = parse_info_slug(&self.topic_prefix, &msg.topic) {
                        if msg.payload.is_empty() {
                            infos.remove(slug);
                        } else if let Ok(v) = serde_json::from_slice::<Value>(&msg.payload) {
                            infos.insert(slug.to_string(), v);
                        }
                    } else if let Some(slug) =
                        crate::topics::parse_enabled_topic(&self.topic_prefix, &msg.topic)
                    {
                        match std::str::from_utf8(&msg.payload).unwrap_or("").trim() {
                            PAYLOAD_ENABLED => {
                                enabled.insert(slug.to_string(), true);
                            }
                            PAYLOAD_DISABLED => {
                                enabled.insert(slug.to_string(), false);
                            }
                            _ => {
                                // Empty (= remove command pending) — drop.
                                enabled.remove(slug);
                            }
                        }
                    }
                }
            }
        }

        let mut out = Vec::with_capacity(infos.len());
        for (slug, info) in infos {
            out.push(DeviceSnapshot {
                slug: slug.clone(),
                name: info
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                unique_id: info
                    .get("unique_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
                bus: info.get("bus").and_then(|v| v.as_u64()).unwrap_or(0) as u16,
                vendor: info.get("vendor").and_then(|v| v.as_u64()).unwrap_or(0) as u16,
                product: info.get("product").and_then(|v| v.as_u64()).unwrap_or(0) as u16,
                version: info.get("version").and_then(|v| v.as_u64()).unwrap_or(0) as u16,
                enabled: enabled.get(&slug).copied(),
            });
        }
        Ok(out)
    }

    pub async fn enable_device(&self, slug: &str) -> Result<()> {
        let topic = device_enabled_topic(&self.topic_prefix, slug);
        self.runtime
            .handle
            .publish_str(&topic, PAYLOAD_ENABLED, true)
            .await
            .with_context(|| format!("publish {topic}"))
    }

    pub async fn disable_device(&self, slug: &str) -> Result<()> {
        let topic = device_enabled_topic(&self.topic_prefix, slug);
        self.runtime
            .handle
            .publish_str(&topic, PAYLOAD_DISABLED, true)
            .await
            .with_context(|| format!("publish {topic}"))
    }

    /// Publishes empty retained to `_devices/<slug>/enabled`. The
    /// running daemon interprets this as a remove command and clears
    /// the info topic and HA discovery; if the daemon isn't running,
    /// the next start will see the cleared `enabled` topic and remove
    /// the entry then.
    pub async fn remove_device(&self, slug: &str) -> Result<()> {
        let topic = device_enabled_topic(&self.topic_prefix, slug);
        self.runtime
            .handle
            .publish_empty_retained(&topic)
            .await
            .with_context(|| format!("publish empty retained {topic}"))
    }

    /// Politely disconnect and stop the eventloop task.
    pub async fn shutdown(self) -> Result<()> {
        graceful_shutdown(
            self.runtime.handle,
            self.runtime.event_task,
            self.runtime.shutdown_flag,
        )
        .await;
        Ok(())
    }
}

fn parse_info_slug<'a>(topic_prefix: &str, topic: &'a str) -> Option<&'a str> {
    let prefix = format!("{topic_prefix}/_devices/");
    let rest = topic.strip_prefix(&prefix)?;
    if rest.is_empty() || rest.contains('/') {
        return None;
    }
    Some(rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topics::device_info_topic;

    #[test]
    fn parse_info_slug_round_trips_with_builder() {
        let topic = device_info_topic("evmqtt", "kbd");
        assert_eq!(parse_info_slug("evmqtt", &topic), Some("kbd"));
    }

    #[test]
    fn parse_info_slug_rejects_enabled_subtopic() {
        let topic = device_enabled_topic("evmqtt", "kbd");
        // _devices/kbd/enabled is NOT the info topic itself.
        assert_eq!(parse_info_slug("evmqtt", &topic), None);
    }

    #[test]
    fn parse_info_slug_rejects_wrong_prefix() {
        assert_eq!(parse_info_slug("evmqtt", "homeassistant/foo"), None);
    }
}
