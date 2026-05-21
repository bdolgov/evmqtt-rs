use crate::config::{HassConfig, MqttConfig};
use crate::db::{Database, MatchOutcome};
use crate::discovery::DeviceIdentity;
use crate::hass::{device_info_payload, discovery_payload};
use crate::monitor::{self, MonitorHandle};
use crate::mqtt::{IncomingPublish, MqttHandle};
use crate::topics::{
    PAYLOAD_DISABLED, PAYLOAD_ENABLED, action_topic, device_discovery_topic, device_enabled_topic,
    device_identifier, device_info_topic, parse_enabled_topic,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Everything the coordinator's tasks send back to it.
#[derive(Debug)]
pub enum CoordinatorMsg {
    /// A live `/dev/input/event*` device was opened. Coordinator decides
    /// whether it's a known record or a new one, persists, and (if
    /// enabled) spawns the monitor.
    DeviceConnected(DeviceIdentity),
    /// The monitor task for `slug` exited because the device went
    /// away (ENODEV, stream end). Coordinator drops the connected /
    /// monitored slots so a re-plug can re-attach.
    DeviceDisconnected { slug: String },
    /// A key event arrived for `slug`. If new, add to DB and republish
    /// info+discovery.
    KeyObserved { slug: String, code: u16 },
    /// Externally-triggered (CLI or HA switch). Updates DB enabled
    /// state and starts/stops the monitor.
    EnableCommand { slug: String, on: bool },
    /// Drop the record entirely, clear retained MQTT, abort monitor.
    RemoveCommand { slug: String },
}

/// Sender half of the coordinator's command channel.
pub type CoordinatorTx = mpsc::UnboundedSender<CoordinatorMsg>;
pub type CoordinatorRx = mpsc::UnboundedReceiver<CoordinatorMsg>;

pub struct Coordinator {
    db: Database,
    db_path: PathBuf,
    mqtt: MqttHandle,
    mqtt_cfg: MqttConfig,
    hass_cfg: HassConfig,
    tx: CoordinatorTx,
    /// Slugs whose physical device is currently open (used to spawn a
    /// monitor on enable without waiting for a re-plug).
    connected: HashMap<String, DeviceIdentity>,
    /// Slugs with a live monitor task.
    monitored: HashMap<String, MonitorHandle>,
}

impl Coordinator {
    pub fn new(
        db: Database,
        db_path: PathBuf,
        mqtt: MqttHandle,
        mqtt_cfg: MqttConfig,
        hass_cfg: HassConfig,
        tx: CoordinatorTx,
    ) -> Self {
        Self {
            db,
            db_path,
            mqtt,
            mqtt_cfg,
            hass_cfg,
            tx,
            connected: HashMap::new(),
            monitored: HashMap::new(),
        }
    }

    /// Publish current retained topics for every device already in the
    /// DB, including the disabled ones. Without this every restart of
    /// the daemon (or every fresh broker / HA install) would leave
    /// known-but-not-currently-connected devices invisible to HA, and
    /// a disabled device that is connected would never get its
    /// "Enabled" switch into HA until the user manages to enable it
    /// some other way.
    pub async fn republish_known(&self) {
        for rec in &self.db.devices {
            self.publish_info(rec).await;
            self.publish_enabled_mirror(rec).await;
            self.publish_discovery(rec).await;
        }
    }

    /// Main loop. Returns when both channels close.
    pub async fn run(
        mut self,
        mut rx: CoordinatorRx,
        mut mqtt_incoming: mpsc::UnboundedReceiver<IncomingPublish>,
    ) {
        // Make sure every known device has fresh info / enabled /
        // discovery on the broker before we start processing watcher
        // events. Especially important so disabled devices show up in
        // HA with their "Enabled" switch -- otherwise the only way to
        // enable them would be via the CLI, which the HA-only user
        // does not have.
        self.republish_known().await;

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => self.handle(msg).await,
                Some(pub_msg) = mqtt_incoming.recv() => self.handle_mqtt(pub_msg).await,
                else => break,
            }
        }
        self.abort_all_monitors();
    }

    fn abort_all_monitors(&mut self) {
        for (slug, handle) in self.monitored.drain() {
            let _ = handle.shutdown.send(());
            handle.join.abort();
            debug!(%slug, "aborted monitor on shutdown");
        }
    }

    async fn handle_mqtt(&mut self, msg: IncomingPublish) {
        let Some(slug) = parse_enabled_topic(&self.mqtt_cfg.topic_prefix, &msg.topic) else {
            return;
        };
        let body = std::str::from_utf8(&msg.payload).unwrap_or("").trim();
        if body.is_empty() {
            // Empty retained on the enabled topic is the "remove this
            // device" signal — published by --remove-device or by HA
            // clearing the switch.
            self.handle(CoordinatorMsg::RemoveCommand {
                slug: slug.to_string(),
            })
            .await;
            return;
        }
        let on = match body {
            PAYLOAD_ENABLED => true,
            PAYLOAD_DISABLED => false,
            other => {
                warn!(
                    %slug,
                    payload = %other,
                    "unrecognised payload on enabled topic; expected on/off",
                );
                return;
            }
        };
        self.handle(CoordinatorMsg::EnableCommand {
            slug: slug.to_string(),
            on,
        })
        .await;
    }

    async fn handle(&mut self, msg: CoordinatorMsg) {
        match msg {
            CoordinatorMsg::DeviceConnected(id) => self.on_connected(id).await,
            CoordinatorMsg::DeviceDisconnected { slug } => self.on_disconnected(slug),
            CoordinatorMsg::KeyObserved { slug, code } => self.on_key(slug, code).await,
            CoordinatorMsg::EnableCommand { slug, on } => self.on_enable(slug, on).await,
            CoordinatorMsg::RemoveCommand { slug } => self.on_remove(slug).await,
        }
    }

    async fn on_connected(&mut self, id: DeviceIdentity) {
        // Find or create the record.
        let (slug, freshly_inserted, persist) = match self.db.match_or_insert(&id) {
            MatchOutcome::Matched { slug, backfilled } => (slug, false, backfilled),
            MatchOutcome::Inserted { slug } => {
                info!(
                    path = %id.path.display(),
                    name = %id.name,
                    slug = %slug,
                    "new device registered; disabled by default",
                );
                (slug, true, true)
            }
        };
        if persist && let Err(e) = self.db.save_atomic(&self.db_path) {
            warn!(error = %e, "failed to persist DB after device sighting");
        }
        if freshly_inserted {
            let rec = self.db.find(&slug).expect("just-inserted record").clone();
            self.publish_info(&rec).await;
            self.publish_enabled_mirror(&rec).await;
            self.publish_discovery(&rec).await;
        }

        if self.connected.contains_key(&slug) {
            debug!(%slug, "device already connected; ignoring duplicate sighting");
            return;
        }
        self.connected.insert(slug.clone(), id);

        let enabled = self.db.find(&slug).map(|r| r.enabled).unwrap_or(false);
        if enabled {
            self.spawn_monitor(&slug);
        } else {
            debug!(%slug, "device is connected but disabled; not monitoring");
        }
    }

    fn on_disconnected(&mut self, slug: String) {
        self.connected.remove(&slug);
        self.monitored.remove(&slug);
        info!(%slug, "device disconnected");
    }

    async fn on_key(&mut self, slug: String, code: u16) {
        let inserted = match self.db.find_mut(&slug) {
            Some(rec) => rec.record_observed_key(code),
            None => {
                warn!(%slug, code, "key observed for unknown slug");
                return;
            }
        };
        if !inserted {
            return;
        }
        if let Err(e) = self.db.save_atomic(&self.db_path) {
            warn!(error = %e, "failed to persist DB after new key");
        }
        let rec = self.db.find(&slug).expect("known slug").clone();
        info!(%slug, code, "new key observed; republishing discovery");
        self.publish_info(&rec).await;
        self.publish_discovery(&rec).await;
    }

    async fn on_enable(&mut self, slug: String, on: bool) {
        let rec = match self.db.find_mut(&slug) {
            Some(r) if r.enabled == on => {
                debug!(%slug, on, "enable command is a no-op; state already matches");
                return;
            }
            Some(r) => {
                r.enabled = on;
                r.clone()
            }
            None => {
                debug!(
                    %slug,
                    "enable command for unknown slug; ignoring (might be a stale retained message)",
                );
                return;
            }
        };
        if let Err(e) = self.db.save_atomic(&self.db_path) {
            warn!(error = %e, "failed to persist DB after enable change");
        }
        info!(%slug, enabled = on, "enabled-state change applied");
        self.publish_enabled_mirror(&rec).await;

        if on {
            if self.connected.contains_key(&slug) && !self.monitored.contains_key(&slug) {
                self.spawn_monitor(&slug);
            }
        } else if let Some(handle) = self.monitored.remove(&slug) {
            let _ = handle.shutdown.send(());
            handle.join.abort();
        }
    }

    async fn on_remove(&mut self, slug: String) {
        if !self.db.remove(&slug) {
            debug!(%slug, "remove command for unknown slug; ignoring");
            return;
        }
        if let Err(e) = self.db.save_atomic(&self.db_path) {
            warn!(error = %e, "failed to persist DB after remove");
        }
        if let Some(handle) = self.monitored.remove(&slug) {
            let _ = handle.shutdown.send(());
            handle.join.abort();
        }
        self.connected.remove(&slug);

        // Clear retained topics so HA forgets the device.
        let info = device_info_topic(&self.mqtt_cfg.topic_prefix, &slug);
        let enabled = device_enabled_topic(&self.mqtt_cfg.topic_prefix, &slug);
        let ident = device_identifier(&self.mqtt_cfg.topic_prefix, &slug);
        let disc = device_discovery_topic(&self.hass_cfg.discovery_prefix, &ident);
        for topic in [info, enabled, disc] {
            if let Err(e) = self.mqtt.publish_empty_retained(&topic).await {
                warn!(%topic, error = %e, "failed to clear retained topic");
            }
        }
        info!(%slug, "device removed");
    }

    fn spawn_monitor(&mut self, slug: &str) {
        let Some(id) = self.connected.get(slug).cloned() else {
            return;
        };
        let action_topic: Arc<str> =
            Arc::from(action_topic(&self.mqtt_cfg.topic_prefix, slug).as_str());
        let handle = monitor::start(
            self.mqtt.clone(),
            action_topic,
            slug.to_string(),
            id,
            self.tx.clone(),
        );
        info!(%slug, "monitor task started");
        self.monitored.insert(slug.to_string(), handle);
    }

    async fn publish_info(&self, rec: &crate::db::DeviceRecord) {
        let topic = device_info_topic(&self.mqtt_cfg.topic_prefix, &rec.slug);
        self.publish_retained(&topic, device_info_payload(rec), "device info")
            .await;
    }

    async fn publish_enabled_mirror(&self, rec: &crate::db::DeviceRecord) {
        let topic = device_enabled_topic(&self.mqtt_cfg.topic_prefix, &rec.slug);
        let payload = if rec.enabled {
            PAYLOAD_ENABLED
        } else {
            PAYLOAD_DISABLED
        };
        self.publish_retained(&topic, payload.as_bytes().to_vec(), "enabled mirror")
            .await;
    }

    async fn publish_discovery(&self, rec: &crate::db::DeviceRecord) {
        if !self.hass_cfg.enabled {
            return;
        }
        let identifier = device_identifier(&self.mqtt_cfg.topic_prefix, &rec.slug);
        let topic = device_discovery_topic(&self.hass_cfg.discovery_prefix, &identifier);
        let payload = discovery_payload(rec, &self.mqtt_cfg, &self.hass_cfg);
        self.publish_retained(&topic, payload, "discovery").await;
    }

    async fn publish_retained(&self, topic: &str, payload: Vec<u8>, what: &str) {
        if let Err(e) = self.mqtt.publish_bytes(topic, payload, true).await {
            warn!(%topic, what, error = %e, "failed to publish retained");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DeviceRecord;
    use rumqttc::{AsyncClient, MqttOptions};

    fn mqtt_cfg() -> MqttConfig {
        MqttConfig {
            host: "x".into(),
            port: 1883,
            username: None,
            password: None,
            topic_prefix: "evmqtt".into(),
            client_id_prefix: "test".into(),
            keepalive_secs: 30,
        }
    }

    fn hass_cfg() -> HassConfig {
        HassConfig {
            enabled: true,
            discovery_prefix: "homeassistant".into(),
            name: "evmqtt".into(),
        }
    }

    fn fake_mqtt() -> MqttHandle {
        // Eventloop is dropped: publishes accumulate in the bounded
        // request channel, which is fine as long as tests stay well
        // under capacity (100).
        let (client, _eventloop) =
            AsyncClient::new(MqttOptions::new("test", "127.0.0.1", 1883), 100);
        MqttHandle::new(client)
    }

    fn rec(slug: &str, enabled: bool) -> DeviceRecord {
        DeviceRecord {
            slug: slug.to_string(),
            name: "Test Device".to_string(),
            unique_id: None,
            bus: 0,
            vendor: 0,
            product: 0,
            version: 0,
            physical_path: None,
            capability_fingerprint: None,
            capability_tag: None,
            enabled,
            observed_keys: Vec::new(),
        }
    }

    fn make_coord(initial: Vec<DeviceRecord>) -> (Coordinator, std::path::PathBuf) {
        let tmp = std::env::temp_dir().join(format!(
            "evmqtt-test-{}-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let db = Database { devices: initial };
        let (tx, _rx) = mpsc::unbounded_channel();
        let coord = Coordinator::new(db, tmp.clone(), fake_mqtt(), mqtt_cfg(), hass_cfg(), tx);
        (coord, tmp)
    }

    #[tokio::test]
    async fn on_enable_is_idempotent_when_state_matches() {
        // Echo absorption: HA's own publishes come back over the
        // subscription, and a no-op must not rewrite the DB.
        let (mut coord, db_path) = make_coord(vec![rec("kbd", true)]);
        coord
            .handle(CoordinatorMsg::EnableCommand {
                slug: "kbd".into(),
                on: true,
            })
            .await;
        assert!(
            !db_path.exists(),
            "no-op enable must not persist the DB at {}",
            db_path.display(),
        );
    }

    #[tokio::test]
    async fn on_enable_persists_when_state_changes() {
        let (mut coord, db_path) = make_coord(vec![rec("kbd", false)]);
        coord
            .handle(CoordinatorMsg::EnableCommand {
                slug: "kbd".into(),
                on: true,
            })
            .await;
        assert!(
            db_path.exists(),
            "real state change must persist the DB at {}",
            db_path.display(),
        );
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn empty_payload_on_enabled_topic_routes_to_remove() {
        let (mut coord, db_path) = make_coord(vec![rec("kbd", false)]);
        coord
            .handle_mqtt(crate::mqtt::IncomingPublish {
                topic: "evmqtt/_devices/kbd/enabled".to_string(),
                payload: Vec::new(),
                retain: true,
            })
            .await;
        assert!(
            coord.db.find("kbd").is_none(),
            "remove must drop the record"
        );
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn on_payload_routes_to_enable_command() {
        let (mut coord, db_path) = make_coord(vec![rec("kbd", false)]);
        coord
            .handle_mqtt(crate::mqtt::IncomingPublish {
                topic: "evmqtt/_devices/kbd/enabled".to_string(),
                payload: PAYLOAD_ENABLED.as_bytes().to_vec(),
                retain: true,
            })
            .await;
        assert_eq!(coord.db.find("kbd").map(|r| r.enabled), Some(true));
        let _ = std::fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn unrecognised_payload_is_ignored() {
        let (mut coord, db_path) = make_coord(vec![rec("kbd", false)]);
        coord
            .handle_mqtt(crate::mqtt::IncomingPublish {
                topic: "evmqtt/_devices/kbd/enabled".to_string(),
                payload: b"maybe".to_vec(),
                retain: true,
            })
            .await;
        assert_eq!(
            coord.db.find("kbd").map(|r| r.enabled),
            Some(false),
            "bad payloads must not change state",
        );
        assert!(
            !db_path.exists(),
            "no-op must not persist the DB at {}",
            db_path.display(),
        );
    }
}
