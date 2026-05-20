use crate::config::MqttConfig;
use crate::topics::{PAYLOAD_AVAILABLE, PAYLOAD_NOT_AVAILABLE};
use rumqttc::{AsyncClient, Event, EventLoop, LastWill, MqttOptions, Packet, QoS};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace};

/// Minimal MQTT publish surface, abstracted so the per-key publish path
/// in `monitor::handle_key_event` can be exercised against a fake in tests.
/// `MqttHandle` is the production implementation.
pub trait Publisher: Send + Sync {
    fn publish_str(
        &self,
        topic: &str,
        payload: &str,
        retain: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;

    fn publish_bytes(
        &self,
        topic: &str,
        payload: Vec<u8>,
        retain: bool,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// Cloneable wrapper around the rumqttc `AsyncClient`.
#[derive(Clone)]
pub struct MqttHandle {
    client: Arc<AsyncClient>,
}

impl MqttHandle {
    pub fn new(client: AsyncClient) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    pub async fn publish_str(
        &self,
        topic: &str,
        payload: &str,
        retain: bool,
    ) -> Result<(), rumqttc::ClientError> {
        self.client
            .publish(topic, QoS::AtLeastOnce, retain, payload.as_bytes().to_vec())
            .await
    }

    pub async fn publish_bytes(
        &self,
        topic: &str,
        payload: Vec<u8>,
        retain: bool,
    ) -> Result<(), rumqttc::ClientError> {
        self.client
            .publish(topic, QoS::AtLeastOnce, retain, payload)
            .await
    }

    pub async fn disconnect(&self) -> Result<(), rumqttc::ClientError> {
        self.client.disconnect().await
    }
}

impl Publisher for MqttHandle {
    async fn publish_str(&self, topic: &str, payload: &str, retain: bool) -> anyhow::Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, retain, payload.as_bytes().to_vec())
            .await
            .map_err(anyhow::Error::from)
    }

    async fn publish_bytes(
        &self,
        topic: &str,
        payload: Vec<u8>,
        retain: bool,
    ) -> anyhow::Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, retain, payload)
            .await
            .map_err(anyhow::Error::from)
    }
}

pub struct MqttRuntime {
    pub handle: MqttHandle,
    pub event_task: JoinHandle<()>,
}

/// Build the client + spawn the event-loop driver task.
///
/// The driver task owns the rumqttc `EventLoop`, processes
/// connect/disconnect/reconnect, and logs traffic at `trace` for
/// debugging. We don't subscribe to anything — device_triggers are
/// purely outbound.
pub fn spawn(cfg: &MqttConfig, client_id: String, availability_topic: String) -> MqttRuntime {
    info!(
        host = cfg.host,
        port = cfg.port,
        client_id,
        "connecting to MQTT broker"
    );

    let mut options = MqttOptions::new(client_id, cfg.host.clone(), cfg.port);
    options.set_keep_alive(Duration::from_secs(cfg.keepalive_secs.max(5) as u64));
    options.set_clean_session(true);
    options.set_last_will(LastWill::new(
        availability_topic,
        PAYLOAD_NOT_AVAILABLE.as_bytes().to_vec(),
        QoS::AtLeastOnce,
        true,
    ));
    if let (Some(u), Some(p)) = (cfg.username.as_deref(), cfg.password.as_deref())
        && !u.is_empty()
    {
        options.set_credentials(u, p);
    }

    let (client, eventloop) = AsyncClient::new(options, 100);
    let handle = MqttHandle::new(client);
    let event_task = tokio::spawn(run_eventloop(eventloop));
    MqttRuntime { handle, event_task }
}

async fn run_eventloop(mut eventloop: EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                info!(?ack, "MQTT connected");
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                debug!(topic = %p.topic, "received publish (no subscribers — dropping)");
            }
            Ok(other) => {
                trace!(?other, "mqtt traffic");
            }
            Err(e) => {
                error!(error = %e, "MQTT event loop error, will retry");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// Publish `online` on the availability topic, retained.
pub async fn announce_online(
    handle: &MqttHandle,
    availability_topic: &str,
) -> Result<(), rumqttc::ClientError> {
    handle
        .publish_str(availability_topic, PAYLOAD_AVAILABLE, true)
        .await
}

/// Publish `offline` on the availability topic, retained.
pub async fn announce_offline(
    handle: &MqttHandle,
    availability_topic: &str,
) -> Result<(), rumqttc::ClientError> {
    handle
        .publish_str(availability_topic, PAYLOAD_NOT_AVAILABLE, true)
        .await
}

// Re-export under the old name for users who care; otherwise unused.
#[allow(dead_code)]
pub use rumqttc::ClientError;
