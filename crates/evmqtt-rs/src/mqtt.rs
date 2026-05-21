use crate::config::MqttConfig;
use crate::topics::PAYLOAD_NOT_AVAILABLE;
use anyhow::{Result, anyhow};
use rumqttc::{AsyncClient, ConnectionError, Event, EventLoop, LastWill, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace};

#[derive(Debug, Clone)]
pub struct IncomingPublish {
    pub topic: String,
    pub payload: Vec<u8>,
    pub retain: bool,
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
        trace!(%topic, retain, payload, "publish");
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
        trace!(
            %topic,
            retain,
            payload = %String::from_utf8_lossy(&payload),
            "publish",
        );
        self.client
            .publish(topic, QoS::AtLeastOnce, retain, payload)
            .await
    }

    pub async fn publish_empty_retained(&self, topic: &str) -> Result<(), rumqttc::ClientError> {
        trace!(%topic, retain = true, payload = "", "publish (empty)");
        self.client
            .publish(topic, QoS::AtLeastOnce, true, Vec::new())
            .await
    }

    pub async fn subscribe(&self, topic: &str) -> Result<(), rumqttc::ClientError> {
        trace!(%topic, "subscribe");
        self.client.subscribe(topic, QoS::AtLeastOnce).await
    }

    pub async fn disconnect(&self) -> Result<(), rumqttc::ClientError> {
        trace!("disconnect");
        self.client.disconnect().await
    }
}

pub struct MqttRuntime {
    pub handle: MqttHandle,
    pub event_task: JoinHandle<()>,
    /// Receives every inbound `Publish` packet. Consumers filter by
    /// topic; mqtt.rs deliberately stays topic-agnostic.
    pub incoming: mpsc::UnboundedReceiver<IncomingPublish>,
    /// Set by [`graceful_shutdown`] before the deliberate DISCONNECT.
    /// While set, the eventloop swallows errors at `debug` instead of
    /// logging them at `error` and retrying -- this suppresses the
    /// "Connection closed by peer abruptly" message that always
    /// follows our own DISCONNECT.
    pub shutdown_flag: Arc<AtomicBool>,
    /// Last observed connection state. Receivers can `wait_ready` to
    /// block until either Connected or PermanentFailure, or watch for
    /// post-startup PermanentFailure transitions to abort the daemon.
    pub conn_state: watch::Receiver<ConnState>,
}

/// Three-state lifecycle of the rumqttc eventloop:
/// * `Connecting` -- initial; or a transient error is being retried.
/// * `Connected` -- the broker has sent CONNACK.
/// * `PermanentFailure(reason)` -- a clearly-fatal error (`ConnectionRefused`, TLS handshake
///   failure, protocol mismatch) was observed; the eventloop has stopped retrying and exited.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ConnState {
    #[default]
    Connecting,
    Connected,
    PermanentFailure(String),
}

impl MqttRuntime {
    /// Block until the broker acknowledges CONNECT (`Connected`), the
    /// eventloop reports a permanent error, or `timeout` elapses.
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        wait_ready(self.conn_state.clone(), timeout).await
    }
}

async fn wait_ready(mut rx: watch::Receiver<ConnState>, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        match &*rx.borrow_and_update() {
            ConnState::Connected => return Ok(()),
            ConnState::PermanentFailure(msg) => {
                return Err(anyhow!("MQTT connection failed: {msg}"));
            }
            ConnState::Connecting => {}
        }
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    return Err(anyhow!("MQTT eventloop terminated before connecting"));
                }
            }
            _ = &mut deadline => {
                return Err(anyhow!("timed out after {timeout:?} waiting for MQTT CONNACK"));
            }
        }
    }
}

/// Resolve when the eventloop transitions into `PermanentFailure`.
/// Used by the daemon to compose its main shutdown future so that a
/// fatal MQTT error after startup exits the process instead of running
/// in a dead-MQTT state forever.
pub async fn wait_for_permanent_failure(mut rx: watch::Receiver<ConnState>) {
    loop {
        if matches!(*rx.borrow_and_update(), ConnState::PermanentFailure(_)) {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Heuristic for "this error will not go away by retrying".
/// Conservative: only the variants where rumqttc has definitively
/// heard "no" from the peer count. Anything that might just be the
/// network being unreachable for a moment stays transient.
fn is_permanent(e: &ConnectionError) -> bool {
    matches!(
        e,
        ConnectionError::ConnectionRefused(_)
            | ConnectionError::NotConnAck(_)
            | ConnectionError::Tls(_)
    )
}

/// Build the client + spawn the event-loop driver task.
///
/// The driver task owns the rumqttc `EventLoop`, processes
/// connect/disconnect/reconnect, and forwards every inbound publish to
/// the `incoming` channel. Subscriptions are the caller's
/// responsibility (use `MqttHandle::subscribe`).
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
    let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let (conn_tx, conn_rx) = watch::channel(ConnState::Connecting);
    let event_task = tokio::spawn(run_eventloop(
        eventloop,
        incoming_tx,
        shutdown_flag.clone(),
        conn_tx,
    ));
    MqttRuntime {
        handle,
        event_task,
        incoming: incoming_rx,
        shutdown_flag,
        conn_state: conn_rx,
    }
}

async fn run_eventloop(
    mut eventloop: EventLoop,
    incoming: mpsc::UnboundedSender<IncomingPublish>,
    shutdown_flag: Arc<AtomicBool>,
    conn_state: watch::Sender<ConnState>,
) {
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                info!(?ack, "MQTT connected");
                let _ = conn_state.send(ConnState::Connected);
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                trace!(
                    topic = %p.topic,
                    retain = p.retain,
                    payload = %String::from_utf8_lossy(&p.payload),
                    "incoming publish",
                );
                let msg = IncomingPublish {
                    topic: p.topic.clone(),
                    payload: p.payload.to_vec(),
                    retain: p.retain,
                };
                if incoming.send(msg).is_err() {
                    debug!("incoming channel closed; dropping publish");
                }
            }
            Ok(other) => {
                trace!(?other, "mqtt traffic");
            }
            Err(e) => {
                if shutdown_flag.load(Ordering::Relaxed) {
                    debug!(error = %e, "eventloop error after shutdown; exiting");
                    return;
                }
                if is_permanent(&e) {
                    error!(error = %e, "MQTT permanent failure; eventloop exiting (will not retry)");
                    let _ = conn_state.send(ConnState::PermanentFailure(e.to_string()));
                    return;
                }
                error!(error = %e, "MQTT event loop error, will retry");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

/// Politely close the connection. Sets the shutdown flag (so any
/// subsequent eventloop error is silenced), sends DISCONNECT, then
/// waits up to half a second for the eventloop task to exit on its
/// own; aborts if it doesn't.
pub async fn graceful_shutdown(
    handle: MqttHandle,
    event_task: JoinHandle<()>,
    shutdown_flag: Arc<AtomicBool>,
) {
    shutdown_flag.store(true, Ordering::Relaxed);
    let _ = handle.disconnect().await;
    let abort = event_task.abort_handle();
    let _ = tokio::time::timeout(Duration::from_millis(500), event_task).await;
    abort.abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::{ConnectReturnCode, Packet, StateError};

    #[test]
    fn is_permanent_for_connection_refused() {
        let e = ConnectionError::ConnectionRefused(ConnectReturnCode::BadUserNamePassword);
        assert!(is_permanent(&e));
    }

    #[test]
    fn is_permanent_for_not_connack() {
        let e = ConnectionError::NotConnAck(Packet::PingResp);
        assert!(is_permanent(&e));
    }

    #[test]
    fn is_not_permanent_for_transient_errors() {
        // A bare I/O error (e.g. "connection reset") is the canonical
        // transient case: the broker is reachable but flaky, retrying
        // is the right answer.
        let e = ConnectionError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert!(!is_permanent(&e));
        assert!(!is_permanent(&ConnectionError::NetworkTimeout));
        assert!(!is_permanent(&ConnectionError::FlushTimeout));
        assert!(!is_permanent(&ConnectionError::MqttState(
            StateError::ConnectionAborted,
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_returns_immediately_when_already_connected() {
        let (tx, rx) = watch::channel(ConnState::Connected);
        let _keep = tx; // keep the sender alive for the duration of the await
        wait_ready(rx, Duration::from_secs(10))
            .await
            .expect("Connected should resolve immediately");
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_returns_on_connected_transition() {
        let (tx, rx) = watch::channel(ConnState::Connecting);
        let join = tokio::spawn(wait_ready(rx, Duration::from_secs(10)));
        // Yield so the task hits its first borrow and registers for changes.
        tokio::task::yield_now().await;
        tx.send(ConnState::Connected).unwrap();
        join.await.unwrap().expect("Connected should resolve");
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_errors_on_permanent_failure() {
        let (_tx, rx) = watch::channel(ConnState::PermanentFailure("bad creds".into()));
        let err = wait_ready(rx, Duration::from_secs(10))
            .await
            .expect_err("PermanentFailure must fail");
        assert!(err.to_string().contains("bad creds"));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_errors_on_timeout() {
        let (_tx, rx) = watch::channel(ConnState::Connecting);
        let err = wait_ready(rx, Duration::from_millis(50))
            .await
            .expect_err("timeout must fail");
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_ready_errors_when_sender_dropped() {
        let (tx, rx) = watch::channel(ConnState::Connecting);
        let join = tokio::spawn(wait_ready(rx, Duration::from_secs(10)));
        tokio::task::yield_now().await;
        drop(tx);
        let err = join.await.unwrap().expect_err("dropped sender must fail");
        assert!(err.to_string().contains("terminated before connecting"));
    }
}
