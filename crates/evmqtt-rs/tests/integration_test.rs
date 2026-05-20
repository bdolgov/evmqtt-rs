//! End-to-end integration test.
//!
//! Wiring:
//!   • Embedded rumqttd broker bound to a free local port.
//!   • A rumqttc subscriber subscribed to `#` so every publish during
//!     the test is logged to stdout (run with `--nocapture` to see it);
//!     `evmqtt/+/action` publishes are also demuxed into one channel
//!     per configured device, which the assertions pull from.
//!   • evmqtt-rs running in-process against the embedded broker, with
//!     two `[[device]]` entries (different `bus_vendor_product_version`
//!     matchers, different `mqtt_path`s).
//!   • uinput virtual devices that the test connects, disconnects, and
//!     reconnects across the flow — each carrying its config's id quad.
//!
//! When `/dev/uinput` isn't writable the test panics with an instruction
//! to skip it explicitly via `cargo test -- --skip integration_test`.
//! `UniqueId` matchers can't be exercised here because the kernel has no
//! `UI_SET_UNIQ` ioctl — see `monitor::tests` for that path.

#![cfg(target_os = "linux")]

use KeyState::*;
use evdev::uinput::VirtualDevice;
use evdev::{AttributeSet, BusType, EventType, InputEvent, InputId, KeyCode};

/// Press / release as a self-documenting third arg to [`Harness::send_key`].
#[derive(Copy, Clone, Debug)]
enum KeyState {
    Press,
    Release,
}

impl KeyState {
    fn value(self) -> i32 {
        match self {
            Press => 1,
            Release => 0,
        }
    }
}
use evmqtt_rs::app;
use evmqtt_rs::config::{Config, DeviceConfig, DeviceMatcher, HassConfig, MqttConfig};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, Config as BrokerConfig, ConnectionSettings, RouterConfig, ServerSettings};
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::Path;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_test() {
    require_uinput();

    let mut h = Harness::new(vec![
        DeviceSpec::new("dev-one", 0x06, 0xCAFE, 0xBABE, 0x0001),
        DeviceSpec::new("dev-two", 0x06, 0xDEAD, 0xBEEF, 0x0001),
    ])
    .await;

    h.connect(0).await;
    h.start_server().await;
    h.send_key(0, KeyCode::KEY_A, Press);
    h.assert_publish("evmqtt/dev-one/action", "a_press").await;
    h.send_key(0, KeyCode::KEY_A, Release);
    h.assert_publish("evmqtt/dev-one/action", "a_release").await;
    h.disconnect(0).await;
    h.connect(1).await;
    h.send_key(1, KeyCode::KEY_B, Press);
    h.assert_publish("evmqtt/dev-two/action", "b_press").await;
    h.send_key(1, KeyCode::KEY_B, Release);
    h.assert_publish("evmqtt/dev-two/action", "b_release").await;
    h.connect(0).await;
    h.send_key(0, KeyCode::KEY_C, Press);
    h.assert_publish("evmqtt/dev-one/action", "c_press").await;
    h.send_key(0, KeyCode::KEY_C, Release);
    h.assert_publish("evmqtt/dev-one/action", "c_release").await;
    h.send_key(1, KeyCode::KEY_D, Press);
    h.assert_publish("evmqtt/dev-two/action", "d_press").await;
    h.send_key(1, KeyCode::KEY_D, Release);
    h.assert_publish("evmqtt/dev-two/action", "d_release").await;
    h.shutdown().await;
}

// ──────────────────────────────────────────────────────────────────────
// Harness
// ──────────────────────────────────────────────────────────────────────

/// One configured device in the test scenario.
struct DeviceSpec {
    mqtt_path: String,
    bus: u16,
    vendor: u16,
    product: u16,
    version: u16,
}

impl DeviceSpec {
    fn new(mqtt_path: &str, bus: u16, vendor: u16, product: u16, version: u16) -> Self {
        Self {
            mqtt_path: mqtt_path.to_string(),
            bus,
            vendor,
            product,
            version,
        }
    }

    fn action_topic(&self) -> String {
        format!("evmqtt/{}/action", self.mqtt_path)
    }

    fn to_device_config(&self) -> DeviceConfig {
        DeviceConfig {
            matcher: DeviceMatcher::BusVendorProductVersion(
                self.bus,
                self.vendor,
                self.product,
                self.version,
            ),
            name: format!("Test {}", self.mqtt_path),
            mqtt_path: Some(self.mqtt_path.clone()),
        }
    }
}

struct Harness {
    specs: Vec<DeviceSpec>,
    /// Currently-connected virtual device per spec index (`None` ⇒ disconnected).
    slots: Vec<Option<VirtualDevice>>,
    /// One channel per configured device's action topic. The subscriber
    /// demuxes by topic so a stray publish on one device never lands in
    /// a neighbour's channel.
    msg_rxs: HashMap<String, mpsc::UnboundedReceiver<String>>,
    port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    app_task: Option<JoinHandle<()>>,
}

impl Harness {
    /// Spin up the broker and the always-on subscriber. The server
    /// (evmqtt-rs) is started later by [`start_server`].
    async fn new(specs: Vec<DeviceSpec>) -> Self {
        let port = pick_free_port();
        std::thread::spawn(move || {
            let cfg = build_broker_config(port);
            let mut broker = Broker::new(cfg);
            broker.start().expect("broker.start");
        });
        wait_for_tcp_ready("127.0.0.1", port).await;

        // One channel per device's action topic. Subscriber routes by
        // exact topic match into the right channel and logs everything
        // it sees (whether routed or not) to stdout.
        let mut action_topic_txs: HashMap<String, mpsc::UnboundedSender<String>> = HashMap::new();
        let mut msg_rxs: HashMap<String, mpsc::UnboundedReceiver<String>> = HashMap::new();
        for spec in &specs {
            let (tx, rx) = mpsc::unbounded_channel::<String>();
            action_topic_txs.insert(spec.action_topic(), tx);
            msg_rxs.insert(spec.action_topic(), rx);
        }
        spawn_subscriber(port, action_topic_txs).await;

        let slots = (0..specs.len()).map(|_| None).collect();
        Self {
            specs,
            slots,
            msg_rxs,
            port,
            shutdown_tx: None,
            app_task: None,
        }
    }

    /// Spawn evmqtt-rs in-process and wait for the watcher to settle.
    async fn start_server(&mut self) {
        assert!(self.app_task.is_none(), "server already started");
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let cfg = Config {
            mqtt: MqttConfig {
                host: "127.0.0.1".into(),
                port: self.port,
                username: None,
                password: None,
                topic_prefix: "evmqtt".into(),
                client_id_prefix: "evmqtt-rs-test".into(),
                keepalive_secs: 30,
            },
            hass: HassConfig::default(),
            devices: self.specs.iter().map(|s| s.to_device_config()).collect(),
        };
        eprintln!(
            "[harness] starting evmqtt-rs against 127.0.0.1:{}",
            self.port
        );
        let task = tokio::spawn(async move {
            let _ = app::run_with_shutdown(cfg, async move {
                shutdown_rx.await.ok();
            })
            .await;
        });
        self.shutdown_tx = Some(shutdown_tx);
        self.app_task = Some(task);
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    /// Create the virtual device for `idx` in uinput. The kernel issues
    /// inotify CREATE/ATTRIB on `/dev/input`; we wait briefly so the
    /// evmqtt-rs watcher has time to open and grab the new node.
    async fn connect(&mut self, idx: usize) {
        assert!(
            self.slots[idx].is_none(),
            "device {idx} ({}) already connected",
            self.specs[idx].mqtt_path,
        );
        eprintln!(
            "[harness] connect device[{idx}] ({})",
            self.specs[idx].mqtt_path
        );
        self.slots[idx] = Some(build_virtual_device(&self.specs[idx]));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    /// Drop the virtual device for `idx`. The kernel removes the
    /// `/dev/input/eventN` node and evmqtt-rs's monitor task exits on
    /// ENODEV, freeing the config slot for the next reconnect.
    async fn disconnect(&mut self, idx: usize) {
        eprintln!(
            "[harness] disconnect device[{idx}] ({})",
            self.specs[idx].mqtt_path,
        );
        let _ = self.slots[idx]
            .take()
            .unwrap_or_else(|| panic!("device {idx} not connected"));
        tokio::time::sleep(Duration::from_millis(600)).await;
    }

    /// Emit a single press or release event for `key` on device `idx`.
    /// `VirtualDevice::emit` appends `SYN_REPORT`, so each call sends
    /// exactly one self-contained input batch — same shape a real
    /// keyboard reports the two phases in.
    fn send_key(&mut self, idx: usize, key: KeyCode, state: KeyState) {
        eprintln!(
            "[harness] send_key device[{idx}] ({}) key={key:?} state={state:?}",
            self.specs[idx].mqtt_path,
        );
        let device = self.slots[idx]
            .as_mut()
            .unwrap_or_else(|| panic!("device {idx} not connected"));
        let ev = InputEvent::new(EventType::KEY.0, key.code(), state.value());
        device.emit(&[ev]).expect("emit");
    }

    /// Strict assertion: the *next* publish on `topic` must have exactly
    /// `expected_payload`. Fails on mismatch or after 10 s of silence.
    async fn assert_publish(&mut self, topic: &str, expected_payload: &str) {
        let rx = self
            .msg_rxs
            .get_mut(topic)
            .unwrap_or_else(|| panic!("no channel registered for topic {topic}"));
        let payload = match timeout(Duration::from_secs(10), rx.recv()).await {
            Ok(Some(p)) => p,
            Ok(None) => panic!("subscriber channel closed for {topic}"),
            Err(_) => panic!("timed out waiting for `{expected_payload}` on {topic}"),
        };
        assert_eq!(payload, expected_payload, "publish on {topic} mismatched");
    }

    async fn shutdown(mut self) {
        eprintln!("[harness] shutdown");
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(()).ok();
        }
        if let Some(task) = self.app_task.take() {
            let _ = timeout(Duration::from_secs(5), task)
                .await
                .expect("evmqtt-rs did not shut down within 5s");
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Lower-level helpers
// ──────────────────────────────────────────────────────────────────────

fn require_uinput() {
    let path = Path::new("/dev/uinput");
    if let Err(e) = std::fs::OpenOptions::new().write(true).open(path) {
        panic!(
            "uinput not available: cannot open /dev/uinput for writing ({e}). \
             This integration test needs a virtual input device. Either give \
             the running user write access to /dev/uinput (typically by being \
             in the `input` group and shipping a udev rule), or skip this test \
             with:\n    cargo test -- --skip integration_test",
        );
    }
}

fn pick_free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

async fn wait_for_tcp_ready(host: &str, port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect((host, port)).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("broker did not accept connections at {host}:{port} within 5s");
}

/// Subscribe to `#`, log every received publish, and route action-topic
/// publishes that match a known device into the per-topic channels.
async fn spawn_subscriber(
    port: u16,
    action_topic_txs: HashMap<String, mpsc::UnboundedSender<String>>,
) {
    let mut opts = MqttOptions::new("e2e-sub", "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(opts, 100);
    tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let mut subbed = false;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    if !subbed {
                        client
                            .subscribe("#", QoS::AtLeastOnce)
                            .await
                            .expect("subscribe");
                        subbed = true;
                    }
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    let topic = p.topic.clone();
                    let payload = String::from_utf8_lossy(&p.payload).to_string();
                    let retain_tag = if p.retain { " [retained]" } else { "" };
                    eprintln!("[mqtt]{retain_tag} {topic} -> {payload}");
                    if let Some(tx) = action_topic_txs.get(topic.as_str()) {
                        let _ = tx.send(payload);
                    }
                }
                Ok(_) => {}
                Err(_) if started.elapsed() > Duration::from_secs(30) => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    });
    // Give the subscribe a moment to be acknowledged before the caller
    // proceeds to spawn evmqtt-rs.
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn build_virtual_device(spec: &DeviceSpec) -> VirtualDevice {
    // Advertise the keys the test scenario actually uses.
    let mut keys = AttributeSet::<KeyCode>::new();
    keys.insert(KeyCode::KEY_A);
    keys.insert(KeyCode::KEY_B);
    keys.insert(KeyCode::KEY_C);
    keys.insert(KeyCode::KEY_D);
    let id = InputId::new(BusType(spec.bus), spec.vendor, spec.product, spec.version);
    let name = format!("evmqtt-rs e2e {}", spec.mqtt_path);
    VirtualDevice::builder()
        .expect("open /dev/uinput")
        .name(name.as_bytes())
        .input_id(id)
        .with_keys(&keys)
        .expect("with_keys")
        .build()
        .expect("uinput build")
}

fn build_broker_config(port: u16) -> BrokerConfig {
    let mut v4 = HashMap::new();
    v4.insert(
        "v4-1".to_string(),
        ServerSettings {
            name: "v4-1".to_string(),
            listen: format!("127.0.0.1:{port}").parse().unwrap(),
            tls: None,
            next_connection_delay_ms: 1,
            connections: ConnectionSettings {
                connection_timeout_ms: 5_000,
                max_payload_size: 20_480,
                max_inflight_count: 100,
                auth: None,
                external_auth: None,
                dynamic_filters: true,
            },
        },
    );
    BrokerConfig {
        id: 0,
        router: RouterConfig {
            max_connections: 32,
            max_outgoing_packet_count: 200,
            max_segment_size: 104_857_600,
            max_segment_count: 10,
            custom_segment: None,
            initialized_filters: None,
            shared_subscriptions_strategy: Default::default(),
        },
        v4: Some(v4),
        v5: None,
        ws: None,
        cluster: None,
        console: None,
        bridge: None,
        prometheus: None,
        metrics: None,
    }
}
