//! End-to-end integration test for the dynamic-device flow.
//!
//! Two uinput devices, one daemon, one embedded broker. The test
//! drives the public flow from outside the binary:
//!   1. Both devices appear in MQTT and HA, disabled by default.
//!   2. Enabling one device starts monitoring only that device.
//!   3. Key presses on the enabled device flow to its action topic
//!      and grow its HA discovery payload to include the new keys.
//!   4. Enabling the second device works independently of the first.
//!   5. Removing one device clears its retained MQTT state without
//!      touching the other.
//!
//! Skip on systems where `/dev/uinput` is not writable:
//!     cargo test -- --skip integration_test

#![cfg(target_os = "linux")]

use evdev::uinput::VirtualDevice;
use evdev::{AttributeSet, BusType, EventType, InputEvent, InputId, KeyCode};
use evmqtt_rs::daemon;
use evmqtt_rs::client::Client;
use evmqtt_rs::config::{HassConfig, MqttConfig};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use rumqttd::{Broker, Config as BrokerConfig, ConnectionSettings, RouterConfig, ServerSettings};
use serde_json::Value;
use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_test() {
    require_uinput();

    let mut h = Harness::new().await;
    h.start_daemon().await;

    // 1. Connect two devices with distinct (vendor, product) so the
    //    daemon allocates two slugs without collision. Slug derivation
    //    lives in `slug::slugify` and is exercised by its own unit
    //    tests; the names below slugify to "evmqtt-rs-e2e-a-kbd" and
    //    "evmqtt-rs-e2e-b-kbd" -- both used as literal strings throughout
    //    so a reader can grep for either.
    h.connect_device(0, "evmqtt-rs e2e A", 0xCAFE, 0xBABE);
    h.connect_device(1, "evmqtt-rs e2e B", 0xDEAD, 0xBEEF);

    // 2. Both devices are announced as retained info + retained
    //    enabled=off + retained HA discovery.
    let info_a_json: Value =
        serde_json::from_str(&h.wait_retained("evmqtt/_devices/evmqtt-rs-e2e-a-kbd").await).unwrap();
    let info_b_json: Value =
        serde_json::from_str(&h.wait_retained("evmqtt/_devices/evmqtt-rs-e2e-b-kbd").await).unwrap();
    assert_eq!(info_a_json["slug"], "evmqtt-rs-e2e-a-kbd");
    assert_eq!(info_a_json["name"], "evmqtt-rs e2e A (kbd)");
    assert_eq!(info_a_json["vendor"], 0xCAFE);
    assert_eq!(info_a_json["product"], 0xBABE);
    assert!(
        info_a_json.get("observed_keys").is_none(),
        "info topic must not carry observed_keys",
    );
    assert_eq!(info_b_json["slug"], "evmqtt-rs-e2e-b-kbd");
    assert_eq!(info_b_json["vendor"], 0xDEAD);
    assert_eq!(info_b_json["product"], 0xBEEF);

    assert_eq!(
        h.wait_retained("evmqtt/_devices/evmqtt-rs-e2e-a-kbd/enabled")
            .await,
        "off"
    );
    assert_eq!(
        h.wait_retained("evmqtt/_devices/evmqtt-rs-e2e-b-kbd/enabled")
            .await,
        "off"
    );

    let disc_a_json: Value = serde_json::from_str(
        &h.wait_retained("homeassistant/device/evmqtt_evmqtt-rs-e2e-a-kbd/config")
            .await,
    )
    .unwrap();
    let disc_b_json: Value = serde_json::from_str(
        &h.wait_retained("homeassistant/device/evmqtt_evmqtt-rs-e2e-b-kbd/config")
            .await,
    )
    .unwrap();
    assert!(disc_a_json["cmps"]["enabled"].is_object());
    assert!(disc_b_json["cmps"]["enabled"].is_object());
    assert!(
        !disc_a_json["cmps"]
            .as_object()
            .unwrap()
            .contains_key("a_press"),
        "no triggers before any keys are observed on A",
    );

    // 3. Enable A only. The daemon mirrors "on" back and starts the
    //    monitor for A; B stays unmonitored.
    h.enable("evmqtt-rs-e2e-a-kbd").await;
    h.wait_for_publish("evmqtt/_devices/evmqtt-rs-e2e-a-kbd/enabled", |p| p == "on")
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Press a key on A. Action publish lands; discovery republishes
    // with a_press / a_release components.
    h.send_key(0, KeyCode::KEY_A, true);
    h.wait_for_publish("evmqtt/evmqtt-rs-e2e-a-kbd/action", |p| p == "a_press")
        .await;
    h.send_key(0, KeyCode::KEY_A, false);
    h.wait_for_publish("evmqtt/evmqtt-rs-e2e-a-kbd/action", |p| p == "a_release")
        .await;
    h.wait_for_publish("homeassistant/device/evmqtt_evmqtt-rs-e2e-a-kbd/config", |p| {
        p.contains("a_press") && p.contains("a_release")
    })
    .await;

    // B is still disabled. Pressing on B must not produce an action
    // publish on B's topic.
    h.send_key(1, KeyCode::KEY_C, true);
    h.send_key(1, KeyCode::KEY_C, false);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !h.has_seen("evmqtt/evmqtt-rs-e2e-b-kbd/action"),
        "B is disabled; no action publishes should appear for it",
    );

    // 4. Drop A's uinput device and recreate it under the same
    //    name+vendor+product. This is the bluetooth-keyboard reconnect
    //    case: the kernel hands out a different /dev/input/eventN, but
    //    the daemon must match the new identity to the existing DB
    //    record and resume monitoring under the original slug. The
    //    next keypress on the *new* uinput instance must still appear
    //    on `evmqtt/evmqtt-rs-e2e-a-kbd/action`, not on a freshly minted
    //    slug.
    h.disconnect_device(0);
    tokio::time::sleep(Duration::from_millis(500)).await;
    h.connect_device(0, "evmqtt-rs e2e A", 0xCAFE, 0xBABE);
    tokio::time::sleep(Duration::from_millis(700)).await;
    h.send_key(0, KeyCode::KEY_D, true);
    h.wait_for_publish("evmqtt/evmqtt-rs-e2e-a-kbd/action", |p| p == "d_press")
        .await;
    h.send_key(0, KeyCode::KEY_D, false);
    h.wait_for_publish("evmqtt/evmqtt-rs-e2e-a-kbd/action", |p| p == "d_release")
        .await;
    // The reconnect must not have re-registered A as a brand new
    // device under a `-2` slug.
    assert!(
        !h.has_seen("evmqtt/_devices/evmqtt-rs-e2e-a-kbd-2"),
        "reconnect must reuse the existing slug, not allocate -2",
    );

    // 5. Enable B too. Now its key presses propagate independently.
    h.enable("evmqtt-rs-e2e-b-kbd").await;
    h.wait_for_publish("evmqtt/_devices/evmqtt-rs-e2e-b-kbd/enabled", |p| p == "on")
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    h.send_key(1, KeyCode::KEY_B, true);
    h.wait_for_publish("evmqtt/evmqtt-rs-e2e-b-kbd/action", |p| p == "b_press")
        .await;
    h.send_key(1, KeyCode::KEY_B, false);
    h.wait_for_publish("evmqtt/evmqtt-rs-e2e-b-kbd/action", |p| p == "b_release")
        .await;
    h.wait_for_publish("homeassistant/device/evmqtt_evmqtt-rs-e2e-b-kbd/config", |p| {
        p.contains("b_press") && p.contains("b_release")
    })
    .await;

    h.shutdown().await;
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Harness {
    port: u16,
    history: Arc<Mutex<Vec<(String, String, bool)>>>,
    waiters: Arc<Mutex<Vec<Waiter>>>,
    /// `Some` while the slot is "plugged in". `disconnect_device`
    /// drops the VirtualDevice (kernel removes its eventN node);
    /// `connect_device` on the same slot recreates it.
    devices: Vec<Option<VirtualDevice>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    app_task: Option<JoinHandle<()>>,
    db_path: PathBuf,
    _db_dir: tempdir::TempDir,
}

struct Waiter {
    topic: String,
    predicate: Box<dyn Fn(&str) -> bool + Send>,
    tx: Option<oneshot::Sender<String>>,
}

impl Harness {
    async fn new() -> Self {
        let port = pick_free_port();
        std::thread::spawn(move || {
            let cfg = build_broker_config(port);
            let mut broker = Broker::new(cfg);
            broker.start().expect("broker.start");
        });
        wait_for_tcp_ready("127.0.0.1", port).await;

        let history: Arc<Mutex<Vec<(String, String, bool)>>> = Arc::new(Mutex::new(Vec::new()));
        let waiters: Arc<Mutex<Vec<Waiter>>> = Arc::new(Mutex::new(Vec::new()));
        spawn_subscriber(port, history.clone(), waiters.clone()).await;

        let db_dir = tempdir::TempDir::new("evmqtt-rs-e2e").expect("tempdir");
        let db_path = db_dir.path().join("db.toml");

        Self {
            port,
            history,
            waiters,
            devices: Vec::new(),
            shutdown_tx: None,
            app_task: None,
            db_path,
            _db_dir: db_dir,
        }
    }

    fn mqtt_cfg(&self, client_id_prefix: &str) -> MqttConfig {
        MqttConfig {
            host: "127.0.0.1".into(),
            port: self.port,
            username: None,
            password: None,
            topic_prefix: "evmqtt".into(),
            client_id_prefix: client_id_prefix.into(),
            keepalive_secs: 30,
        }
    }

    async fn start_daemon(&mut self) {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mqtt = self.mqtt_cfg("evmqtt-rs-test-daemon");
        let hass = HassConfig {
            enabled: true,
            discovery_prefix: "homeassistant".into(),
            name: "evmqtt".into(),
        };
        let db = self.db_path.clone();
        eprintln!("[harness] starting daemon");
        let task = tokio::spawn(async move {
            let _ = daemon::run_with_shutdown(mqtt, hass, db, async move {
                shutdown_rx.await.ok();
            })
            .await;
        });
        self.shutdown_tx = Some(shutdown_tx);
        self.app_task = Some(task);
        // Give the daemon time to CONNECT, subscribe, settle.
        tokio::time::sleep(Duration::from_millis(900)).await;
    }

    fn connect_device(&mut self, idx: usize, name: &str, vendor: u16, product: u16) {
        let mut keys = AttributeSet::<KeyCode>::new();
        keys.insert(KeyCode::KEY_A);
        keys.insert(KeyCode::KEY_B);
        keys.insert(KeyCode::KEY_C);
        keys.insert(KeyCode::KEY_D);
        let id = InputId::new(BusType(0x06), vendor, product, 0x0001);
        let dev = VirtualDevice::builder()
            .expect("open /dev/uinput")
            .name(name.as_bytes())
            .input_id(id)
            .with_keys(&keys)
            .expect("with_keys")
            .build()
            .expect("uinput build");
        if idx >= self.devices.len() {
            self.devices.resize_with(idx + 1, || None);
        }
        assert!(
            self.devices[idx].is_none(),
            "device slot {idx} is already occupied",
        );
        self.devices[idx] = Some(dev);
    }

    fn disconnect_device(&mut self, idx: usize) {
        let _ = self.devices[idx]
            .take()
            .unwrap_or_else(|| panic!("no device at slot {idx} to disconnect"));
    }

    fn send_key(&mut self, idx: usize, key: KeyCode, press: bool) {
        let dev = self.devices[idx]
            .as_mut()
            .unwrap_or_else(|| panic!("no device at slot {idx}"));
        let ev = InputEvent::new(EventType::KEY.0, key.code(), i32::from(press));
        dev.emit(&[ev]).expect("emit");
    }

    /// Connect a one-shot `Client`, enable the given slug, disconnect.
    /// Same path as `evmqtt-rs --enable-device SLUG`.
    async fn enable(&self, slug: &str) {
        let client = Client::connect(&self.mqtt_cfg("evmqtt-rs-test-cli"))
            .await
            .expect("client connect");
        client.enable_device(slug).await.expect("enable_device");
        client.shutdown().await.expect("client shutdown");
    }

    async fn wait_for_publish<F>(&self, topic: &str, pred: F) -> String
    where
        F: Fn(&str) -> bool + Send + 'static,
    {
        {
            let hist = self.history.lock().unwrap();
            for (t, p, _) in hist.iter() {
                if t == topic && pred(p) {
                    return p.clone();
                }
            }
        }
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().unwrap().push(Waiter {
            topic: topic.to_string(),
            predicate: Box::new(pred),
            tx: Some(tx),
        });
        match timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => panic!("waiter dropped for {topic}"),
            Err(_) => {
                let hist = self.history.lock().unwrap();
                let recent: Vec<_> = hist.iter().rev().take(10).collect();
                panic!("timed out waiting on {topic}; recent traffic: {recent:?}");
            }
        }
    }

    async fn wait_retained(&self, topic: &str) -> String {
        self.wait_for_publish(topic, |_| true).await
    }

    fn has_seen(&self, topic: &str) -> bool {
        let hist = self.history.lock().unwrap();
        hist.iter().any(|(t, _, _)| t == topic)
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            tx.send(()).ok();
        }
        if let Some(task) = self.app_task.take() {
            let _ = timeout(Duration::from_secs(5), task).await;
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn require_uinput() {
    let path = Path::new("/dev/uinput");
    if let Err(e) = std::fs::OpenOptions::new().write(true).open(path) {
        panic!(
            "uinput not available: cannot open /dev/uinput for writing ({e}). \
             Either give the running user write access (typically by joining \
             the `input` group + shipping a udev rule) or skip this test with \
             `cargo test -- --skip integration_test`.",
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

async fn spawn_subscriber(
    port: u16,
    history: Arc<Mutex<Vec<(String, String, bool)>>>,
    waiters: Arc<Mutex<Vec<Waiter>>>,
) {
    let mut opts = MqttOptions::new(format!("e2e-sub-{}", std::process::id()), "127.0.0.1", port);
    opts.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(opts, 200);
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
                    history
                        .lock()
                        .unwrap()
                        .push((topic.clone(), payload.clone(), p.retain));
                    let mut ws = waiters.lock().unwrap();
                    let mut hits = Vec::new();
                    for (i, w) in ws.iter().enumerate() {
                        if w.topic == topic && (w.predicate)(&payload) {
                            hits.push(i);
                        }
                    }
                    for i in hits.into_iter().rev() {
                        let mut waiter = ws.remove(i);
                        if let Some(tx) = waiter.tx.take() {
                            let _ = tx.send(payload.clone());
                        }
                    }
                }
                Ok(_) => {}
                Err(_) if started.elapsed() > Duration::from_secs(120) => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
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

mod tempdir {
    use std::path::{Path, PathBuf};

    pub struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        pub fn new(prefix: &str) -> std::io::Result<Self> {
            let base = std::env::temp_dir();
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let path = base.join(format!("{prefix}-{}-{nonce}", std::process::id()));
            std::fs::create_dir_all(&path)?;
            Ok(Self { path })
        }

        pub fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}
