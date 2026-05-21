# evmqtt-rs architecture

This file describes how the crate is laid out and how the pieces fit
together at runtime. It is meant for someone reading the code for the
first time, and also as a checklist for refactors -- the last section
calls out shape problems worth fixing.

## What the binary does

`evmqtt-rs` is a long-running daemon (and a small management CLI sharing
the same binary) that:

1. Watches `/dev/input/event*` for keyboards.
2. Persists every keyboard it has ever seen in a local TOML database.
3. Mirrors that database into retained MQTT topics under a per-instance
   prefix, including a Home Assistant device-discovery bundle per device.
4. Exposes an "Enabled" switch in HA per device. When enabled, the daemon
   grabs the device, reads `evdev` events, and publishes `*_press` /
   `*_release` events to a per-device action topic.
5. Adds an HA trigger pair for every key it has ever observed on every
   enabled device, lazily.

The CLI verbs (`--list-devices`, `--enable-device`, `--disable-device`,
`--remove-device`) are short-lived MQTT clients that publish to the same
control topics a HA user would touch.

## Module map

```
main.rs        Entry point. Parse Args; dispatch to daemon or cli.
config.rs      clap Args (with EVMQTT_* env), Mode, MqttConfig, HassConfig.
daemon.rs      Long-running daemon top-level wiring + signal handling.
coordinator.rs Actor task: owns Database, in-memory connected/monitored maps,
               drives MQTT publishes, asks monitor::start to spawn monitors.
watcher.rs     /dev/input directory walk + inotify; emits DeviceIdentity
               events to the coordinator.
monitor.rs     `monitor::start` constructs a MonitorHandle (JoinHandle +
               shutdown oneshot) and spawns a per-device task that opens
               the evdev node, grabs it, streams key events, and publishes
               on the pre-built per-device action topic.
discovery.rs   DeviceIdentity struct + open / enumerate helpers.
db.rs          Database, DeviceRecord, atomic save, Schema enum on disk.
hass.rs        HA per-device discovery payload + device-info JSON.
topics.rs      Pure topic-string builders + payload constants + Action.
mqtt.rs        rumqttc wrapper: MqttHandle, MqttRuntime, eventloop
               driver, graceful_shutdown.
slug.rs        slugify + key_slug helpers.
client.rs      Public Rust API for "talk to a running daemon over MQTT".
cli.rs         Thin glue: argv -> client::Client calls + tabled output.
```

## Tasks and channels

When the daemon is running, three long-lived tasks plus N short-lived
monitor tasks coexist:

```
  +-------------------+   inbound Publish      +----------------+
  | mqtt::run_eventloop|----------------------->| coordinator    |
  |  (rumqttc eventloop)|                       |   ::run        |
  +-------------------+                         |                |
                                                |                |
  +-------------------+   DeviceConnected       |                |
  | watcher::run_watcher|----------------------->|                |
  |  (inotify driver)  |                         |                |
  +-------------------+                         |                |
                                                |                |
  +-------------------+   KeyObserved /         |                |
  | monitor::run_device|  DeviceDisconnected    |                |
  | (one per enabled  |----------------------->|                |
  |  device)          |                         |                |
  +-------------------+                         +-------+--------+
        ^                                               |
        | spawn / shutdown oneshot                      |
        +-----------------------------------------------+
                                                         outbound:
                                                MqttHandle -> rumqttc client
                                                  + Database::save_atomic
```

Channels:

* `mpsc::UnboundedSender<CoordinatorMsg>` -- watcher and monitors send
  events to the coordinator. Coordinator stores a clone of the sender
  so it can pass one to each monitor it spawns.
* `mpsc::UnboundedReceiver<IncomingPublish>` -- the rumqttc eventloop
  forwards every inbound Publish here. The coordinator's main loop
  reads it via `tokio::select!` together with the coordinator command
  channel. `mqtt.rs` stays topic-agnostic; the coordinator parses
  `parse_enabled_topic`.
* `oneshot::Sender<()>` per spawned monitor -- coordinator uses this
  to ask a monitor to exit on disable / remove. Monitor distinguishes
  "shutdown by coordinator" (no DeviceDisconnected) from "device went
  away" (sends DeviceDisconnected).
* `oneshot::Sender<()>` for the watcher -- the daemon wakes it on
  shutdown.
* `Arc<AtomicBool> shutdown_flag` -- daemon and Client set this before
  DISCONNECT so the eventloop swallows the inevitable "Connection
  closed by peer abruptly" silently.

## Lifecycles

### Daemon startup

1. `daemon::run_with_shutdown` loads `Database` (missing file -> empty).
2. `mqtt::spawn` creates the rumqttc client, spawns the eventloop task,
   returns `MqttRuntime`.
3. `MqttRuntime::wait_ready(30 s)` blocks until the eventloop reports
   `ConnState::Connected` (CONNACK received). It returns an error early
   if the broker rejects the connect (`ConnectionRefused`, TLS handshake
   failure, protocol mismatch) so the daemon doesn't sit in an infinite
   retry loop on permanent errors; the 30 s budget is just generous
   enough that a systemd service can come up before the network is
   fully online without flapping. Once ready, publish retained
   `{prefix}/status = online`.
4. Subscribe to `{prefix}/_devices/+/enabled`. Sleep ~300 ms so the
   broker has time to push retained `enabled` messages before the
   watcher starts feeding `DeviceConnected` events. MQTT does not
   expose a "retained delivery done" signal (SubAck only confirms the
   subscription is registered), hence the flat sleep.
5. Spawn the coordinator task. Its very first action is
   `republish_known()` -- info JSON, enabled mirror, HA discovery for
   every device in the DB, so disabled-but-known devices reappear
   in HA after a daemon or broker restart.
6. Spawn the watcher with a shutdown oneshot. It sweeps `/dev/input`
   once (manual directory walk + `evdev::Device::open`, not
   `evdev::enumerate()` which misbehaves in containers), then runs the
   inotify loop. Every freshly-seen path goes through a 30 s open
   retry: udev's chown and (in HA addons) the cgroup device controller
   often haven't caught up by the time inotify fires, so the watcher
   retries every second for up to 30 s before giving up.
7. `info!("daemon ready")`. The daemon then `select!`s between the
   externally-provided shutdown future and a `wait_for_permanent_failure`
   that watches the same `ConnState` channel `wait_ready` used. A
   post-startup permanent MQTT failure exits the daemon instead of
   leaving it running in a dead-MQTT state.

### A new device arrives

1. Watcher opens `/dev/input/eventN`, builds a `DeviceIdentity`, filters
   on `identity.has_keys`, sends `DeviceConnected(identity)`.
2. Coordinator looks up the identity with `Database::match_or_insert`,
   which both finds the record (via `DeviceRecord::matches`) and
   backfills any previously-absent capability_fingerprint /
   capability_tag / unique_id / physical_path on a match. The match
   itself runs three checks in order: bus/vendor/product/version must
   be equal (hard requirement); if both sides carry a `unique_id`,
   they must agree; if the stored record has a
   `capability_fingerprint`, it must agree with the live one. Name is
   never consulted -- the kernel-supplied name is a label, not an
   identity.
3. If nothing matched, the record is inserted: the slug is allocated
   from the slugified name, with `-2`, `-3`, … appended on collision.
   When the live device's `capability_tag` is non-`"other"` it is
   folded into the slug seed (`logitech-receiver-mouse`) so a
   multi-collection USB receiver yields informative slugs.
4. New devices: publish info JSON + enabled=off mirror + HA discovery
   (with just the Enabled switch in `cmps`).
5. Record the identity in `connected: HashMap<slug, DeviceIdentity>`.
6. If `record.enabled` is true (carried over from a previous run),
   `spawn_monitor` opens a separate tokio task running
   `monitor::run_device`.

### A key press on an enabled device

1. `monitor::run_device` reads an `evdev::EventSummary::Key`, classifies
   value 0/1 as Press/Release (autorepeat ignored).
2. It publishes `{prefix}/{slug}/action = {kslug}_press|release` (not
   retained).
3. It sends `KeyObserved { slug, code }` to the coordinator.
4. Coordinator calls `DeviceRecord::record_observed_key` (no-op if
   already known); if newly inserted, persists DB, republishes info JSON
   and HA discovery (now with two extra `cmps` entries for press and
   release).

### Enable / disable from HA or CLI

1. HA writes `on` / `off` retained to `_devices/{slug}/enabled`.
2. The rumqttc eventloop forwards the message via `mqtt_incoming`.
3. `coordinator::handle_mqtt` parses the topic, normalises the payload,
   and dispatches `EnableCommand { slug, on }` through the same
   `handle()` switch as internal messages.
4. `on_enable` updates the DB and either calls `spawn_monitor` (if
   on and the device is connected) or asks the existing monitor to
   exit via its oneshot.

### Remove

1. Triggered by an empty retained publish on `_devices/{slug}/enabled`
   (which `--remove-device` writes, and HA writes if you delete the
   switch entity).
2. `on_remove` drops the DB record, aborts a live monitor if any, and
   publishes empty-retained to all three topics for that slug:
   `_devices/{slug}`, `_devices/{slug}/enabled`, and the HA discovery
   topic. HA forgets the device.

### Daemon shutdown

1. The outer `select!` resolves either on the injected shutdown future
   (SIGINT/SIGTERM in production) or on
   `wait_for_permanent_failure` -- an MQTT permanent failure observed
   after startup exits the process the same way.
2. Watcher oneshot is fired and awaited.
3. `drop(coord_tx)` closes the coordinator command channel; the
   coordinator's `select!` falls through to the `else` branch and
   exits its loop. On the way out it aborts every live monitor.
4. Publish retained `{prefix}/status = offline`, then sleep 200 ms so
   the broker has a chance to flush that publish before we hang up.
5. `mqtt::graceful_shutdown` flips the shutdown flag (so the
   eventloop swallows the inevitable "Connection closed by peer
   abruptly"), sends DISCONNECT, awaits the eventloop task with a
   500 ms timeout, aborts as fallback.

### CLI verbs

`Client::connect` runs the same `mqtt::spawn` setup (with a different
client id + a dummy LWT). `list_devices` subscribes to the two
wildcards, drains retained messages until 500 ms of silence (or 5 s
hard cap), parses the info JSON and the enabled mirror separately, and
returns `Vec<DeviceSnapshot>`. `enable_device` / `disable_device`
publish retained `on` / `off`; `remove_device` publishes empty
retained. `shutdown` runs `graceful_shutdown`.

## Cross-cutting concerns

* **Atomic DB writes**: temp file in the same directory, `sync_all`,
  `rename(2)`. On crash we see either the previous state or the new
  state, never partial. Cleanup of stale `.tmp.<pid>.<nonce>` files on
  rename failure.
* **Schema versioning**: on disk the database is a `Schema` enum,
  currently with a single `SchemaV1` variant. Adding `SchemaV2` is an
  additive change; old binaries fail closed on an unknown variant.
* **Slug stability**: a slug, once assigned, is permanent. Renaming
  the kernel device name does not re-slug. HA identifiers are derived
  from slug + topic prefix, so they survive renames too.
* **Identity matching**: each `DeviceRecord` stores every id we
  extracted from evdev (BVPV, unique_id, physical_path,
  capability_fingerprint, capability_tag). `DeviceRecord::matches`
  requires BVPV equality unconditionally, then disqualifies on a
  disagreeing unique_id or capability_fingerprint when both sides
  have one; an absent field on either side is treated as "no
  information," not a mismatch. This keeps the matcher implicit on
  disk (no "matcher choice" field) and lets a record persisted by an
  older binary -- before capability_fingerprint existed -- still
  match its live device on the first post-upgrade sighting.
  `match_or_insert` then backfills the previously-absent fields, so
  the record reaches a fully-populated steady state after one
  sighting.
* **Echo absorption**: the daemon receives back its own publishes on
  `_devices/+/enabled`. Handlers are idempotent (no-op when state
  already matches), so echoes don't loop.
* **Container friendliness**: enumeration walks `/dev/input` by hand
  with `read_dir` + `evdev::Device::open` rather than going through
  `evdev::enumerate()`, which depends on udev/sysfs paths that aren't
  populated in containers.

## Shape problems and improvements

After reading the modules in sequence, these are the layering /
responsibility issues that remain:

### 1. Database lookup is O(n) per call

`Database::find`, `find_mut`, `match_identity` all iterate the
`Vec<DeviceRecord>`. Fine for a handful of devices (everyone has 1-5
keyboards), but if we ever wanted to support a non-trivial fleet of
devices it would be worth keying by slug in a `HashMap` and keeping
a secondary index by `unique_id`. Not urgent.

### 2. `MqttRuntime` is consumed piecewise

`daemon::run_with_shutdown` immediately destructures
`MqttRuntime` into five locals because the eventloop receiver moves
into the coordinator while the JoinHandle, handle, flag, and
conn_state stay for shutdown / failure detection. The struct exists
mainly as a typed return. If we ever add another field this becomes
annoying; an alternative is to expose
`MqttRuntime::split() -> (MqttHandle, mpsc::Receiver, ShutdownHandle)`
where `ShutdownHandle` packs the JoinHandle + flag + conn_state +
a single `shutdown(self)` method. Not necessary today; just a sketch
for the next time mqtt.rs grows.

### 3. The 300 ms retained-drain sleep in startup

`daemon::run_with_shutdown` still sleeps 300 ms after subscribing to
`{prefix}/_devices/+/enabled` so retained messages on that wildcard
land before the watcher starts pushing `DeviceConnected` events.
rumqttc does expose `SubAck`, but SubAck only tells us the
subscription is registered, not that the retained replay is finished
-- there is no "retained done" signal in MQTT itself. The current
shape is a flat sleep; a more robust version would subscribe, then
read from `mqtt_incoming` until a short period of silence (the same
trick `Client::list_devices` uses). Defer until it actually causes
trouble.
