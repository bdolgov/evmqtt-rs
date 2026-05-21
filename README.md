# evmqtt-rs

`evmqtt-rs` turns keyboard key presses into MQTT messages, because sometimes
a simple keyboard is the best remote control for your smart home.

It runs on any Linux device (Raspberry Pi, Home Assistant Green, TrueNAS, ...),
supports any input device recognized by Linux (wired keyboards, Bluetooth
keyboards, ...), integrates with Home Assistant over MQTT, and needs only 5 MiB
of disk and 20 MiB of RAM.

## Highlights

* No static device list — every `/dev/input/event*` keyboard is auto-detected
  and announced to Home Assistant on first sighting.
* Each detected device shows up in HA with an **Enabled** switch. Flip it on
  and `evmqtt-rs` starts grabbing events from that device; flip it off and
  the device returns to the kernel. State survives restarts.
* Triggers are added to HA per-key, lazily — the first time you press a key
  on an enabled device, that key's `*_press` / `*_release` triggers appear
  in HA, ready to use in automations.

## Installation

Prerequisites: Linux machine with connected keyboard, [Rust toolchain], and an
MQTT broker.

[Rust toolchain]: https://rustup.rs

1. Build and install the binary:

   ```bash
   git clone https://github.com/bdolgov/evmqtt-rs.git
   cd evmqtt-rs
   cargo build --release
   sudo install -m 755 target/release/evmqtt-rs /usr/local/bin/evmqtt-rs
   ```

2. Write the environment file with your MQTT credentials. The same file is
   used by the systemd service **and** sourced from the shell when you run
   management commands by hand.

   ```bash
   sudo install -m 600 /dev/stdin /etc/evmqtt-rs.env <<'EOF'
   EVMQTT_MQTT_HOST=192.168.1.10
   EVMQTT_MQTT_PORT=1883
   EVMQTT_MQTT_USERNAME=mqtt_user
   EVMQTT_MQTT_PASSWORD=mqtt_password
   # Optional overrides:
   # EVMQTT_MQTT_TOPIC_PREFIX=evmqtt
   # EVMQTT_HASS_DISCOVERY_PREFIX=homeassistant
   # EVMQTT_LOG=info
   EOF
   ```

3. Install and start the systemd service:

   ```bash
   sudo install -m 644 evmqtt-rs.service /etc/systemd/system/evmqtt-rs.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now evmqtt-rs.service
   ```

   (Or write a similar unit for your init system if you are not using
   systemd. The daemon needs `--daemon`, an `input`-group identity for
   `/dev/input/event*` access, and a writable state directory for
   `db.toml` — pointed at by `EVMQTT_DB`.)

4. Enable a device. Either flip its "Enabled" switch in Home Assistant, or
   run the CLI by hand:

   ```bash
   # /etc/evmqtt-rs.env is root-owned, so read it through sudo and let
   # `set -a` export every variable defined inside.
   set -a && eval "$(sudo cat /etc/evmqtt-rs.env)" && set +a

   evmqtt-rs --list-devices
   ╭──────────────────────────────┬─────────┬──────────────────────────────┬───────────┬────────┬────────┬─────────┬─────────╮
   │ slug                         │ enabled │ name                         │ unique_id │ bus    │ vendor │ product │ version │
   ├──────────────────────────────┼─────────┼──────────────────────────────┼───────────┼────────┼────────┼─────────┼─────────┤
   │ at-translated-set-2-keyboard │ off     │ AT Translated Set 2 keyboard │ -         │ 0x0011 │ 0x0001 │ 0x0001  │ 0xab00  │
   ╰──────────────────────────────┴─────────┴──────────────────────────────┴───────────┴────────┴────────┴─────────┴─────────╯
   evmqtt-rs --enable-device at-translated-set-2-keyboard
   ```

5. Press some keys, watch triggers appear in MQTT and Home Assistant. If
   something is wrong:

   ```bash
   sudo journalctl -fu evmqtt-rs.service
   ```

## Home Assistant Integration

`evmqtt-rs` uses Home Assistant [device-based MQTT discovery]: one retained
config message per device, at `<discovery_prefix>/device/<id>/config`. Every
detected device gets:

* An **Enabled** switch component, wired to the daemon's enable/disable
  control topic. Flipping the switch in HA enables or disables monitoring
  for that device.
* One [MQTT Device Trigger] pair (`*_press` and `*_release`) for every key
  the daemon has ever observed on that device. The discovery payload is
  re-published whenever a new key arrives.

Triggers only appear after a key has been pressed at least once with the
device enabled. If you want them pre-populated, press each key once with
HA's MQTT integration listening — the discovery message is retained.

[device-based MQTT discovery]: https://www.home-assistant.io/integrations/mqtt/#device-based-discovery
[MQTT Device Trigger]: https://www.home-assistant.io/integrations/device_trigger.mqtt/

## MQTT Topic Structure

| Topic                                            | Direction        | Notes                                                |
| ------------------------------------------------ | -----------------| ---------------------------------------------------- |
| `<topic_prefix>/status`                          | evmqtt → broker  | `online` / `offline` (LWT)                           |
| `<topic_prefix>/_devices/<slug>`                 | evmqtt → broker  | Retained JSON describing the device                  |
| `<topic_prefix>/_devices/<slug>/enabled`         | both directions  | Retained `on` / `off`. Write to control the daemon   |
| `<topic_prefix>/<slug>/action`                   | evmqtt → broker  | `<key>_press` / `<key>_release` events               |
| `<discovery_prefix>/device/<id>/config`          | evmqtt → broker  | HA device-based discovery payload                    |

`<slug>` is the daemon-assigned id (slugified name with `-2`, `-3`, … on
collision); `<id>` is `<topic_prefix>_<slug>`.

Writing an empty retained message to `<topic_prefix>/_devices/<slug>/enabled`
is interpreted by the running daemon as a remove command (see
`--remove-device` below) — it drops the device from the database and clears
the retained topics.

## Configuration

All settings are flags. Each flag has a matching `EVMQTT_*` environment
variable so the daemon can be driven entirely from a systemd
`EnvironmentFile=` (or `docker run -e ...`) without a config file.

### MQTT (`EVMQTT_MQTT_*`)

| Flag                        | Env                              | Default       |
| --------------------------- | -------------------------------- | ------------- |
| `--mqtt-host`               | `EVMQTT_MQTT_HOST`               | **required**  |
| `--mqtt-port`               | `EVMQTT_MQTT_PORT`               | `1883`        |
| `--mqtt-username`           | `EVMQTT_MQTT_USERNAME`           | none          |
| `--mqtt-password`           | `EVMQTT_MQTT_PASSWORD`           | none          |
| `--mqtt-topic-prefix`       | `EVMQTT_MQTT_TOPIC_PREFIX`       | `evmqtt`      |
| `--mqtt-client-id-prefix`   | `EVMQTT_MQTT_CLIENT_ID_PREFIX`   | `evmqtt-rs`   |
| `--mqtt-keepalive-secs`     | `EVMQTT_MQTT_KEEPALIVE_SECS`     | `30`          |

### Home Assistant (`EVMQTT_HASS_*`)

| Flag                        | Env                              | Default          |
| --------------------------- | -------------------------------- | ---------------- |
| `--hass-enabled`            | `EVMQTT_HASS_ENABLED`            | `true`           |
| `--hass-discovery-prefix`   | `EVMQTT_HASS_DISCOVERY_PREFIX`   | `homeassistant`  |
| `--hass-name`               | `EVMQTT_HASS_NAME`               | same as `--mqtt-topic-prefix` |

### Local state

| Flag           | Env             | Default                          |
| -------------- | --------------- | -------------------------------- |
| `--db PATH`    | `EVMQTT_DB`     | `/var/lib/evmqtt-rs/db.toml`     |

### Modes

| Flag                        | What it does                                                                  |
| --------------------------- | ----------------------------------------------------------------------------- |
| `--daemon`                  | Run the watcher and MQTT bridge. Mutually exclusive with the others.          |
| `--list-devices`            | Connect, dump the retained device snapshot, exit.                             |
| `--enable-device SLUG`      | (repeatable) Tell the daemon to enable `SLUG`. Persists across restarts.      |
| `--disable-device SLUG`     | (repeatable) Disable `SLUG`.                                                  |
| `--remove-device SLUG`      | (repeatable) Drop `SLUG` from the database and clear its retained MQTT state. |

## FAQ

### Where does state live?

In a TOML file at `/var/lib/evmqtt-rs/db.toml` by default
(`--db` / `EVMQTT_DB` to override). The daemon writes it atomically
(temp file in the same directory + `rename(2)`), so power loss
leaves the previous or the new content but never a partial file.
Observed keys are stored as a `Vec<u16>` of evdev codes to keep the
file small.

### How do I get a device to show up in Home Assistant?

Plug it in. The daemon detects every `/dev/input/event*` with key
capability, allocates a slug from the device name, publishes the
device info + an "Enabled" switch via HA discovery, and adds an
entry to `db.toml`. Until you enable the switch the daemon doesn't
touch the device; the console keeps it.

### Why do triggers only appear after I press a key?

By design — that's the only way `evmqtt-rs` knows which keys the
device emits. HA device triggers are momentary events keyed on
`(device, key)` pairs, so we can't announce a useful trigger until
we've seen the key. Once observed, the trigger is retained and HA
keeps it across restarts.

### Two devices have the same name. Which one wins?

The first one to appear gets the bare slug (`usb-keyboard`); the
second gets `usb-keyboard-2`, the third `usb-keyboard-3`, and so
on. Slugs are permanent: once assigned they don't change even if
the device is renamed by the kernel later. The daemon's matching
is based on the most precise identifier available, in order:
`unique_id` (USB serial / Bluetooth MAC), bus/vendor/product/version
quad, then exact name.

### `/dev/input/event*` is "permission denied"

The running user needs to be in the `input` group. The bundled
systemd unit handles this automatically via `SupplementaryGroups=input`.
For interactive use:

```bash
sudo usermod -a -G input "$USER"
# log out + back in
id -nG | grep -q input && echo ok
```

### Will this hijack my console / X / Wayland keyboard?

Only for devices whose Enabled switch is on. When enabled the
daemon calls `EVIOCGRAB` and pulls events out of the kernel input
layer; when disabled it leaves the device alone. *Don't* enable
the switch on your laptop's primary keyboard.

### What about key autorepeat / held keys?

Autorepeat (`evdev value=2`) is ignored on purpose — HA device
triggers are momentary, not stateful. You get one `…_press` when
the key goes down and one `…_release` when it comes up, regardless
of how long it was held.

### How do I increase the logging level?

Set the `EVMQTT_LOG` environment variable to `debug` or `trace`.
Any [`tracing` `EnvFilter`][envfilter] directive works.

[envfilter]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html

## Comparison with alternatives

### vs Home Assistant's [`keyboard_remote`] integration

* `keyboard_remote` is effectively unmaintained; `evmqtt-rs` is
  actively developed and accepting fixes.
* `keyboard_remote` is YAML-only. `evmqtt-rs` exposes each device
  in the HA UI as an MQTT device with an Enabled switch; no
  `configuration.yaml` editing required.
* `evmqtt-rs` triggers carry readable names
  (`volumeup_press`, `volumedown_release`, ...) instead of raw
  numeric key codes.
* `keyboard_remote` only sees keyboards plugged into the host that
  is running Home Assistant. `evmqtt-rs` runs anywhere on the
  network and talks to HA over MQTT, so the keyboard can live on a
  Raspberry Pi Zero in another room, a NUC by the TV, etc.

[`keyboard_remote`]: https://www.home-assistant.io/integrations/keyboard_remote/

### vs the original Python [`evmqtt`][evmqtt-original]

* The original models each device as a single HA `sensor` whose
  state is the last key code seen, so pressing the same key twice
  in a row never changes the state and HA does not refire. With
  `evmqtt-rs`, every press/release is an independent device
  trigger, fired every time.
* Hotplug: `evmqtt-rs` watches `/dev/input` with inotify and
  re-attaches automatically when a device disappears and comes
  back. This matters for Bluetooth keyboards in particular -- they
  routinely drop off and reappear under a new `eventN`, and the
  original `evmqtt` requires a process restart in that case.
* Lower footprint: `evmqtt-rs` ships as a single ~5 MiB static
  binary and uses around 20 MiB of RAM at runtime, vs a CPython
  install plus the `evdev` and `paho-mqtt` modules.

## Acknowledgments

* Concept and Python original by [the `evmqtt` authors][evmqtt-original]
  and James Bulpin's [original gist].
* Built on [`evdev`](https://crates.io/crates/evdev),
  [`inotify`](https://crates.io/crates/inotify),
  [`rumqttc`](https://crates.io/crates/rumqttc),
  and [`tokio`](https://tokio.rs/).

[evmqtt-original]: https://github.com/odtgit/evmqtt
[original gist]: https://gist.github.com/jamesbulpin/b940e7d81e2e65158f12e59b4d6a0c3c
