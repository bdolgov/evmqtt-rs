# evmqtt-rs

`evmqtt-rs` turns keyboard key presses into MQTT messages, because sometimes
a simple keyboard is the best remote control for your smart home.

It runs on any Linux device (Raspberry Pi, Home Assistant Green, TrueNAS, ...),
supports any input device recognized by Linux (wired keyboards, Bluetooth
keyboards, ...), integrates with Home Assistant over MQTT, and needs only 5 MiB
of disk and 20 MiB of RAM.

## Installation

Prerequisites: Linux machine with connected keyboard, [Rust toolchain], and an
MQTT broker.

[Rust toolchain]: https://rustup.rs

1. Compile and install the binary:

   TODO: Replace with `cargo binstall` or something like that.

   ```bash
   git clone https://github.com/bdolgov/evmqtt-rs.git
   cd evmqtt-rs
   cargo build --release
   sudo install -m 755 target/release/evmqtt-rs /usr/local/bin/evmqtt-rs
   ```

2. Configure MQTT:

   ```bash
   cat <<EOF | sudo tee /etc/evmqtt-rs.toml
   [mqtt]
   host = "192.168.1.10"
   port = 1883
   username = "mqtt_user"     # Or remove if the broker doesn't authenticate.
   password = "mqtt_password"

   EOF
   ```

3. Put all connected input devices into the config:

   ```bash
   sudo evmqtt-rs --detect |& sudo tee -a /etc/evmqtt-rs.toml
   ```

   If some input devices need to stay attached to the OS instead of MQTT,
   remove them from the config.

4. Configure `systemd` service:

   ```bash
   sudo install -m 644 evmqtt-rs.service /etc/systemd/system/evmqtt-rs.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now evmqtt-rs.service
   ```

   (Or write a similar config for your init system if you are not using
   systemd.)

5. Press some keys, observe messages in MQTT and devices in Home Assistant.
   If something is wrong, check the logs:

   ```bash
   sudo journalctl -fu evmqtt-rs.service
   ```

## Home Assistant Integration

`evmqtt-rs` uses Home Assistant [MQTT discovery] to automatically create one
Home Assistant MQTT device for every configured input device.

Every device gets two [MQTT Device Trigger]s:

* `{key}` pressed: when the key is pressed.
* `{key}` released: when the key is released.

These triggers can be referenced from Home Assistant automation rules.

[MQTT discovery]: https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery
[MQTT Device Trigger]: https://www.home-assistant.io/integrations/device_trigger.mqtt/

## MQTT Topic Structure

The topic structure resembles the topic structure that [Zigbee2MQTT] uses for
remote controllers.

* `<topic_prefix>/status`: `online` or `offline`, depending on whether `evmqtt-rs` is running.
* `<topic_prefix>/<mqtt_path>/action`: `<key>_press` or `<key>_release`.

Where:

* `<topic_prefix>` is the topic prefix from the configuration; defaults to `evmqtt` if unspecified.
* `<mqtt_path>` is the device-specific subtopic name from the configuration; defaults to the slugified name of the device if unspecified.
* `<key>` is the identifier of the pressed or released key.

[Zigbee2MQTT]: https://www.zigbee2mqtt.io/

## Configuration reference

### `[mqtt]`

| Field              | Type   | Default       | Notes                                                                                           |
| ------------------ | ------ | ------------- | ----------------------------------------------------------------------------------------------- |
| `host`             | string | **required**  | Broker hostname or IP                                                                           |
| `port`             | u16    | `1883`        |                                                                                                 |
| `username`         | string | none          | Omit (or both creds) for anonymous brokers                                                      |
| `password`         | string | none          |                                                                                                 |
| `topic_prefix`     | string | `"evmqtt"`    | Base of every published topic — `<topic_prefix>/<mqtt_path>/action` and `<topic_prefix>/status` |
| `client_id_prefix` | string | `"evmqtt-rs"` | The runtime appends `-<hostname>-<pid>` so every restart joins as a fresh client                |
| `keepalive_secs`   | u16    | `30`          | Clamped to a 5 s minimum                                                                        |

### `[hass]`

| Field              | Type   | Default           | Notes                                                                                       |
| ------------------ | ------ | ----------------- | ------------------------------------------------------------------------------------------- |
| `enabled`          | bool   | `true`            | When `false`, action events still publish but retained HA discovery payloads are suppressed |
| `discovery_prefix` | string | `"homeassistant"` | Must match HA's MQTT integration discovery prefix                                           |
| `name`             | string | `"evmqtt"`        | Prepended to each device's HA friendly name                                                 |

### `[[device]]`

One entry per physical input device. Repeatable.

| Field       | Type         | Default         | Notes                                                                   |
| ----------- | ------------ | --------------- | ----------------------------------------------------------------------- |
| `matcher`   | inline table | **required**    | Exactly one of the three variants below                                 |
| `name`      | string       | **required**    | Home Assistant friendly name; must be non-empty                         |
| `mqtt_path` | string       | `slugify(name)` | MQTT topic slug for this device; must be unique across all `[[device]]` |

The `matcher` variants, in order of how reliably they survive reboots:

* `matcher = { unique_id = "..." }` — exact match against the kernel's
  `EVIOCGUNIQ` value (same as `/sys/class/input/eventN/device/uniq`).
  Best when the device exposes one — most USB HID devices expose a
  serial number; Bluetooth devices expose a MAC address.
* `matcher = { bus_vendor_product_version = [b, v, p, ver] }` — four
  `u16`s. Hex literals (`[0x0003, 0x046d, 0xc52b, 0x0111]`) are the
  natural form. Equivalent to `id/bustype`, `id/vendor`, `id/product`,
  `id/version` from sysfs. Useful when the device has no `uniq`;
  matches any device of that model.
* `matcher = { name = "..." }` — exact, case-sensitive match against
  `EVIOCGNAME`. Use only when neither of the above is available.

## FAQ

### `--detect` finds nothing, or I get "permission denied" on `/dev/input/event*`

The process needs read access to `/dev/input/event*`. When detecting devices,
using `sudo` to get access is okay.

On most distros those files are mode `0660` owned by `root:input`, so the running user
has to be a member of the `input` group:

```bash
sudo usermod -a -G input "$USER"
# log out + back in
id -nG | grep -q input && echo "ok"
```

The provided systemd config puts `evmqtt-rs` into the `input` group automatically.

### Why one trigger per key instead of one sensor per device?

The original Python [`evmqtt`][evmqtt-original] exposes each input
device as a single `sensor` whose state is the last key code received
(`"KEY_VOLUMEUP"`, then `"KEY_VOLUMEDOWN"`, …). That model breaks down
in two ways: pressing the *same* key twice in a row doesn't change the
sensor state, so HA never re-triggers without a clearing step; and
modifier combinations have to be encoded as suffixed strings
(`KEY_A_KEY_LEFTSHIFT_KEY_LEFTCTRL`) that are painful to match.

Per-key `device_trigger`s sidestep both: each key press is an *event*,
not a state change, so the same key fires every time; and modifiers are
independent triggers you compose in automations.

### Two devices match the same `[[device]]` — what happens?

First-come wins. Whichever physical device the watcher sees first gets
that slot; later matches log `device is already attached, ignoring
duplicate match` at debug level. If you have two identical devices,
prefer `unique_id` or `bus_vendor_product_version` — `name` matchers
can't tell them apart.

### What happens when a device disconnects?

The monitor task notices the read error, logs at `info` level
(`device disconnected; monitor exiting`), and frees its `[[device]]`
slot. When the device reappears — *even at a different
`/dev/input/eventN`* — the watcher attaches again and resumes
publishing to the same MQTT topic, so HA never sees a topology change.

### Will this hijack my console / X / Wayland keyboard?

It calls `EVIOCGRAB` on each device it monitors — yes, that steals
input from the console. That's the point: for a media remote you want
events to flow to MQTT, not show up as stray characters on tty1. But
*do not* point `evmqtt-rs` at your laptop's primary keyboard.

### What about key autorepeat / held keys?

Autorepeat (`evdev value=2`) is ignored on purpose — HA device triggers
are momentary, not stateful. You'll get one `…_press` when the key
goes down and one `…_release` when it comes up, regardless of how long
it was held.

### How do I increase the logging level?

Set the `EVMQTT_LOG` environment variable to `debug` or `trace`.

`EVMQTT_LOG` accepts any [`tracing` `EnvFilter`][envfilter] directive,
so `EVMQTT_LOG=evmqtt_rs::watcher=trace,info` works for narrowing.

[envfilter]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html

### Triggers don't show up in HA

They're published lazily, the first time you press the corresponding
key. If you want them pre-populated, press each key once with HA's
MQTT integration listening — the discovery message is retained, so HA
will keep it across restarts.

## Acknowledgments

* Concept and Python original by [the `evmqtt` authors][evmqtt-original]
  and James Bulpin's [original gist].
* Built on [`evdev`](https://crates.io/crates/evdev),
  [`inotify`](https://crates.io/crates/inotify),
  [`rumqttc`](https://crates.io/crates/rumqttc),
  and [`tokio`](https://tokio.rs/).

[evmqtt-original]: https://github.com/odtgit/evmqtt
[original gist]: https://gist.github.com/jamesbulpin/b940e7d81e2e65158f12e59b4d6a0c3c
