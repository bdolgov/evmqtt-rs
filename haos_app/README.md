# evmqtt-rs

`evmqtt-rs` turns keyboard key presses into MQTT messages, because sometimes
a simple keyboard is the best remote control for your smart home.

## Highlights

* No static device list — every connected device with buttons is auto-detected
  and announced to Home Assistant on first sighting.
* Each detected device shows up in HA with an **Enabled** switch. Flip it on
  and `evmqtt-rs` starts grabbing events from that device; flip it off and
  the device returns to the kernel. State survives restarts. and disconnects.
* Triggers are added to HA per-key, lazily — the first time you press a key
  on an enabled device, that key's `*_press` / `*_release` triggers appear
  in HA, ready to use in automations.

## Home Assistant Integration

Every detected device gets:

* An **Enabled** switch component, wired to the daemon's enable/disable
  control topic. Flipping the switch in HA enables or disables monitoring
  for that device.
* One [MQTT Device Trigger] pair (`*_press` and `*_release`) for every key
  the daemon has ever observed on that device. The discovery payload is
  re-published whenever a new key arrives.

Triggers only appear after a key has been pressed at least once with the
device enabled. If you want them pre-populated, press each key once with
HA's MQTT integration listening — the discovery message is retained.

[MQTT Device Trigger]: https://www.home-assistant.io/integrations/device_trigger.mqtt/
