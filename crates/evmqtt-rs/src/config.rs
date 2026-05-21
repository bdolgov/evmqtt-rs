use clap::Parser;
use std::path::PathBuf;

/// Top-level CLI / env-var surface.
///
/// Every connection parameter has a matching `EVMQTT_*` env var so the
/// binary can be driven entirely from a systemd `EnvironmentFile=` (or
/// `docker run -e ...`) without a config file on disk.
#[derive(Debug, Parser)]
#[command(
    name = "evmqtt-rs",
    version,
    about = "Turns keyboard key presses into MQTT messages",
    long_about = None,
)]
pub struct Args {
    // ── MQTT ───────────────────────────────────────────────────────────
    /// MQTT broker hostname or IP.
    #[arg(long, env = "EVMQTT_MQTT_HOST")]
    pub mqtt_host: String,

    /// MQTT broker port.
    #[arg(long, env = "EVMQTT_MQTT_PORT", default_value_t = 1883)]
    pub mqtt_port: u16,

    /// MQTT username (omit for anonymous brokers).
    #[arg(long, env = "EVMQTT_MQTT_USERNAME")]
    pub mqtt_username: Option<String>,

    /// MQTT password.
    #[arg(long, env = "EVMQTT_MQTT_PASSWORD")]
    pub mqtt_password: Option<String>,

    /// Topic prefix for every published topic.
    #[arg(long, env = "EVMQTT_MQTT_TOPIC_PREFIX", default_value = "evmqtt")]
    pub mqtt_topic_prefix: String,

    /// Client-id prefix; the runtime appends `-<host>-<pid>`.
    #[arg(
        long,
        env = "EVMQTT_MQTT_CLIENT_ID_PREFIX",
        default_value = "evmqtt-rs"
    )]
    pub mqtt_client_id_prefix: String,

    /// MQTT keepalive in seconds (clamped to a minimum of 5).
    #[arg(long, env = "EVMQTT_MQTT_KEEPALIVE_SECS", default_value_t = 30)]
    pub mqtt_keepalive_secs: u16,

    // ── Home Assistant ─────────────────────────────────────────────────
    /// Publish HA discovery payloads when true.
    #[arg(
        long,
        env = "EVMQTT_HASS_ENABLED",
        default_value_t = true,
        action = clap::ArgAction::Set,
    )]
    pub hass_enabled: bool,

    /// HA MQTT discovery prefix.
    #[arg(
        long,
        env = "EVMQTT_HASS_DISCOVERY_PREFIX",
        default_value = "homeassistant"
    )]
    pub hass_discovery_prefix: String,

    /// HA friendly-name prefix; the device name follows. When unset,
    /// defaults to `mqtt-topic-prefix` so changing the topic prefix
    /// also reflects in HA's UI without a second flag flip.
    #[arg(long, env = "EVMQTT_HASS_NAME")]
    pub hass_name: Option<String>,

    // ── Local state ────────────────────────────────────────────────────
    /// Path to the device database (TOML, atomically rewritten).
    #[arg(long, env = "EVMQTT_DB", default_value = "/var/lib/evmqtt-rs/db.toml")]
    pub db: PathBuf,

    // ── Mode ───────────────────────────────────────────────────────────
    /// Run the daemon: watch `/dev/input/event*` and bridge to MQTT.
    #[arg(long, conflicts_with_all = ["list_devices", "enable_device", "disable_device", "remove_device"])]
    pub daemon: bool,

    /// List devices currently known to the running daemon (snapshot
    /// from retained `_devices/+` topics).
    #[arg(long, conflicts_with_all = ["daemon", "enable_device", "disable_device", "remove_device"])]
    pub list_devices: bool,

    /// Enable a device by slug. Repeat for multiple devices.
    #[arg(long, value_name = "SLUG", conflicts_with_all = ["daemon", "list_devices"])]
    pub enable_device: Vec<String>,

    /// Disable a device by slug. Repeat for multiple devices.
    #[arg(long, value_name = "SLUG", conflicts_with_all = ["daemon", "list_devices"])]
    pub disable_device: Vec<String>,

    /// Remove a device: drops the entry, clears retained MQTT topics
    /// and HA discovery. Repeat for multiple devices.
    #[arg(long, value_name = "SLUG", conflicts_with_all = ["daemon", "list_devices"])]
    pub remove_device: Vec<String>,
}

/// What the user asked the binary to do.
#[derive(Debug, Clone)]
pub enum Mode {
    Daemon,
    ListDevices,
    /// Slugs to enable, then slugs to disable, then slugs to remove.
    /// Any combination is allowed in one invocation.
    Manage {
        enable: Vec<String>,
        disable: Vec<String>,
        remove: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Base of every published topic — `<topic_prefix>/<slug>/action`
    /// for events and `<topic_prefix>/status` for the LWT availability
    /// topic.
    pub topic_prefix: String,
    pub client_id_prefix: String,
    pub keepalive_secs: u16,
}

#[derive(Debug, Clone)]
pub struct HassConfig {
    /// When `false`, action events still publish but retained discovery
    /// payloads are suppressed.
    pub enabled: bool,
    pub discovery_prefix: String,
    /// Prefixed to each device's HA friendly name.
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Runtime {
    pub mqtt: MqttConfig,
    pub hass: HassConfig,
    pub db_path: PathBuf,
    pub mode: Mode,
}

impl Args {
    /// Pull the raw args apart into the daemon's runtime view.
    ///
    /// Returns `None` for `mode` when the user invoked the binary with
    /// no subcommand and no `--daemon` flag — the caller should print
    /// help in that case.
    pub fn into_runtime(self) -> Result<Runtime, &'static str> {
        let mode = if self.daemon {
            Mode::Daemon
        } else if self.list_devices {
            Mode::ListDevices
        } else if !self.enable_device.is_empty()
            || !self.disable_device.is_empty()
            || !self.remove_device.is_empty()
        {
            Mode::Manage {
                enable: self.enable_device,
                disable: self.disable_device,
                remove: self.remove_device,
            }
        } else {
            return Err("no mode selected: pass --daemon, --list-devices, \
                 --enable-device, --disable-device, or --remove-device");
        };

        if self.mqtt_host.trim().is_empty() {
            return Err("--mqtt-host / EVMQTT_MQTT_HOST must not be empty");
        }
        if self.mqtt_port == 0 {
            return Err("--mqtt-port must be non-zero");
        }
        if self.mqtt_topic_prefix.trim().is_empty() {
            return Err("--mqtt-topic-prefix must not be empty");
        }
        if self.hass_discovery_prefix.trim().is_empty() {
            return Err("--hass-discovery-prefix must not be empty");
        }

        let hass_name = self
            .hass_name
            .unwrap_or_else(|| self.mqtt_topic_prefix.clone());
        Ok(Runtime {
            mqtt: MqttConfig {
                host: self.mqtt_host,
                port: self.mqtt_port,
                username: self.mqtt_username,
                password: self.mqtt_password,
                topic_prefix: self.mqtt_topic_prefix,
                client_id_prefix: self.mqtt_client_id_prefix,
                keepalive_secs: self.mqtt_keepalive_secs,
            },
            hass: HassConfig {
                enabled: self.hass_enabled,
                discovery_prefix: self.hass_discovery_prefix,
                name: hass_name,
            },
            db_path: self.db,
            mode,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn clap_definition_is_well_formed() {
        // Catches conflict_with mis-typings and other compile-time-ish errors.
        Args::command().debug_assert();
    }

    fn parse(args: &[&str]) -> Args {
        Args::try_parse_from(std::iter::once(&"evmqtt-rs").chain(args.iter())).expect("parse")
    }

    #[test]
    fn parses_minimal_daemon_invocation() {
        let args = parse(&["--daemon", "--mqtt-host", "broker"]);
        let rt = args.into_runtime().expect("runtime");
        assert!(matches!(rt.mode, Mode::Daemon));
        assert_eq!(rt.mqtt.host, "broker");
        assert_eq!(rt.mqtt.port, 1883);
        assert_eq!(rt.mqtt.topic_prefix, "evmqtt");
        assert_eq!(rt.hass.discovery_prefix, "homeassistant");
        assert!(rt.hass.enabled);
    }

    #[test]
    fn parses_list_devices() {
        let args = parse(&["--list-devices", "--mqtt-host", "broker"]);
        let rt = args.into_runtime().expect("runtime");
        assert!(matches!(rt.mode, Mode::ListDevices));
    }

    #[test]
    fn parses_management_verbs() {
        let args = parse(&[
            "--mqtt-host",
            "broker",
            "--enable-device",
            "a",
            "--enable-device",
            "b",
            "--disable-device",
            "c",
            "--remove-device",
            "d",
        ]);
        let rt = args.into_runtime().expect("runtime");
        match rt.mode {
            Mode::Manage {
                enable,
                disable,
                remove,
            } => {
                assert_eq!(enable, vec!["a", "b"]);
                assert_eq!(disable, vec!["c"]);
                assert_eq!(remove, vec!["d"]);
            }
            other => panic!("expected Manage, got {other:?}"),
        }
    }

    #[test]
    fn rejects_no_mode() {
        let args = parse(&["--mqtt-host", "broker"]);
        assert!(args.into_runtime().is_err());
    }

    #[test]
    fn rejects_conflicting_modes() {
        let res = Args::try_parse_from([
            "evmqtt-rs",
            "--daemon",
            "--list-devices",
            "--mqtt-host",
            "b",
        ]);
        assert!(res.is_err(), "--daemon and --list-devices must conflict");
    }

    #[test]
    fn rejects_empty_host() {
        let args = parse(&["--daemon", "--mqtt-host", "   "]);
        assert!(args.into_runtime().is_err());
    }
}
