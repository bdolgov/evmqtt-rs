use evdev::{Device, EventType};
use std::path::{Path, PathBuf};
use tracing::warn;

/// Everything we know about an open input device. All fields come from
/// evdev ioctls (EVIOCGNAME / EVIOCGUNIQ / EVIOCGID / EVIOCGPHYS) — the same
/// kernel sources that populate `/sys/class/input/eventN/device/{name,uniq,id/*,phys}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceIdentity {
    pub path: PathBuf,
    pub name: String,
    pub unique_id: Option<String>,
    pub bus: u16,
    pub vendor: u16,
    pub product: u16,
    pub version: u16,
    pub physical_path: Option<String>,
    pub has_keys: bool,
}

impl DeviceIdentity {
    pub fn from_open_device(path: PathBuf, device: &Device) -> Self {
        let name = device
            .name()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let unique_id = device
            .unique_name()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());
        let id = device.input_id();
        let physical_path = device
            .physical_path()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty());
        let has_keys = device
            .supported_events()
            .iter()
            .any(|t| t == EventType::KEY);
        Self {
            path,
            name,
            unique_id,
            bus: id.bus_type().0,
            vendor: id.vendor(),
            product: id.product(),
            version: id.version(),
            physical_path,
            has_keys,
        }
    }
}

/// Open a specific `/dev/input/event*` path and build a `DeviceIdentity`.
pub fn open_identity(path: &Path) -> Option<DeviceIdentity> {
    match Device::open(path) {
        Ok(d) => Some(DeviceIdentity::from_open_device(path.to_path_buf(), &d)),
        Err(e) => {
            warn!(path = %path.display(), error = %e, "could not open device");
            None
        }
    }
}

/// Enumerate every visible `/dev/input/event*` device, in path order.
///
/// We walk the directory by hand and open each `eventN` node directly
/// rather than calling `evdev::enumerate()` -- the latter goes through
/// helper crates whose udev/sysfs paths can return nothing inside
/// containers even when `/dev/input/eventN` is bind-mounted and
/// perfectly readable. A plain `read_dir` of `/dev/input` is the most
/// reliable thing that works equally well on bare metal and in Docker.
pub fn enumerate_identities() -> Vec<DeviceIdentity> {
    let entries = match std::fs::read_dir(INPUT_DIR) {
        Ok(e) => e,
        Err(e) => {
            warn!(dir = INPUT_DIR, error = %e, "could not read input directory");
            return Vec::new();
        }
    };
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        if is_event_node(s) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    paths
        .into_iter()
        .filter_map(|p| open_identity(&p))
        .collect()
}

const INPUT_DIR: &str = "/dev/input";

/// True iff `name` looks like an `eventN` node where N is one or more
/// ASCII digits. Public so the inotify watcher in `watcher.rs` can
/// share the same definition.
pub fn is_event_node(name: &str) -> bool {
    name.strip_prefix("event")
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_event_node_accepts_event_digits() {
        assert!(is_event_node("event0"));
        assert!(is_event_node("event42"));
    }

    #[test]
    fn is_event_node_rejects_others() {
        assert!(!is_event_node("event"));
        assert!(!is_event_node("eventX"));
        assert!(!is_event_node("mice"));
        assert!(!is_event_node("js0"));
        assert!(!is_event_node("by-id"));
    }
}
