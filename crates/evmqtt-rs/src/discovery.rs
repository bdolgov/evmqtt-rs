use evdev::{AttributeSetRef, Device, EventType, KeyCode, RelativeAxisCode};
use std::path::{Path, PathBuf};
use std::{fs, io};
use tracing::warn;

/// Everything we know about an open input device. The first batch of
/// fields comes from evdev ioctls (EVIOCGNAME / EVIOCGUNIQ / EVIOCGID /
/// EVIOCGPHYS) — the same kernel sources that populate
/// `/sys/class/input/eventN/device/{name,uniq,id/*,phys}`. The
/// capability fields are computed from the device's supported event
/// types / codes and are how we tell apart the multiple `eventN` nodes
/// of a single USB receiver that share name + BVPV + phys.
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
    /// Stable hex hash of the device's exposed capabilities (event
    /// types + key codes + REL/ABS axes). The same physical interface
    /// — even moved between USB ports — produces the same value;
    /// different sub-devices of a multi-collection HID receiver produce
    /// different values.
    pub capability_fingerprint: String,
    /// Short human tag derived from capabilities (`kbd`, `numpad`,
    /// `mouse`, `other`) used to disambiguate display names in HA when
    /// a single USB receiver registers several otherwise-identically-
    /// named records. Always set: devices that don't match any of the
    /// known shapes fall through to `"other"`.
    pub capability_tag: &'static str,
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
            capability_fingerprint: capability_fingerprint(device),
            capability_tag: capability_tag(device),
        }
    }
}

/// Open a specific `/dev/input/event*` path and build a `DeviceIdentity`.
/// Returns the underlying `io::Error` on failure so callers can decide
/// whether to retry, warn, or stay quiet.
pub fn try_open_identity(path: &Path) -> io::Result<DeviceIdentity> {
    Device::open(path).map(|d| DeviceIdentity::from_open_device(path.to_path_buf(), &d))
}

/// List every visible `/dev/input/event*` node, in path order.
///
/// We walk the directory by hand rather than calling `evdev::enumerate()` --
/// the latter goes through helper crates whose udev/sysfs paths can return
/// nothing inside containers even when `/dev/input/eventN` is bind-mounted
/// and perfectly readable. A plain `read_dir` of `/dev/input` is the most
/// reliable thing that works equally well on bare metal and in Docker.
pub fn list_event_paths() -> Vec<PathBuf> {
    let entries = match fs::read_dir(INPUT_DIR) {
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
}

const INPUT_DIR: &str = "/dev/input";

/// True iff `name` looks like an `eventN` node where N is one or more
/// ASCII digits. Public so the inotify watcher in `watcher.rs` can
/// share the same definition.
pub fn is_event_node(name: &str) -> bool {
    name.strip_prefix("event")
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit()))
}

/// Compute a stable hex fingerprint of `device`'s capabilities.
///
/// Folds (sorted) supported event types, key codes, REL axes, and ABS
/// axes into a single 64-bit FNV-1a hash. FNV-1a is used (rather than
/// `std::hash::DefaultHasher`) because its algorithm is fully
/// specified, so the digest stays stable across Rust releases and
/// platforms — the value lands in the on-disk DB.
pub fn capability_fingerprint(device: &Device) -> String {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce4_84222325;
    const FNV_PRIME: u64 = 0x00000100_000001b3;

    fn mix(hash: &mut u64, v: u16) {
        for b in v.to_le_bytes() {
            *hash ^= b as u64;
            *hash = hash.wrapping_mul(FNV_PRIME);
        }
    }

    let mut hash: u64 = FNV_OFFSET_BASIS;

    // Section markers keep two sections with similar bit patterns from
    // colliding (e.g. KEY_A and REL_X both have small numeric codes).
    mix(&mut hash, 0xE001);
    let mut types: Vec<u16> = device.supported_events().iter().map(|t| t.0).collect();
    types.sort_unstable();
    for t in types {
        mix(&mut hash, t);
    }

    mix(&mut hash, 0xE002);
    if let Some(keys) = device.supported_keys() {
        let mut codes: Vec<u16> = keys.iter().map(|k| k.0).collect();
        codes.sort_unstable();
        for c in codes {
            mix(&mut hash, c);
        }
    }

    mix(&mut hash, 0xE003);
    if let Some(rels) = device.supported_relative_axes() {
        let mut codes: Vec<u16> = rels.iter().map(|r| r.0).collect();
        codes.sort_unstable();
        for c in codes {
            mix(&mut hash, c);
        }
    }

    mix(&mut hash, 0xE004);
    if let Some(abss) = device.supported_absolute_axes() {
        let mut codes: Vec<u16> = abss.iter().map(|a| a.0).collect();
        codes.sort_unstable();
        for c in codes {
            mix(&mut hash, c);
        }
    }

    format!("{hash:016x}")
}

/// Best-effort classification of a device into a short, human-readable
/// tag. Used only to differentiate display names; matching is by
/// fingerprint, not tag (two devices that classify to the same tag
/// will still have distinct fingerprints).
///
/// The order is deliberate. A typing keyboard with extra media /
/// power keys is still a keyboard, so `KEY_A` wins first. A device
/// with only `KEY_KP*` (no letters) is a numpad. After that, REL_X/Y
/// is the canonical mouse signature. Everything else (consumer-
/// control collections, system-control collections, joysticks with
/// no REL, ...) falls through to `"other"`; distinctness between
/// them is preserved by the capability fingerprint, not by the tag.
pub fn capability_tag(device: &Device) -> &'static str {
    classify_capabilities(device.supported_keys(), device.supported_relative_axes())
}

/// Inner classifier — same logic as [`capability_tag`] but takes the
/// capability sets directly so unit tests can drive it without a real
/// evdev `Device`.
fn classify_capabilities(
    keys: Option<&AttributeSetRef<KeyCode>>,
    rels: Option<&AttributeSetRef<RelativeAxisCode>>,
) -> &'static str {
    if let Some(keys) = keys {
        if keys.contains(KeyCode::KEY_A) {
            return "kbd";
        }
        if keys.contains(KeyCode::KEY_KP0) {
            return "numpad";
        }
    }
    if let Some(rels) = rels
        && rels.contains(RelativeAxisCode::REL_X)
        && rels.contains(RelativeAxisCode::REL_Y)
    {
        return "mouse";
    }
    "other"
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

    use evdev::AttributeSet;

    fn keys(codes: &[KeyCode]) -> AttributeSet<KeyCode> {
        let mut s = AttributeSet::<KeyCode>::new();
        for c in codes {
            s.insert(*c);
        }
        s
    }

    fn rels(codes: &[RelativeAxisCode]) -> AttributeSet<RelativeAxisCode> {
        let mut s = AttributeSet::<RelativeAxisCode>::new();
        for c in codes {
            s.insert(*c);
        }
        s
    }

    #[test]
    fn classify_kbd_when_key_a_present() {
        let k = keys(&[KeyCode::KEY_A, KeyCode::KEY_VOLUMEUP]);
        assert_eq!(classify_capabilities(Some(&k), None), "kbd");
    }

    #[test]
    fn classify_numpad_when_key_kp0_present_but_no_key_a() {
        let k = keys(&[KeyCode::KEY_KP0, KeyCode::KEY_KP1]);
        assert_eq!(classify_capabilities(Some(&k), None), "numpad");
    }

    #[test]
    fn classify_kbd_wins_over_numpad() {
        // KEY_A wins even when KP keys are also present.
        let k = keys(&[KeyCode::KEY_A, KeyCode::KEY_KP0]);
        assert_eq!(classify_capabilities(Some(&k), None), "kbd");
    }

    #[test]
    fn classify_mouse_when_rel_xy_present() {
        let r = rels(&[RelativeAxisCode::REL_X, RelativeAxisCode::REL_Y]);
        assert_eq!(classify_capabilities(None, Some(&r)), "mouse");
    }

    #[test]
    fn classify_other_when_only_rel_x() {
        let r = rels(&[RelativeAxisCode::REL_X]);
        assert_eq!(classify_capabilities(None, Some(&r)), "other");
    }

    #[test]
    fn classify_other_when_no_caps() {
        assert_eq!(classify_capabilities(None, None), "other");
    }

    #[test]
    fn classify_other_when_keys_but_no_letters_or_kp() {
        // Power/consumer-control buttons: neither KEY_A nor KEY_KP0.
        let k = keys(&[KeyCode::KEY_POWER, KeyCode::KEY_MUTE]);
        assert_eq!(classify_capabilities(Some(&k), None), "other");
    }
}
