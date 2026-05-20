use crate::config::DeviceMatcher;
use evdev::{Device, EventType};
use std::io::{self, Write};
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

    pub fn matches(&self, m: &DeviceMatcher) -> bool {
        match m {
            DeviceMatcher::UniqueId(s) => self.unique_id.as_deref() == Some(s.as_str()),
            DeviceMatcher::BusVendorProductVersion(b, v, p, ver) => {
                self.bus == *b && self.vendor == *v && self.product == *p && self.version == *ver
            }
            DeviceMatcher::Name(s) => self.name == *s,
        }
    }

    /// Pick the most precise matcher this identity supports.
    /// Order: UniqueId (if non-empty) → BusVendorProductVersion → Name.
    pub fn suggest_matcher(&self) -> DeviceMatcher {
        if let Some(uniq) = self.unique_id.as_ref() {
            return DeviceMatcher::UniqueId(uniq.clone());
        }
        if self.vendor != 0 || self.product != 0 {
            return DeviceMatcher::BusVendorProductVersion(
                self.bus,
                self.vendor,
                self.product,
                self.version,
            );
        }
        DeviceMatcher::Name(self.name.clone())
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

/// Enumerate every visible input device.
pub fn enumerate_identities() -> Vec<DeviceIdentity> {
    let mut out: Vec<DeviceIdentity> = evdev::enumerate()
        .map(|(path, device)| DeviceIdentity::from_open_device(path, &device))
        .collect();
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

/// Write a TOML `[[device]]` snippet for each visible input device, using
/// the most precise matcher each device supports. Intended for human users
/// to paste into their config.
pub fn write_detect_snippets<W: Write>(out: &mut W) -> io::Result<usize> {
    let identities = enumerate_identities();
    if identities.is_empty() {
        writeln!(
            out,
            "# no input devices visible (check group membership / permissions on /dev/input)"
        )?;
        return Ok(0);
    }
    for (i, id) in identities.iter().enumerate() {
        if i > 0 {
            writeln!(out)?;
        }
        write_one_snippet(out, id)?;
    }
    Ok(identities.len())
}

fn write_one_snippet<W: Write>(out: &mut W, id: &DeviceIdentity) -> io::Result<()> {
    writeln!(
        out,
        "# {}  ({})  has_keys={}",
        id.name,
        id.path.display(),
        if id.has_keys { "yes" } else { "no" },
    )?;
    writeln!(out, "[[device]]")?;
    match id.suggest_matcher() {
        DeviceMatcher::UniqueId(s) => {
            writeln!(out, "matcher = {{ unique_id = {} }}", toml_string(&s))?;
        }
        DeviceMatcher::BusVendorProductVersion(b, v, p, ver) => {
            writeln!(
                out,
                "matcher = {{ bus_vendor_product_version = [0x{b:04x}, 0x{v:04x}, 0x{p:04x}, 0x{ver:04x}] }}",
            )?;
        }
        DeviceMatcher::Name(s) => {
            writeln!(out, "matcher = {{ name = {} }}", toml_string(&s))?;
        }
    }
    writeln!(out, "name    = {}", toml_string(&id.name))?;
    writeln!(
        out,
        "# mqtt_path = {}",
        toml_string(&crate::slug::slugify(&id.name))
    )?;
    Ok(())
}

/// Render a string as a TOML basic string literal.
fn toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id_with(
        name: &str,
        unique_id: Option<&str>,
        bus: u16,
        vendor: u16,
        product: u16,
        version: u16,
    ) -> DeviceIdentity {
        DeviceIdentity {
            path: PathBuf::from("/dev/input/event0"),
            name: name.to_string(),
            unique_id: unique_id.map(|s| s.to_string()),
            bus,
            vendor,
            product,
            version,
            physical_path: None,
            has_keys: true,
        }
    }

    #[test]
    fn matches_unique_id_exactly() {
        let id = id_with("USB Keyboard", Some("abc"), 3, 0x046d, 0xc52b, 0x0111);
        assert!(id.matches(&DeviceMatcher::UniqueId("abc".into())));
        assert!(!id.matches(&DeviceMatcher::UniqueId("xyz".into())));
    }

    #[test]
    fn matches_bvp_quad() {
        let id = id_with("USB Keyboard", None, 0x0003, 0x046d, 0xc52b, 0x0111);
        assert!(id.matches(&DeviceMatcher::BusVendorProductVersion(
            0x0003, 0x046d, 0xc52b, 0x0111
        )));
        assert!(!id.matches(&DeviceMatcher::BusVendorProductVersion(
            0x0003, 0x046d, 0xc52b, 0x0000
        )));
    }

    #[test]
    fn matches_name_exact_case_sensitive() {
        let id = id_with("USB Keyboard", None, 0, 0, 0, 0);
        assert!(id.matches(&DeviceMatcher::Name("USB Keyboard".into())));
        assert!(!id.matches(&DeviceMatcher::Name("usb keyboard".into())));
    }

    #[test]
    fn suggest_prefers_unique_id() {
        let id = id_with("USB Keyboard", Some("abc"), 3, 0x046d, 0xc52b, 0x0111);
        assert_eq!(id.suggest_matcher(), DeviceMatcher::UniqueId("abc".into()));
    }

    #[test]
    fn suggest_falls_back_to_bvp() {
        let id = id_with("USB Keyboard", None, 0x0003, 0x046d, 0xc52b, 0x0111);
        assert_eq!(
            id.suggest_matcher(),
            DeviceMatcher::BusVendorProductVersion(0x0003, 0x046d, 0xc52b, 0x0111)
        );
    }

    #[test]
    fn suggest_falls_back_to_name() {
        let id = id_with("Custom GPIO", None, 0, 0, 0, 0);
        assert_eq!(
            id.suggest_matcher(),
            DeviceMatcher::Name("Custom GPIO".into())
        );
    }

    #[test]
    fn toml_string_escapes_quotes_and_backslashes() {
        assert_eq!(toml_string("simple"), r#""simple""#);
        assert_eq!(toml_string(r#"a"b\c"#), r#""a\"b\\c""#);
    }
}
