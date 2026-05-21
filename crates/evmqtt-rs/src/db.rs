use crate::discovery::DeviceIdentity;
use crate::slug::slugify;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("failed to read database {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to write database {path}: {source}")]
    Write {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse database {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to serialize database: {0}")]
    Serialize(#[from] toml::ser::Error),
}

/// One device the daemon has ever seen. Every identifier we extracted
/// from evdev is stored; the *match* against a fresh `DeviceIdentity`
/// is computed on the fly, preferring the most precise signal both
/// sides supply (unique_id ▶ bus/vendor/product/version ▶ name).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeviceRecord {
    /// Stable internal id. Used as the MQTT topic slug and the suffix
    /// of the HA device identifier. Never changes after first
    /// assignment.
    pub slug: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique_id: Option<String>,
    #[serde(default)]
    pub bus: u16,
    #[serde(default)]
    pub vendor: u16,
    #[serde(default)]
    pub product: u16,
    #[serde(default)]
    pub version: u16,
    #[serde(default)]
    pub enabled: bool,
    /// u16 evdev key codes. Stored as integers rather than symbolic
    /// names to keep the file small.
    #[serde(default)]
    pub observed_keys: Vec<u16>,
}

impl DeviceRecord {
    /// Does this stored record describe the device behind `id`?
    ///
    /// Mirrors `DeviceIdentity::suggest_matcher`: prefer the most
    /// precise signal both sides have.
    pub fn matches(&self, id: &DeviceIdentity) -> bool {
        if let (Some(ours), Some(theirs)) = (self.unique_id.as_deref(), id.unique_id.as_deref()) {
            return ours == theirs;
        }
        if self.vendor != 0 || self.product != 0 {
            return self.bus == id.bus
                && self.vendor == id.vendor
                && self.product == id.product
                && self.version == id.version;
        }
        self.name == id.name
    }

    /// True iff `code` was newly added. Caller can use this to decide
    /// whether to republish discovery.
    pub fn record_observed_key(&mut self, code: u16) -> bool {
        if self.observed_keys.contains(&code) {
            return false;
        }
        self.observed_keys.push(code);
        self.observed_keys.sort_unstable();
        true
    }
}

/// On-disk schema. The outer enum carries the format version in its
/// variant name. To add a new format, add another variant alongside
/// `SchemaV1` and extend the load match. Old binaries reading a newer
/// file fail closed (unknown variant). New binaries reading an older
/// file dispatch on the variant and migrate in-memory if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum Schema {
    SchemaV1(SchemaV1),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SchemaV1 {
    #[serde(default)]
    devices: Vec<DeviceRecord>,
}

#[derive(Debug, Default, Clone)]
pub struct Database {
    pub devices: Vec<DeviceRecord>,
}

impl Database {
    /// Read the database from `path`. A missing file yields an empty
    /// database (first run); other I/O errors propagate.
    pub fn load(path: &Path) -> Result<Self, DbError> {
        let text = match fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => {
                return Err(DbError::Read {
                    path: path.display().to_string(),
                    source: e,
                });
            }
        };
        let parsed: Schema = toml::from_str(&text).map_err(|e| DbError::Parse {
            path: path.display().to_string(),
            source: e,
        })?;
        let devices = match parsed {
            Schema::SchemaV1(v1) => v1.devices,
        };
        Ok(Self { devices })
    }

    /// Atomically replace the on-disk file with the current state.
    /// Writes to a sibling temp file, fsyncs it, then renames over the
    /// target. A crash either leaves the previous file untouched or
    /// presents the new one — never a partial write.
    pub fn save_atomic(&self, path: &Path) -> Result<(), DbError> {
        let on_disk = Schema::SchemaV1(SchemaV1 {
            devices: self.devices.clone(),
        });
        let text = toml::to_string(&on_disk)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        fs::create_dir_all(&parent).map_err(|e| DbError::Write {
            path: parent.display().to_string(),
            source: e,
        })?;

        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "db.toml".to_string());
        let tmp_path = parent.join(format!(".{file_name}.tmp.{pid}.{nonce}"));

        {
            let mut f = fs::File::create(&tmp_path).map_err(|e| DbError::Write {
                path: tmp_path.display().to_string(),
                source: e,
            })?;
            f.write_all(text.as_bytes()).map_err(|e| DbError::Write {
                path: tmp_path.display().to_string(),
                source: e,
            })?;
            f.sync_all().map_err(|e| DbError::Write {
                path: tmp_path.display().to_string(),
                source: e,
            })?;
        }
        if let Err(e) = fs::rename(&tmp_path, path) {
            // Best-effort cleanup so we don't leave .tmp turds behind.
            let _ = fs::remove_file(&tmp_path);
            return Err(DbError::Write {
                path: path.display().to_string(),
                source: e,
            });
        }
        Ok(())
    }

    pub fn find(&self, slug: &str) -> Option<&DeviceRecord> {
        self.devices.iter().find(|d| d.slug == slug)
    }

    pub fn find_mut(&mut self, slug: &str) -> Option<&mut DeviceRecord> {
        self.devices.iter_mut().find(|d| d.slug == slug)
    }

    /// First record whose stored identifiers match the live identity.
    pub fn match_identity(&self, id: &DeviceIdentity) -> Option<&DeviceRecord> {
        self.devices.iter().find(|d| d.matches(id))
    }

    /// Insert a brand-new record built from `id`. Returns the slug it
    /// was assigned. Caller is responsible for matching first; calling
    /// this on a device that already exists in the DB will create a
    /// duplicate entry under a `-N` slug.
    pub fn insert(&mut self, id: &DeviceIdentity) -> String {
        let slug = self.allocate_slug(&slugify(&id.name));
        let rec = DeviceRecord {
            slug: slug.clone(),
            name: id.name.clone(),
            unique_id: id.unique_id.clone(),
            bus: id.bus,
            vendor: id.vendor,
            product: id.product,
            version: id.version,
            enabled: false,
            observed_keys: Vec::new(),
        };
        self.devices.push(rec);
        slug
    }

    /// Returns `true` iff the slug existed.
    pub fn remove(&mut self, slug: &str) -> bool {
        let len = self.devices.len();
        self.devices.retain(|d| d.slug != slug);
        self.devices.len() != len
    }

    fn allocate_slug(&self, base: &str) -> String {
        let taken: HashSet<&str> = self.devices.iter().map(|d| d.slug.as_str()).collect();
        if !taken.contains(base) {
            return base.to_string();
        }
        for n in 2.. {
            let candidate = format!("{base}-{n}");
            if !taken.contains(candidate.as_str()) {
                return candidate;
            }
        }
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident(name: &str, uniq: Option<&str>, b: u16, v: u16, p: u16, ver: u16) -> DeviceIdentity {
        DeviceIdentity {
            path: PathBuf::from("/dev/input/event0"),
            name: name.to_string(),
            unique_id: uniq.map(|s| s.to_string()),
            bus: b,
            vendor: v,
            product: p,
            version: ver,
            physical_path: None,
            has_keys: true,
        }
    }

    #[test]
    fn allocates_slug_from_name() {
        let mut db = Database::default();
        let slug = db.insert(&ident("USB Keyboard", Some("abc"), 3, 1, 2, 3));
        assert_eq!(slug, "usb-keyboard");
    }

    #[test]
    fn slug_collisions_get_numeric_suffix() {
        let mut db = Database::default();
        db.insert(&ident("USB Keyboard", Some("a"), 3, 1, 2, 3));
        let s2 = db.insert(&ident("USB Keyboard", Some("b"), 3, 1, 2, 3));
        let s3 = db.insert(&ident("USB Keyboard", Some("c"), 3, 1, 2, 3));
        assert_eq!(s2, "usb-keyboard-2");
        assert_eq!(s3, "usb-keyboard-3");
    }

    #[test]
    fn match_prefers_unique_id() {
        let mut db = Database::default();
        db.insert(&ident("Keyboard", Some("abc"), 3, 1, 2, 3));
        // Same unique_id, different BVPV/name — should still match.
        let id = ident("Renamed", Some("abc"), 9, 9, 9, 9);
        let m = db.match_identity(&id).unwrap();
        assert_eq!(m.slug, "keyboard");
    }

    #[test]
    fn match_falls_back_to_bvp() {
        let mut db = Database::default();
        db.insert(&ident("Keyboard", None, 3, 0x046d, 0xc52b, 0x0111));
        // No unique_id either side — match by quad.
        let id = ident("Some other name", None, 3, 0x046d, 0xc52b, 0x0111);
        let m = db.match_identity(&id).unwrap();
        assert_eq!(m.slug, "keyboard");
    }

    #[test]
    fn match_falls_back_to_name_when_no_other_ids() {
        let mut db = Database::default();
        db.insert(&ident("GPIO Keys", None, 0, 0, 0, 0));
        let id = ident("GPIO Keys", None, 0, 0, 0, 0);
        assert!(db.match_identity(&id).is_some());
        let id2 = ident("Different", None, 0, 0, 0, 0);
        assert!(db.match_identity(&id2).is_none());
    }

    #[test]
    fn unique_id_on_one_side_only_does_not_match_by_uniq() {
        let mut db = Database::default();
        // Stored has unique_id; incoming doesn't — uniq match is skipped,
        // BVPV match applies and succeeds.
        db.insert(&ident("Keyboard", Some("abc"), 3, 0x046d, 0xc52b, 0x0111));
        let id = ident("Keyboard", None, 3, 0x046d, 0xc52b, 0x0111);
        assert!(db.match_identity(&id).is_some());
    }

    #[test]
    fn observed_keys_dedup_and_sort() {
        let mut db = Database::default();
        db.insert(&ident("Keyboard", Some("abc"), 3, 1, 2, 3));
        let rec = db.find_mut("keyboard").unwrap();
        assert!(rec.record_observed_key(30));
        assert!(rec.record_observed_key(28));
        assert!(!rec.record_observed_key(30));
        assert_eq!(rec.observed_keys, vec![28, 30]);
    }

    #[test]
    fn save_then_load_round_trip() {
        let mut db = Database::default();
        db.insert(&ident("Keyboard", Some("abc"), 3, 1, 2, 3));
        let rec = db.find_mut("keyboard").unwrap();
        rec.enabled = true;
        rec.record_observed_key(30);
        rec.record_observed_key(115);

        let tmp =
            std::env::temp_dir().join(format!("evmqtt-rs-db-test-{}.toml", std::process::id()));
        db.save_atomic(&tmp).unwrap();
        let loaded = Database::load(&tmp).unwrap();
        let _ = fs::remove_file(&tmp);

        assert_eq!(loaded.devices.len(), 1);
        let r = &loaded.devices[0];
        assert_eq!(r.slug, "keyboard");
        assert_eq!(r.name, "Keyboard");
        assert_eq!(r.unique_id.as_deref(), Some("abc"));
        assert!(r.enabled);
        assert_eq!(r.observed_keys, vec![30, 115]);
    }

    #[test]
    fn missing_file_loads_as_empty() {
        let path = PathBuf::from("/nonexistent/evmqtt-test/should-not-exist.toml");
        let db = Database::load(&path).unwrap();
        assert!(db.devices.is_empty());
    }

    #[test]
    fn remove_drops_record() {
        let mut db = Database::default();
        db.insert(&ident("Keyboard", Some("abc"), 3, 1, 2, 3));
        assert!(db.remove("keyboard"));
        assert!(!db.remove("keyboard"));
        assert!(db.devices.is_empty());
    }
}
