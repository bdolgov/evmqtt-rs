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
    /// Full `EVIOCGPHYS` string, e.g. `usb-0000:00:14.0-3/input0`.
    /// Stored for diagnostics only; matching uses the capability
    /// fingerprint (phys can collapse multiple HID collections of the
    /// same interface onto one string, so it's not reliable enough on
    /// its own).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub physical_path: Option<String>,
    /// Stable hex digest of the device's exposed capabilities (event
    /// types + key/REL/ABS codes). This is how we tell apart the
    /// several event nodes a single USB receiver can expose with
    /// identical name/BVPV/phys: each HID collection (mouse / consumer
    /// / system / ...) exposes a different set of codes, so the
    /// fingerprints diverge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_fingerprint: Option<String>,
    /// Short human label derived from capabilities (`kbd`, `mouse`,
    /// `consumer`, `system`). Folded into the display name shown to HA
    /// and into the freshly-allocated slug; not used for matching.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_tag: Option<String>,
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
    /// Full equality across the three identifying axes:
    ///   - serial (`unique_id`)
    ///   - id numbers (bus / vendor / product / version)
    ///   - capabilities (`capability_fingerprint`)
    ///
    /// Name is intentionally not consulted — the kernel/driver-supplied
    /// name is a label, not an identity, and the same device can present
    /// different names across firmware revisions. `unique_id` and
    /// `capability_fingerprint` are skipped only when at least one side
    /// is absent: that's the migration window for records persisted
    /// before those fields existed, and gets closed by the backfill in
    /// [`Database::match_or_insert`].
    pub fn matches(&self, id: &DeviceIdentity) -> bool {
        if self.bus != id.bus
            || self.vendor != id.vendor
            || self.product != id.product
            || self.version != id.version
        {
            return false;
        }
        if let (Some(ours), Some(theirs)) = (self.unique_id.as_deref(), id.unique_id.as_deref())
            && ours != theirs
        {
            return false;
        }
        if let Some(ours) = self.capability_fingerprint.as_deref()
            && ours != id.capability_fingerprint
        {
            return false;
        }
        true
    }

    /// Human-facing name for HA / logs. Folds the capability tag into
    /// the raw evdev name so the four `eventN` nodes of a multi-
    /// collection USB receiver don't surface as four identically-
    /// labelled records. The `"other"` fallback tag is omitted (it
    /// carries no information for a reader).
    pub fn display_name(&self) -> String {
        match self.capability_tag.as_deref() {
            Some(tag) if tag != "other" => format!("{} ({})", self.name, tag),
            _ => self.name.clone(),
        }
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

/// Outcome of `Database::match_or_insert`. The caller persists the DB
/// when this carries new state, and runs the "new device" publish path
/// only on `Inserted`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchOutcome {
    /// An existing record matched. `backfilled` is true when the match
    /// caused us to fill in previously-absent phys fields on the record.
    Matched { slug: String, backfilled: bool },
    /// No existing record matched; a fresh one was inserted.
    Inserted { slug: String },
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
    /// Test-only: production code goes through
    /// [`Database::match_or_insert`] so the backfill of
    /// previously-absent fields happens in lockstep with the match.
    #[cfg(test)]
    fn match_identity(&self, id: &DeviceIdentity) -> Option<&DeviceRecord> {
        self.devices.iter().find(|d| d.matches(id))
    }

    /// Insert a brand-new record built from `id`. Returns the slug it
    /// was assigned. Caller is responsible for matching first; calling
    /// this on a device that already exists in the DB will create a
    /// duplicate entry under a `-N` slug.
    ///
    /// The capability tag is stored separately and folded into the
    /// slug at allocation time so a multi-collection receiver yields
    /// informative slugs (`logitech-receiver-mouse`, …). The stored
    /// `name` stays the raw evdev string so matching can compare it
    /// against future sightings — display formatting happens via
    /// [`DeviceRecord::display_name`].
    pub fn insert(&mut self, id: &DeviceIdentity) -> String {
        // `"other"` is the fallback tag — it carries no information,
        // so we don't pollute the slug or display name with it. The
        // capability fingerprint still distinguishes the records;
        // slug deconfliction falls back to the `-N` suffix when two
        // `"other"` sub-devices of the same dongle collide.
        let slug_seed = if id.capability_tag == "other" {
            id.name.clone()
        } else {
            format!("{} ({})", id.name, id.capability_tag)
        };
        let slug = self.allocate_slug(&slugify(&slug_seed));
        let rec = DeviceRecord {
            slug: slug.clone(),
            name: id.name.clone(),
            unique_id: id.unique_id.clone(),
            bus: id.bus,
            vendor: id.vendor,
            product: id.product,
            version: id.version,
            physical_path: id.physical_path.clone(),
            capability_fingerprint: Some(id.capability_fingerprint.clone()),
            capability_tag: Some(id.capability_tag.to_string()),
            enabled: false,
            observed_keys: Vec::new(),
        };
        self.devices.push(rec);
        slug
    }

    /// Look up `id`, inserting a new record if nothing matches. When an
    /// existing record matches but predates capability-aware matching
    /// (its `capability_fingerprint` is still `None`), the live value
    /// is backfilled so a subsequent sighting of a *different*
    /// sub-device of the same dongle is recognised as distinct
    /// instead of folding into the same slug. `unique_id`,
    /// `physical_path` and `capability_tag` are backfilled at the same
    /// time so the record reaches a fully-populated steady state on
    /// the first post-upgrade sighting.
    pub fn match_or_insert(&mut self, id: &DeviceIdentity) -> MatchOutcome {
        if let Some(idx) = self.devices.iter().position(|d| d.matches(id)) {
            let rec = &mut self.devices[idx];
            let mut backfilled = false;
            if rec.capability_fingerprint.is_none() {
                rec.capability_fingerprint = Some(id.capability_fingerprint.clone());
                backfilled = true;
            }
            if rec.capability_tag.is_none() {
                rec.capability_tag = Some(id.capability_tag.to_string());
                backfilled = true;
            }
            if rec.unique_id.is_none()
                && let Some(ref uniq) = id.unique_id
            {
                rec.unique_id = Some(uniq.clone());
                backfilled = true;
            }
            if rec.physical_path.is_none()
                && let Some(ref phys) = id.physical_path
            {
                rec.physical_path = Some(phys.clone());
                backfilled = true;
            }
            MatchOutcome::Matched {
                slug: rec.slug.clone(),
                backfilled,
            }
        } else {
            MatchOutcome::Inserted {
                slug: self.insert(id),
            }
        }
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
            capability_fingerprint: "0000000000000000".to_string(),
            capability_tag: "other",
        }
    }

    fn ident_with_cap(
        name: &str,
        b: u16,
        v: u16,
        p: u16,
        ver: u16,
        fingerprint: &str,
        tag: &'static str,
    ) -> DeviceIdentity {
        let mut id = ident(name, None, b, v, p, ver);
        id.capability_fingerprint = fingerprint.to_string();
        id.capability_tag = tag;
        id
    }

    #[test]
    fn slug_folds_in_capability_tag() {
        let mut db = Database::default();
        let slug = db.insert(&ident_with_cap(
            "USB Keyboard",
            3,
            1,
            2,
            3,
            "0000000000000000",
            "kbd",
        ));
        assert_eq!(slug, "usb-keyboard-kbd");
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
    fn unique_id_alone_is_not_enough() {
        // OLD behaviour: matching unique_id wins over everything.
        // NEW behaviour: BVPV (and capabilities, when present) must
        // also agree — uniq is just one axis of equality.
        let mut db = Database::default();
        db.insert(&ident("Keyboard", Some("abc"), 3, 1, 2, 3));
        let renamed_and_replugged = ident("Renamed", Some("abc"), 9, 9, 9, 9);
        assert!(db.match_identity(&renamed_and_replugged).is_none());
    }

    #[test]
    fn matches_on_bvp_when_name_differs() {
        // Name is a label, not an identity — the same device on a
        // different driver build can present a different name. As
        // long as BVPV (and any present uniq / fingerprint) agree, it
        // is the same record.
        let mut db = Database::default();
        db.insert(&ident("Keyboard", None, 3, 0x046d, 0xc52b, 0x0111));
        let id = ident("Some other name", None, 3, 0x046d, 0xc52b, 0x0111);
        let m = db.match_identity(&id).unwrap();
        assert_eq!(m.slug, "keyboard");
    }

    #[test]
    fn zero_bvp_devices_distinguished_by_capability_fingerprint() {
        // GPIO-style devices report no BVPV. With name no longer
        // consulted, the capability fingerprint is what tells two
        // such devices apart.
        let mut db = Database::default();
        db.insert(&ident("GPIO Keys", None, 0, 0, 0, 0));
        let same_caps = ident("Different label", None, 0, 0, 0, 0);
        assert!(db.match_identity(&same_caps).is_some());
        let different_caps = ident_with_cap("GPIO Keys", 0, 0, 0, 0, "ffffffffffffffff", "other");
        assert!(db.match_identity(&different_caps).is_none());
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
    fn same_bvp_different_capabilities_become_separate_records() {
        // Multi-collection USB receiver: four interfaces with identical
        // name + BVPV + no unique_id, distinguishable only by their
        // exposed capabilities.
        let mut db = Database::default();
        let kbd = ident_with_cap(
            "Logitech Receiver",
            3,
            0x046d,
            0xc52b,
            0x0111,
            "aaaaaaaaaaaaaaaa",
            "kbd",
        );
        let mouse = ident_with_cap(
            "Logitech Receiver",
            3,
            0x046d,
            0xc52b,
            0x0111,
            "bbbbbbbbbbbbbbbb",
            "mouse",
        );
        // Consumer-control and system-control collections both fall
        // through the classifier to `"other"`. They're still distinct
        // records because the fingerprints differ; the tag merely
        // makes slug allocation deconflict via the `-N` suffix.
        let consumer = ident_with_cap(
            "Logitech Receiver",
            3,
            0x046d,
            0xc52b,
            0x0111,
            "cccccccccccccccc",
            "other",
        );
        let system = ident_with_cap(
            "Logitech Receiver",
            3,
            0x046d,
            0xc52b,
            0x0111,
            "dddddddddddddddd",
            "other",
        );

        let mut slugs = Vec::new();
        for id in [&kbd, &mouse, &consumer, &system] {
            match db.match_or_insert(id) {
                MatchOutcome::Inserted { slug } => slugs.push(slug),
                MatchOutcome::Matched { .. } => {
                    panic!("each capability shape must yield a distinct record");
                }
            }
        }
        assert_eq!(db.devices.len(), 4);
        assert_eq!(slugs[0], "logitech-receiver-kbd");
        assert_eq!(slugs[1], "logitech-receiver-mouse");
        // `"other"` is omitted from the slug — the `-N` deconfliction
        // suffix handles the case where two `"other"` sub-devices of
        // the same dongle collide.
        assert_eq!(slugs[2], "logitech-receiver");
        assert_eq!(slugs[3], "logitech-receiver-2");
        // The tag also folds into the display name shown to HA (also
        // skipping `"other"`), but the stored `name` stays the raw
        // evdev string so matching can compare it against future
        // sightings.
        assert_eq!(db.devices[1].name, "Logitech Receiver");
        assert_eq!(db.devices[1].display_name(), "Logitech Receiver (mouse)");
        assert_eq!(db.devices[2].display_name(), "Logitech Receiver");

        // Re-sighting the same capability shape matches the existing record.
        assert!(matches!(
            db.match_or_insert(&mouse),
            MatchOutcome::Matched { backfilled: false, slug } if slug == slugs[1],
        ));
    }

    #[test]
    fn legacy_record_without_capability_fingerprint_backfills_then_distinguishes() {
        // Records persisted before capability-aware matching have no
        // fingerprint. The first sighting backfills it; the next
        // sub-device of the same dongle is then correctly seen as
        // distinct.
        let mut db = Database::default();
        db.insert(&ident("Logitech Receiver", None, 3, 0x046d, 0xc52b, 0x0111));
        db.devices[0].capability_fingerprint = None;
        db.devices[0].capability_tag = None;
        db.devices[0].physical_path = None;

        let kbd = ident_with_cap(
            "Logitech Receiver",
            3,
            0x046d,
            0xc52b,
            0x0111,
            "aaaaaaaaaaaaaaaa",
            "kbd",
        );
        let outcome = db.match_or_insert(&kbd);
        assert!(matches!(
            outcome,
            MatchOutcome::Matched {
                backfilled: true,
                ..
            }
        ));
        assert_eq!(
            db.devices[0].capability_fingerprint.as_deref(),
            Some("aaaaaaaaaaaaaaaa")
        );

        let mouse = ident_with_cap(
            "Logitech Receiver",
            3,
            0x046d,
            0xc52b,
            0x0111,
            "bbbbbbbbbbbbbbbb",
            "mouse",
        );
        let outcome = db.match_or_insert(&mouse);
        assert!(matches!(outcome, MatchOutcome::Inserted { .. }));
        assert_eq!(db.devices.len(), 2);
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
