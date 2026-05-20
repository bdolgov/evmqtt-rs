/// Convert arbitrary text into a lowercase, hyphen-separated slug
/// safe to embed in MQTT topics and HA `unique_id`s.
pub fn slugify(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let mut out = String::with_capacity(lower.len());
    let mut prev_hyphen = true; // suppress leading hyphens
    for ch in lower.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            prev_hyphen = false;
        } else if !prev_hyphen {
            out.push('-');
            prev_hyphen = true;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

/// Slugify an evdev key name like `KEY_VOLUMEUP` → `volumeup`.
/// Falls back to a numeric code when the key has no symbolic name.
pub fn key_slug(key_name: &str) -> String {
    let stripped = key_name
        .strip_prefix("KEY_")
        .or_else(|| key_name.strip_prefix("BTN_"))
        .unwrap_or(key_name);
    slugify(stripped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercases_and_hyphenates_spaces() {
        assert_eq!(slugify("USB Keyboard"), "usb-keyboard");
    }

    #[test]
    fn collapses_repeated_separators() {
        assert_eq!(slugify("Logitech  G502__HERO"), "logitech-g502-hero");
    }

    #[test]
    fn strips_unicode_and_punctuation() {
        assert_eq!(
            slugify("Reichweite® (USB) — Bridge"),
            "reichweite-usb-bridge"
        );
    }

    #[test]
    fn trims_leading_and_trailing_separators() {
        assert_eq!(slugify("__bridge__"), "bridge");
        assert_eq!(slugify("-"), "unknown");
    }

    #[test]
    fn empty_yields_unknown() {
        assert_eq!(slugify(""), "unknown");
    }

    #[test]
    fn key_slug_strips_key_prefix() {
        assert_eq!(key_slug("KEY_VOLUMEUP"), "volumeup");
        assert_eq!(key_slug("KEY_LEFTSHIFT"), "leftshift");
    }

    #[test]
    fn key_slug_strips_btn_prefix() {
        assert_eq!(key_slug("BTN_LEFT"), "left");
    }

    #[test]
    fn key_slug_keeps_unknown_format() {
        assert_eq!(key_slug("UNKNOWN_42"), "unknown-42");
    }
}
