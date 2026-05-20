/// (preset_name, background_hex, foreground_hex)
const PRESETS: &[(&str, &str, &str)] = &[
    ("preset0", "#e74c3c", "#ffffff"),  // Red
    ("preset1", "#e67e22", "#ffffff"),  // Orange
    ("preset2", "#8b4513", "#ffffff"),  // Brown
    ("preset3", "#f1c40f", "#000000"),  // Yellow
    ("preset4", "#2ecc71", "#ffffff"),  // Green
    ("preset5", "#1abc9c", "#ffffff"),  // Teal
    ("preset6", "#808000", "#ffffff"),  // Olive
    ("preset7", "#3498db", "#ffffff"),  // Blue
    ("preset8", "#9b59b6", "#ffffff"),  // Purple
    ("preset9", "#c0392b", "#ffffff"),  // Cranberry
    ("preset10", "#708090", "#ffffff"), // Steel
    ("preset11", "#4a5568", "#ffffff"), // DarkSteel
    ("preset12", "#95a5a6", "#000000"), // Gray
    ("preset13", "#636e72", "#ffffff"), // DarkGray
    ("preset14", "#2d3436", "#ffffff"), // Black
    ("preset15", "#8b0000", "#ffffff"), // DarkRed
    ("preset16", "#d35400", "#ffffff"), // DarkOrange
    ("preset17", "#5d3a1a", "#ffffff"), // DarkBrown
    ("preset18", "#b8860b", "#ffffff"), // DarkYellow
    ("preset19", "#1e7e34", "#ffffff"), // DarkGreen
    ("preset20", "#0e6655", "#ffffff"), // DarkTeal
    ("preset21", "#556b2f", "#ffffff"), // DarkOlive
    ("preset22", "#1a5276", "#ffffff"), // DarkBlue
    ("preset23", "#6c3483", "#ffffff"), // DarkPurple
    ("preset24", "#922b21", "#ffffff"), // DarkCranberry
];

/// Look up a preset by name, returning `(bg_hex, fg_hex)`.
///
/// The preset name is matched case-insensitively.
///
/// ```
/// # use bifrost_graph::preset_colors::preset_to_hex;
/// assert_eq!(preset_to_hex("preset0"), Some(("#e74c3c", "#ffffff")));
/// assert_eq!(preset_to_hex("Preset7"), Some(("#3498db", "#ffffff")));
/// assert_eq!(preset_to_hex("unknown"), None);
/// ```
pub fn preset_to_hex(preset: &str) -> Option<(&'static str, &'static str)> {
    let lower = preset.to_ascii_lowercase();
    PRESETS
        .iter()
        .find(|(name, _, _)| *name == lower)
        .map(|(_, bg, fg)| (*bg, *fg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preset_to_hex_exact() {
        let (bg, fg) = preset_to_hex("preset0").expect("should find preset0");
        assert_eq!(bg, "#e74c3c");
        assert_eq!(fg, "#ffffff");
    }

    #[test]
    fn preset_to_hex_case_insensitive() {
        assert!(preset_to_hex("Preset3").is_some());
        assert!(preset_to_hex("PRESET3").is_some());
    }

    #[test]
    fn preset_to_hex_yellow_fg_is_black() {
        let (_, fg) = preset_to_hex("preset3").expect("should find preset3");
        assert_eq!(fg, "#000000");
    }

    #[test]
    fn preset_to_hex_unknown() {
        assert!(preset_to_hex("preset99").is_none());
        assert!(preset_to_hex("garbage").is_none());
    }
}
