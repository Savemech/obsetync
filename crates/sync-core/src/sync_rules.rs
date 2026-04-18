/// Binary file strategy — determines how conflicts are handled per file type.
/// Configured in sync-rules.toml. See D-003.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BinaryStrategy {
    /// Keep both versions on conflict. Default for unrecognized binary.
    ConflictCopy,
    /// Never sync. Each device has its own copy.
    LocalOnly,
    /// Synced but treated as write-once. Conflicts auto-rename.
    Immutable,
}

/// Rules for how different file types are handled during sync.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SyncRules {
    /// Glob pattern → strategy mapping, evaluated in order.
    /// First match wins. Last entry should be a wildcard default.
    #[serde(default = "default_rules")]
    pub binary_rules: Vec<(String, BinaryStrategy)>,
}

impl Default for SyncRules {
    fn default() -> Self {
        Self {
            binary_rules: default_rules(),
        }
    }
}

fn default_rules() -> Vec<(String, BinaryStrategy)> {
    vec![
        // Never sync — machine-local
        ("*.sqlite".into(), BinaryStrategy::LocalOnly),
        ("*.sqlite-wal".into(), BinaryStrategy::LocalOnly),
        ("*.sqlite-shm".into(), BinaryStrategy::LocalOnly),
        (".DS_Store".into(), BinaryStrategy::LocalOnly),
        ("thumbs.db".into(), BinaryStrategy::LocalOnly),
        // Immutable assets
        ("*.png".into(), BinaryStrategy::Immutable),
        ("*.jpg".into(), BinaryStrategy::Immutable),
        ("*.jpeg".into(), BinaryStrategy::Immutable),
        ("*.webp".into(), BinaryStrategy::Immutable),
        ("*.gif".into(), BinaryStrategy::Immutable),
        ("*.bmp".into(), BinaryStrategy::Immutable),
        ("*.svg".into(), BinaryStrategy::Immutable),
        ("*.pdf".into(), BinaryStrategy::Immutable),
        ("*.mp3".into(), BinaryStrategy::Immutable),
        ("*.mp4".into(), BinaryStrategy::Immutable),
        ("*.wav".into(), BinaryStrategy::Immutable),
        ("*.ogg".into(), BinaryStrategy::Immutable),
        ("*.flac".into(), BinaryStrategy::Immutable),
        ("*.zip".into(), BinaryStrategy::Immutable),
        ("*.tar".into(), BinaryStrategy::Immutable),
        ("*.gz".into(), BinaryStrategy::Immutable),
        // Default
        ("*".into(), BinaryStrategy::ConflictCopy),
    ]
}

impl SyncRules {
    /// Determine the strategy for a given file path.
    pub fn strategy_for(&self, path: &str) -> &BinaryStrategy {
        let filename = path.rsplit('/').next().unwrap_or(path);

        for (pattern, strategy) in &self.binary_rules {
            if glob_match(pattern, filename) {
                return strategy;
            }
        }

        // Fallback if no rules match (shouldn't happen with default wildcard).
        &BinaryStrategy::ConflictCopy
    }

    /// Check if a path should be excluded from sync entirely.
    pub fn is_local_only(&self, path: &str) -> bool {
        self.strategy_for(path) == &BinaryStrategy::LocalOnly
    }
}

/// Simple glob matching — supports `*` (any chars) and `?` (single char).
/// Not a full glob implementation, covers the patterns we need.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern = pattern.as_bytes();
    let text = text.as_bytes();
    let mut pi = 0;
    let mut ti = 0;
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Known text file extensions that support three-way merge.
const TEXT_EXTENSIONS: &[&str] = &[
    "md",
    "txt",
    "css",
    "json",
    "js",
    "ts",
    "html",
    "xml",
    "yaml",
    "yml",
    "toml",
    "csv",
    "canvas",
    "excalidraw",
    "svg",
    "tex",
    "bib",
    "org",
    "rst",
];

/// Check if a file path is a text file (eligible for three-way merge).
pub fn is_text_file(path: &str) -> bool {
    let ext = path.rsplit('.').next().unwrap_or("");
    TEXT_EXTENSIONS.contains(&ext.to_lowercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_basic() {
        assert!(glob_match("*.png", "photo.png"));
        assert!(glob_match("*.png", "dir/photo.png")); // matches filename only
        assert!(!glob_match("*.png", "photo.jpg"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match(".DS_Store", ".DS_Store"));
        assert!(!glob_match(".DS_Store", "other.txt"));
    }

    #[test]
    fn default_rules() {
        let rules = SyncRules::default();

        assert_eq!(rules.strategy_for("photo.png"), &BinaryStrategy::Immutable);
        assert_eq!(
            rules.strategy_for("data.sqlite"),
            &BinaryStrategy::LocalOnly
        );
        assert_eq!(
            rules.strategy_for("notes/photo.jpg"),
            &BinaryStrategy::Immutable
        );
        assert_eq!(
            rules.strategy_for("design.psd"),
            &BinaryStrategy::ConflictCopy
        );
    }

    #[test]
    fn local_only_check() {
        let rules = SyncRules::default();
        assert!(rules.is_local_only(".DS_Store"));
        assert!(rules.is_local_only("cache.sqlite"));
        assert!(!rules.is_local_only("notes.md"));
    }

    #[test]
    fn text_file_detection() {
        assert!(is_text_file("notes/jan.md"));
        assert!(is_text_file("style.css"));
        assert!(is_text_file("data.json"));
        assert!(is_text_file("drawing.canvas"));
        assert!(!is_text_file("photo.png"));
        assert!(!is_text_file("data.sqlite"));
    }
}
