use std::path::{Path, PathBuf};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerConfig {
    pub data_dir: PathBuf,
    #[serde(default = "default_sync_port")]
    pub sync_port: u16,
    #[serde(default = "default_admin_port")]
    pub admin_port: u16,
    #[serde(default)]
    pub admin_password_hash: String,
}

fn default_sync_port() -> u16 {
    27182
}

fn default_admin_port() -> u16 {
    27183
}

impl ServerConfig {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            sync_port: default_sync_port(),
            admin_port: default_admin_port(),
            admin_password_hash: String::new(),
        }
    }

    pub fn config_path(data_dir: &Path) -> PathBuf {
        data_dir.join("config.json")
    }

    pub fn save(&self) -> Result<(), std::io::Error> {
        let path = Self::config_path(&self.data_dir);
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        std::fs::write(&path, data)
    }

    pub fn load(data_dir: &Path) -> Result<Self, std::io::Error> {
        let path = Self::config_path(data_dir);
        let data = std::fs::read_to_string(&path)?;
        serde_json::from_str(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn defaults_are_27182_27183() {
        let cfg = ServerConfig::new(PathBuf::from("/data"));
        assert_eq!(cfg.sync_port, 27182);
        assert_eq!(cfg.admin_port, 27183);
        assert_eq!(cfg.data_dir, PathBuf::from("/data"));
        assert!(cfg.admin_password_hash.is_empty());
    }

    #[test]
    fn config_path_lives_inside_data_dir() {
        let p = ServerConfig::config_path(Path::new("/srv/x"));
        assert_eq!(p, PathBuf::from("/srv/x/config.json"));
    }

    #[test]
    fn save_then_load_roundtrip() {
        let dir = tempdir().unwrap();
        let cfg = ServerConfig::new(dir.path().to_path_buf());
        cfg.save().unwrap();

        let loaded = ServerConfig::load(dir.path()).unwrap();
        assert_eq!(loaded.data_dir, cfg.data_dir);
        assert_eq!(loaded.sync_port, cfg.sync_port);
        assert_eq!(loaded.admin_port, cfg.admin_port);
    }

    #[test]
    fn load_missing_file_returns_io_error() {
        let dir = tempdir().unwrap();
        let err = ServerConfig::load(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn load_corrupt_json_returns_invalid_data() {
        let dir = tempdir().unwrap();
        std::fs::write(ServerConfig::config_path(dir.path()), "not json").unwrap();
        let err = ServerConfig::load(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn load_uses_defaults_for_missing_optional_fields() {
        // Only data_dir is required. The rest fall back through serde defaults.
        let dir = tempdir().unwrap();
        let minimal = format!(
            "{{\"data_dir\": \"{}\"}}",
            dir.path().display().to_string().replace('\\', "/")
        );
        std::fs::write(ServerConfig::config_path(dir.path()), &minimal).unwrap();

        let cfg = ServerConfig::load(dir.path()).unwrap();
        assert_eq!(cfg.sync_port, 27182);
        assert_eq!(cfg.admin_port, 27183);
        assert!(cfg.admin_password_hash.is_empty());
    }
}
