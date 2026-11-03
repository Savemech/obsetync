use std::path::{Path, PathBuf};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerConfig {
    pub data_dir: PathBuf,
}

impl ServerConfig {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
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
        let mut config: Self = serde_json::from_str(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // A restored/moved config must never redirect storage to its old copy.
        config.data_dir = data_dir.to_path_buf();
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
    }

    #[test]
    fn load_missing_file_returns_io_error() {
        let dir = tempdir().unwrap();
        let err = ServerConfig::load(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn restored_config_uses_the_explicit_data_directory() {
        let old = tempdir().unwrap();
        let restored = tempdir().unwrap();
        let cfg = ServerConfig::new(old.path().to_path_buf());
        cfg.save().unwrap();
        std::fs::copy(
            ServerConfig::config_path(old.path()),
            ServerConfig::config_path(restored.path()),
        )
        .unwrap();
        let loaded = ServerConfig::load(restored.path()).unwrap();
        assert_eq!(loaded.data_dir, restored.path());
    }

    #[test]
    fn load_corrupt_json_returns_invalid_data() {
        let dir = tempdir().unwrap();
        std::fs::write(ServerConfig::config_path(dir.path()), "not json").unwrap();
        let err = ServerConfig::load(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn old_unused_port_and_password_fields_remain_readable() {
        let dir = tempdir().unwrap();
        std::fs::write(ServerConfig::config_path(dir.path()),
            r#"{"data_dir":"/old","sync_port":28182,"admin_port":28183,"admin_password_hash":"unused"}"#).unwrap();
        let cfg = ServerConfig::load(dir.path()).unwrap();
        assert_eq!(cfg.data_dir, dir.path());
    }
}
