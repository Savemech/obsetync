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
