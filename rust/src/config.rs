/*
 * Configuration for Pulsar Lite
 */

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Broker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server address
    #[serde(default = "default_addr")]
    pub addr: String,

    /// Database path
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// Default number of partitions for new topics
    /// 0 = non-partitioned (default)
    /// >0 = partitioned with N partitions
    #[serde(default)]
    pub default_partitions: usize,

    /// Log level
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_addr() -> String {
    "0.0.0.0:6650".to_string()
}

fn default_db_path() -> PathBuf {
    PathBuf::from("./pulsar-lite.db")
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            db_path: default_db_path(),
            default_partitions: 0,
            log_level: default_log_level(),
        }
    }
}

impl Config {
    /// Load configuration from file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path.as_ref())?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Load configuration from file or return default
    pub fn from_file_or_default<P: AsRef<Path>>(path: P) -> Self {
        Self::from_file(path).unwrap_or_else(|e| {
            log::warn!("Failed to load config file: {}, using defaults", e);
            Self::default()
        })
    }

    /// Save configuration to file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path.as_ref(), content)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.addr, "0.0.0.0:6650");
        assert_eq!(config.db_path, PathBuf::from("./pulsar-lite.db"));
        assert_eq!(config.default_partitions, 0);
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn test_config_serialization() {
        let config = Config {
            addr: "127.0.0.1:6650".to_string(),
            db_path: PathBuf::from("/tmp/test.db"),
            default_partitions: 3,
            log_level: "debug".to_string(),
        };

        let toml_str = toml::to_string(&config).unwrap();
        println!("Serialized config:\n{}", toml_str);

        let parsed: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.addr, config.addr);
        assert_eq!(parsed.db_path, config.db_path);
        assert_eq!(parsed.default_partitions, config.default_partitions);
        assert_eq!(parsed.log_level, config.log_level);
    }
}
