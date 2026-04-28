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

    /// Keep-alive interval in seconds
    #[serde(default = "default_keep_alive_interval_secs")]
    pub keep_alive_interval_secs: u64,

    /// Handshake timeout in seconds
    #[serde(default = "default_handshake_timeout_secs")]
    pub handshake_timeout_secs: u64,

    /// Timeout for a one-shot connection liveness check in seconds
    #[serde(default = "default_connection_liveness_check_timeout_secs")]
    pub connection_liveness_check_timeout_secs: u64,

    /// Maximum number of broker connections (0 = unlimited)
    #[serde(default)]
    pub max_connections: usize,

    /// Maximum number of connections per IP address (0 = unlimited)
    #[serde(default)]
    pub max_connections_per_ip: usize,

    /// Maximum concurrent non-persistent messages per connection.
    #[serde(default = "default_max_concurrent_non_persistent_messages_per_connection")]
    pub max_concurrent_non_persistent_messages_per_connection: usize,

    /// Maximum pending publish requests per connection before TCP throttle activates.
    /// When in-flight count reaches this limit, the broker stops reading from the
    /// TCP connection, causing TCP backpressure to the producer client.
    /// Reading resumes when pending drops to 50% of this limit (hysteresis).
    /// Maps to Pulsar's maxPendingPublishRequestsPerConnection.
    /// Default: 1000
    #[serde(default = "default_max_pending_publish_requests_per_connection")]
    pub max_pending_publish_requests_per_connection: usize,

    /// Maximum allowed message size in bytes.
    #[serde(default = "default_max_message_size_bytes")]
    pub max_message_size_bytes: usize,

    /// Topic-level publish rate limit in messages per second (0 = unlimited).
    #[serde(default)]
    pub publish_rate_messages_per_sec: u64,

    /// Topic-level publish rate limit in bytes per second (0 = unlimited).
    #[serde(default)]
    pub publish_rate_bytes_per_sec: u64,

    /// Pulsar-style connection write-buffer high watermark in bytes.
    /// Mirrors Netty's WRITE_BUFFER_HIGH_WATER_MARK semantics.
    #[serde(default = "default_pulsar_channel_write_buffer_high_water_mark_bytes")]
    pub pulsar_channel_write_buffer_high_water_mark_bytes: usize,

    /// Pulsar-style connection write-buffer low watermark in bytes.
    /// Mirrors Netty's WRITE_BUFFER_LOW_WATER_MARK hysteresis semantics.
    #[serde(default = "default_pulsar_channel_write_buffer_low_water_mark_bytes")]
    pub pulsar_channel_write_buffer_low_water_mark_bytes: usize,
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

fn default_keep_alive_interval_secs() -> u64 {
    30
}

fn default_handshake_timeout_secs() -> u64 {
    30
}

fn default_connection_liveness_check_timeout_secs() -> u64 {
    10
}

fn default_max_concurrent_non_persistent_messages_per_connection() -> usize {
    1000
}

fn default_max_pending_publish_requests_per_connection() -> usize {
    1000
}

fn default_max_message_size_bytes() -> usize {
    5 * 1024 * 1024
}

fn default_pulsar_channel_write_buffer_high_water_mark_bytes() -> usize {
    64 * 1024
}

fn default_pulsar_channel_write_buffer_low_water_mark_bytes() -> usize {
    32 * 1024
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            db_path: default_db_path(),
            default_partitions: 0,
            log_level: default_log_level(),
            keep_alive_interval_secs: default_keep_alive_interval_secs(),
            handshake_timeout_secs: default_handshake_timeout_secs(),
            connection_liveness_check_timeout_secs: default_connection_liveness_check_timeout_secs(
            ),
            max_connections: 0,
            max_connections_per_ip: 0,
            max_concurrent_non_persistent_messages_per_connection:
                default_max_concurrent_non_persistent_messages_per_connection(),
            max_pending_publish_requests_per_connection:
                default_max_pending_publish_requests_per_connection(),
            max_message_size_bytes: default_max_message_size_bytes(),
            publish_rate_messages_per_sec: 0,
            publish_rate_bytes_per_sec: 0,
            pulsar_channel_write_buffer_high_water_mark_bytes:
                default_pulsar_channel_write_buffer_high_water_mark_bytes(),
            pulsar_channel_write_buffer_low_water_mark_bytes:
                default_pulsar_channel_write_buffer_low_water_mark_bytes(),
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
        assert_eq!(config.keep_alive_interval_secs, 30);
        assert_eq!(config.handshake_timeout_secs, 30);
        assert_eq!(config.connection_liveness_check_timeout_secs, 10);
        assert_eq!(config.max_connections, 0);
        assert_eq!(config.max_connections_per_ip, 0);
        assert_eq!(
            config.max_concurrent_non_persistent_messages_per_connection,
            1000
        );
        assert_eq!(config.max_pending_publish_requests_per_connection, 1000);
        assert_eq!(config.max_message_size_bytes, 5 * 1024 * 1024);
        assert_eq!(config.publish_rate_messages_per_sec, 0);
        assert_eq!(config.publish_rate_bytes_per_sec, 0);
        assert_eq!(
            config.pulsar_channel_write_buffer_high_water_mark_bytes,
            64 * 1024
        );
        assert_eq!(
            config.pulsar_channel_write_buffer_low_water_mark_bytes,
            32 * 1024
        );
    }

    #[test]
    fn test_config_serialization() {
        let config = Config {
            addr: "127.0.0.1:6650".to_string(),
            db_path: PathBuf::from("/tmp/test.db"),
            default_partitions: 3,
            log_level: "debug".to_string(),
            keep_alive_interval_secs: 15,
            handshake_timeout_secs: 10,
            connection_liveness_check_timeout_secs: 5,
            max_connections: 100,
            max_connections_per_ip: 8,
            max_concurrent_non_persistent_messages_per_connection: 10000,
            max_pending_publish_requests_per_connection: 2000,
            max_message_size_bytes: 2048,
            publish_rate_messages_per_sec: 123,
            publish_rate_bytes_per_sec: 456,
            pulsar_channel_write_buffer_high_water_mark_bytes: 96 * 1024,
            pulsar_channel_write_buffer_low_water_mark_bytes: 48 * 1024,
        };

        let toml_str = toml::to_string(&config).unwrap();
        println!("Serialized config:\n{}", toml_str);

        let parsed: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(parsed.addr, config.addr);
        assert_eq!(parsed.db_path, config.db_path);
        assert_eq!(parsed.default_partitions, config.default_partitions);
        assert_eq!(parsed.log_level, config.log_level);
        assert_eq!(
            parsed.keep_alive_interval_secs,
            config.keep_alive_interval_secs
        );
        assert_eq!(parsed.handshake_timeout_secs, config.handshake_timeout_secs);
        assert_eq!(
            parsed.connection_liveness_check_timeout_secs,
            config.connection_liveness_check_timeout_secs
        );
        assert_eq!(parsed.max_connections, config.max_connections);
        assert_eq!(parsed.max_connections_per_ip, config.max_connections_per_ip);
        assert_eq!(
            parsed.max_concurrent_non_persistent_messages_per_connection,
            config.max_concurrent_non_persistent_messages_per_connection
        );
        assert_eq!(
            parsed.max_pending_publish_requests_per_connection,
            config.max_pending_publish_requests_per_connection
        );
        assert_eq!(parsed.max_message_size_bytes, config.max_message_size_bytes);
        assert_eq!(
            parsed.publish_rate_messages_per_sec,
            config.publish_rate_messages_per_sec
        );
        assert_eq!(
            parsed.publish_rate_bytes_per_sec,
            config.publish_rate_bytes_per_sec
        );
        assert_eq!(
            parsed.pulsar_channel_write_buffer_high_water_mark_bytes,
            config.pulsar_channel_write_buffer_high_water_mark_bytes
        );
        assert_eq!(
            parsed.pulsar_channel_write_buffer_low_water_mark_bytes,
            config.pulsar_channel_write_buffer_low_water_mark_bytes
        );
    }
}
