/*
 * Custom Error Types
 * Provides type-safe error handling for Pulsar Lite
 */

use std::fmt;

/// Result type alias for Pulsar Lite operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for Pulsar Lite
#[derive(Debug)]
pub enum Error {
    /// Protocol-related errors (encoding/decoding)
    Protocol(String),

    /// Storage-related errors
    Storage(String),

    /// Handler-related errors
    Handler(String),

    /// IO errors
    Io(std::io::Error),

    /// Invalid command or state
    InvalidState(String),

    /// Consumer not found
    ConsumerNotFound(u64),

    /// Producer not found
    ProducerNotFound(u64),

    /// Topic not found
    TopicNotFound(String),

    /// Subscription not found
    SubscriptionNotFound(String, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            Error::Storage(msg) => write!(f, "Storage error: {}", msg),
            Error::Handler(msg) => write!(f, "Handler error: {}", msg),
            Error::Io(err) => write!(f, "IO error: {}", err),
            Error::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            Error::ConsumerNotFound(id) => write!(f, "Consumer not found: {}", id),
            Error::ProducerNotFound(id) => write!(f, "Producer not found: {}", id),
            Error::TopicNotFound(topic) => write!(f, "Topic not found: {}", topic),
            Error::SubscriptionNotFound(topic, sub) => {
                write!(f, "Subscription '{}' not found for topic '{}'", sub, topic)
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::InvalidState(msg.to_string())
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::InvalidState(msg)
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Error::Handler(err.to_string())
    }
}
