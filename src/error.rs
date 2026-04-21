//! Error types for mqtt-persist

use thiserror::Error;

/// Result type used throughout the library
pub type Result<T> = std::result::Result<T, MqttError>;

/// Error types that can occur during MQTT operations
#[derive(Error, Debug)]
pub enum MqttError {
    /// MQTT protocol or connection errors
    #[error("MQTT error: {0}")]
    Mqtt(#[from] rumqttc::ClientError),

    /// Connection errors (network, authentication, etc.)
    #[error("Connection error: {0}")]
    Connection(#[from] rumqttc::ConnectionError),

    /// URL parsing errors
    #[error("Invalid broker URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// Client is not running
    #[error("Client not started - call start() first")]
    ClientNotStarted,

    /// Client is already running
    #[error("Client already started")]
    ClientAlreadyStarted,

    /// Invalid client configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Queue is full and cannot accept more messages
    #[error("Message queue is full (max size: {max_size})")]
    QueueFull { max_size: usize },

    /// Message payload is too large
    #[error("Message payload too large: {size} bytes (max: {max_size})")]
    PayloadTooLarge { size: usize, max_size: usize },

    /// Invalid topic name
    #[error("Invalid topic: {0}")]
    InvalidTopic(String),

    /// Internal channel communication error
    #[error("Internal communication error")]
    ChannelError,

    /// Timeout waiting for operation to complete
    #[error("Operation timed out after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },
}

impl From<tokio::sync::mpsc::error::SendError<crate::queue::QueuedMessage>> for MqttError {
    fn from(_: tokio::sync::mpsc::error::SendError<crate::queue::QueuedMessage>) -> Self {
        MqttError::ChannelError
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for MqttError {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        MqttError::ChannelError
    }
}
