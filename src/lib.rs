//! # mqtt-persist
//!
//! A resilient MQTT client with offline message queuing and automatic reconnection.
//!
//! ## Features
//!
//! - **Offline message queuing**: Messages are queued in memory when disconnected
//! - **Automatic reconnection**: Client reconnects automatically with configurable backoff
//! - **Simple async API**: Built on tokio for high performance
//! - **QoS support**: Supports QoS 0 and 1 message delivery
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use mqtt_persist::{MqttClient, QoS, MqttError};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), MqttError> {
//!     let client = MqttClient::new("mqtt://localhost:1883", "my_device").await?;
//!     
//!     // Publish works whether online or offline
//!     client.publish("sensors/temperature", b"23.5", QoS::AtLeastOnce).await?;
//!     
//!     // Get basic stats
//!     let stats = client.stats().await;
//!     println!("Queued messages: {}", stats.queue_size);
//!     
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod queue;
pub mod connection;
pub mod error;
pub mod config;

// Re-export main types for convenience
pub use client::{MqttClient, MqttConfig};
pub use config::MqttConfigBuilder;
pub use error::{MqttError, Result};
pub use queue::{QueueConfig, OverflowPolicy};

// Re-export rumqttc types that users need
pub use rumqttc::QoS;

/// Statistics about the client state
#[derive(Debug, Clone)]
pub struct ClientStats {
    /// Number of messages currently queued for delivery
    pub queue_size: usize,
    /// Total messages successfully sent since startup
    pub messages_sent: u64,
    /// Total messages queued since startup
    pub messages_queued: u64,
    /// Current connection state
    pub connected: bool,
    /// Number of reconnection attempts
    pub reconnection_attempts: u32,
}

impl Default for ClientStats {
    fn default() -> Self {
        Self {
            queue_size: 0,
            messages_sent: 0,
            messages_queued: 0,
            connected: false,
            reconnection_attempts: 0,
        }
    }
}
