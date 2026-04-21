//! Configuration builder for easy setup

use crate::client::MqttConfig;
use crate::connection::ReconnectConfig;
use crate::queue::{QueueConfig, OverflowPolicy};
use std::time::Duration;

/// Builder for MQTT client configuration
#[derive(Debug, Clone)]
pub struct MqttConfigBuilder {
    config: MqttConfig,
}

impl MqttConfigBuilder {
    /// Create a new configuration builder
    pub fn new(broker_url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            config: MqttConfig::new(broker_url, client_id),
        }
    }

    /// Set the keep alive interval
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.config.keep_alive = keep_alive;
        self
    }

    /// Set the connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.config.connection_timeout = timeout;
        self
    }

    /// Set maximum message size
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = size;
        self
    }

    /// Configure the offline queue
    pub fn queue_config(mut self, config: QueueConfig) -> Self {
        self.config.queue_config = config;
        self
    }

    /// Set maximum queue size
    pub fn max_queue_size(mut self, size: usize) -> Self {
        self.config.queue_config.max_size = size;
        self
    }

    /// Set queue overflow policy
    pub fn overflow_policy(mut self, policy: OverflowPolicy) -> Self {
        self.config.queue_config.overflow_policy = policy;
        self
    }

    /// Set maximum retry attempts for messages
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.queue_config.max_retries = retries;
        self
    }

    /// Configure reconnection behavior
    pub fn reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.config.reconnect_config = config;
        self
    }

    /// Set initial reconnection delay
    pub fn initial_reconnect_delay(mut self, delay: Duration) -> Self {
        self.config.reconnect_config.initial_delay = delay;
        self
    }

    /// Set maximum reconnection delay
    pub fn max_reconnect_delay(mut self, delay: Duration) -> Self {
        self.config.reconnect_config.max_delay = delay;
        self
    }

    /// Set exponential backoff multiplier
    pub fn backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.config.reconnect_config.backoff_multiplier = multiplier;
        self
    }

    /// Set maximum reconnection attempts (0 = unlimited)
    pub fn max_reconnect_attempts(mut self, attempts: u32) -> Self {
        self.config.reconnect_config.max_attempts = attempts;
        self
    }

    /// Enable or disable jitter in reconnection delays
    pub fn jitter(mut self, enabled: bool) -> Self {
        self.config.reconnect_config.jitter = enabled;
        self
    }

    /// Build the configuration
    pub fn build(self) -> MqttConfig {
        self.config
    }
}

/// Convenience methods for common configurations
impl MqttConfigBuilder {
    /// Configuration for high-reliability applications
    pub fn high_reliability(broker_url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self::new(broker_url, client_id)
            .max_queue_size(10_000)
            .max_retries(5)
            .initial_reconnect_delay(Duration::from_millis(100))
            .max_reconnect_delay(Duration::from_secs(30))
            .backoff_multiplier(1.5)
            .jitter(true)
            .overflow_policy(OverflowPolicy::DropOldest)
    }

    /// Configuration for low-resource environments
    pub fn low_resource(broker_url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self::new(broker_url, client_id)
            .max_queue_size(100)
            .max_retries(2)
            .initial_reconnect_delay(Duration::from_secs(1))
            .max_reconnect_delay(Duration::from_secs(120))
            .backoff_multiplier(2.0)
            .max_message_size(64 * 1024) // 64KB
            .overflow_policy(OverflowPolicy::DropOldest)
    }

    /// Configuration for high-throughput applications
    pub fn high_throughput(broker_url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self::new(broker_url, client_id)
            .max_queue_size(50_000)
            .max_retries(3)
            .keep_alive(Duration::from_secs(30))
            .initial_reconnect_delay(Duration::from_millis(50))
            .max_reconnect_delay(Duration::from_secs(10))
            .backoff_multiplier(1.2)
            .max_message_size(10 * 1024 * 1024) // 10MB
            .overflow_policy(OverflowPolicy::Error)
    }

    /// Configuration for testing/development
    pub fn development(broker_url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self::new(broker_url, client_id)
            .max_queue_size(1000)
            .max_retries(1)
            .initial_reconnect_delay(Duration::from_millis(100))
            .max_reconnect_delay(Duration::from_secs(5))
            .backoff_multiplier(1.5)
            .jitter(false) // Deterministic for testing
            .overflow_policy(OverflowPolicy::Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_builder() {
        let config = MqttConfigBuilder::new("mqtt://localhost:1883", "test_client")
            .keep_alive(Duration::from_secs(30))
            .max_queue_size(2000)
            .build();

        assert_eq!(config.broker_url, "mqtt://localhost:1883");
        assert_eq!(config.client_id, "test_client");
        assert_eq!(config.keep_alive, Duration::from_secs(30));
        assert_eq!(config.queue_config.max_size, 2000);
    }

    #[test]
    fn test_preset_configurations() {
        let high_rel = MqttConfigBuilder::high_reliability("mqtt://broker:1883", "device").build();
        assert_eq!(high_rel.queue_config.max_size, 10_000);

        let low_res = MqttConfigBuilder::low_resource("mqtt://broker:1883", "device").build();
        assert_eq!(low_res.queue_config.max_size, 100);

        let high_tp = MqttConfigBuilder::high_throughput("mqtt://broker:1883", "device").build();
        assert_eq!(high_tp.queue_config.max_size, 50_000);
    }
}
