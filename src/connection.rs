//! Connection management and reconnection logic

use std::time::Duration;
use tokio::time::{sleep, Instant};

/// Connection state for the MQTT client
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    /// Client is disconnected
    Disconnected,
    /// Client is attempting to connect
    Connecting,
    /// Client is connected to broker
    Connected,
    /// Client is reconnecting after connection loss
    Reconnecting { attempt: u32 },
}

impl ConnectionState {
    pub fn is_connected(&self) -> bool {
        matches!(self, ConnectionState::Connected)
    }

    pub fn is_connecting(&self) -> bool {
        matches!(self, ConnectionState::Connecting | ConnectionState::Reconnecting { .. })
    }
}

/// Configuration for reconnection behavior
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial delay before first reconnection attempt
    pub initial_delay: Duration,
    /// Maximum delay between reconnection attempts
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Maximum number of consecutive reconnection attempts (0 = unlimited)
    pub max_attempts: u32,
    /// Jitter to add to delays to avoid thundering herd
    pub jitter: bool,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            backoff_multiplier: 2.0,
            max_attempts: 0, // Unlimited
            jitter: true,
        }
    }
}

/// Manages connection state and calculates reconnection delays
pub struct ConnectionManager {
    state: ConnectionState,
    config: ReconnectConfig,
    current_attempt: u32,
    last_connection_time: Option<Instant>,
    last_disconnection_time: Option<Instant>,
    total_reconnections: u32,
}

impl ConnectionManager {
    pub fn new(config: ReconnectConfig) -> Self {
        Self {
            state: ConnectionState::Disconnected,
            config,
            current_attempt: 0,
            last_connection_time: None,
            last_disconnection_time: None,
            total_reconnections: 0,
        }
    }

    /// Get current connection state
    pub fn state(&self) -> &ConnectionState {
        &self.state
    }

    /// Check if currently connected
    pub fn is_connected(&self) -> bool {
        self.state.is_connected()
    }

    /// Mark connection as established
    pub fn connected(&mut self) {
        self.state = ConnectionState::Connected;
        self.current_attempt = 0;
        self.last_connection_time = Some(Instant::now());
        
        if self.last_disconnection_time.is_some() {
            self.total_reconnections += 1;
        }
    }

    /// Mark connection as lost
    pub fn disconnected(&mut self) {
        self.state = ConnectionState::Disconnected;
        self.last_disconnection_time = Some(Instant::now());
    }

    /// Start a connection attempt
    pub fn connecting(&mut self) {
        self.state = ConnectionState::Connecting;
    }

    /// Start a reconnection attempt
    pub fn reconnecting(&mut self) {
        self.current_attempt += 1;
        self.state = ConnectionState::Reconnecting {
            attempt: self.current_attempt,
        };
    }

    /// Check if should attempt reconnection
    pub fn should_reconnect(&self) -> bool {
        match self.state {
            ConnectionState::Connected | ConnectionState::Connecting => false,
            ConnectionState::Disconnected | ConnectionState::Reconnecting { .. } => {
                self.config.max_attempts == 0 || self.current_attempt < self.config.max_attempts
            }
        }
    }

    /// Calculate delay before next reconnection attempt
    pub fn next_reconnect_delay(&self) -> Duration {
        if self.current_attempt == 0 {
            return self.config.initial_delay;
        }

        let base_delay = self.config.initial_delay.as_millis() as f64
            * self.config.backoff_multiplier.powi(self.current_attempt as i32 - 1);
        
        let delay_ms = base_delay.min(self.config.max_delay.as_millis() as f64) as u64;
        let mut delay = Duration::from_millis(delay_ms);

        // Add jitter to prevent thundering herd effect
        if self.config.jitter {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            
            let mut hasher = DefaultHasher::new();
            self.current_attempt.hash(&mut hasher);
            let jitter_factor = (hasher.finish() % 100) as f64 / 100.0; // 0.0-0.99
            
            let jitter_amount = delay.as_millis() as f64 * 0.1 * jitter_factor; // Up to 10% jitter
            delay = Duration::from_millis(delay.as_millis() as u64 + jitter_amount as u64);
        }

        delay
    }

    /// Wait for next reconnection attempt
    pub async fn wait_for_reconnect(&self) {
        let delay = self.next_reconnect_delay();
        sleep(delay).await;
    }

    /// Reset connection state (for manual connect)
    pub fn reset(&mut self) {
        self.state = ConnectionState::Disconnected;
        self.current_attempt = 0;
    }

    /// Get connection statistics
    pub fn stats(&self) -> ConnectionStats {
        ConnectionStats {
            state: self.state.clone(),
            current_attempt: self.current_attempt,
            total_reconnections: self.total_reconnections,
            connection_duration: self.last_connection_time.map(|t| t.elapsed()),
            disconnection_duration: self.last_disconnection_time.map(|t| t.elapsed()),
        }
    }
}

/// Statistics about connection state
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub state: ConnectionState,
    pub current_attempt: u32,
    pub total_reconnections: u32,
    pub connection_duration: Option<Duration>,
    pub disconnection_duration: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state_transitions() {
        let config = ReconnectConfig::default();
        let mut manager = ConnectionManager::new(config);

        assert_eq!(manager.state(), &ConnectionState::Disconnected);
        assert!(!manager.is_connected());

        manager.connecting();
        assert_eq!(manager.state(), &ConnectionState::Connecting);

        manager.connected();
        assert_eq!(manager.state(), &ConnectionState::Connected);
        assert!(manager.is_connected());

        manager.disconnected();
        assert_eq!(manager.state(), &ConnectionState::Disconnected);
        assert!(!manager.is_connected());
    }

    #[test]
    fn test_reconnection_delays() {
        let config = ReconnectConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            max_attempts: 5,
            jitter: false,
        };
        let mut manager = ConnectionManager::new(config);

        // First attempt
        manager.reconnecting();
        let delay1 = manager.next_reconnect_delay();
        assert_eq!(delay1, Duration::from_millis(100));

        // Second attempt
        manager.reconnecting();
        let delay2 = manager.next_reconnect_delay();
        assert_eq!(delay2, Duration::from_millis(200));

        // Third attempt
        manager.reconnecting();
        let delay3 = manager.next_reconnect_delay();
        assert_eq!(delay3, Duration::from_millis(400));
    }

    #[test]
    fn test_max_attempts() {
        let config = ReconnectConfig {
            max_attempts: 3,
            ..Default::default()
        };
        let mut manager = ConnectionManager::new(config);

        assert!(manager.should_reconnect());

        for _ in 0..3 {
            manager.reconnecting();
            assert!(manager.should_reconnect() || manager.current_attempt == 3);
        }

        // Should not reconnect after max attempts
        assert!(!manager.should_reconnect());
    }
}
