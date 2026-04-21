//! Offline message queue for storing messages when disconnected

use rumqttc::QoS;
use std::collections::VecDeque;
use std::time::Instant;
use tokio::sync::Mutex;

/// A message queued for later delivery
#[derive(Debug, Clone)]
pub struct QueuedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: QoS,
    pub timestamp: Instant,
    pub retry_count: u32,
}

impl QueuedMessage {
    pub fn new(topic: String, payload: Vec<u8>, qos: QoS) -> Self {
        Self {
            topic,
            payload,
            qos,
            timestamp: Instant::now(),
            retry_count: 0,
        }
    }

    /// Increment retry count for this message
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    /// Check if this message has exceeded max retries
    pub fn has_exceeded_retries(&self, max_retries: u32) -> bool {
        self.retry_count >= max_retries
    }
}

/// Configuration for the offline queue
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Maximum number of messages to store in queue
    pub max_size: usize,
    /// Maximum retry attempts per message
    pub max_retries: u32,
    /// What to do when queue is full
    pub overflow_policy: OverflowPolicy,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_size: 1000,
            max_retries: 3,
            overflow_policy: OverflowPolicy::DropOldest,
        }
    }
}

/// Policy for handling queue overflow
#[derive(Debug, Clone, PartialEq)]
pub enum OverflowPolicy {
    /// Drop the oldest message when queue is full
    DropOldest,
    /// Drop the newest message (reject the incoming message)
    DropNewest,
    /// Return an error when queue is full
    Error,
}

/// Thread-safe offline message queue
pub struct OfflineQueue {
    queue: Mutex<VecDeque<QueuedMessage>>,
    config: QueueConfig,
}

impl OfflineQueue {
    pub fn new(config: QueueConfig) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(config.max_size)),
            config,
        }
    }

    /// Add a message to the queue
    pub async fn enqueue(&self, message: QueuedMessage) -> crate::Result<()> {
        let mut queue = self.queue.lock().await;
        
        // Check if queue is at capacity
        if queue.len() >= self.config.max_size {
            match self.config.overflow_policy {
                OverflowPolicy::DropOldest => {
                    queue.pop_front();
                }
                OverflowPolicy::DropNewest => {
                    return Ok({}); // Drop the incoming message
                }
                OverflowPolicy::Error => {
                    return Err(crate::MqttError::QueueFull {
                        max_size: self.config.max_size,
                    });
                }
            }
        }

        queue.push_back(message);
        Ok(())
    }

    /// Remove and return the next message from the queue
    pub async fn dequeue(&self) -> Option<QueuedMessage> {
        let mut queue = self.queue.lock().await;
        queue.pop_front()
    }

    /// Peek at the next message without removing it
    pub async fn peek(&self) -> Option<QueuedMessage> {
        let queue = self.queue.lock().await;
        queue.front().cloned()
    }

    /// Get the current queue size
    pub async fn size(&self) -> usize {
        let queue = self.queue.lock().await;
        queue.len()
    }

    /// Check if queue is empty
    pub async fn is_empty(&self) -> bool {
        let queue = self.queue.lock().await;
        queue.is_empty()
    }

    /// Clear all messages from the queue
    pub async fn clear(&self) {
        let mut queue = self.queue.lock().await;
        queue.clear();
    }

    /// Get queue statistics
    pub async fn stats(&self) -> QueueStats {
        let queue = self.queue.lock().await;
        let oldest_timestamp = queue.front().map(|msg| msg.timestamp);
        let newest_timestamp = queue.back().map(|msg| msg.timestamp);
        
        QueueStats {
            size: queue.len(),
            capacity: self.config.max_size,
            oldest_message_age: oldest_timestamp.map(|ts| ts.elapsed()),
            newest_message_age: newest_timestamp.map(|ts| ts.elapsed()),
        }
    }

    /// Drain all messages for processing
    pub async fn drain(&self) -> Vec<QueuedMessage> {
        let mut queue = self.queue.lock().await;
        queue.drain(..).collect()
    }
}

/// Statistics about the offline queue
#[derive(Debug, Clone)]
pub struct QueueStats {
    pub size: usize,
    pub capacity: usize,
    pub oldest_message_age: Option<std::time::Duration>,
    pub newest_message_age: Option<std::time::Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::QoS;

    #[tokio::test]
    async fn test_queue_basic_operations() {
        let config = QueueConfig::default();
        let queue = OfflineQueue::new(config);

        assert!(queue.is_empty().await);
        assert_eq!(queue.size().await, 0);

        let msg = QueuedMessage::new("test/topic".to_string(), b"payload".to_vec(), QoS::AtMostOnce);
        queue.enqueue(msg.clone()).await.unwrap();

        assert!(!queue.is_empty().await);
        assert_eq!(queue.size().await, 1);

        let dequeued = queue.dequeue().await.unwrap();
        assert_eq!(dequeued.topic, msg.topic);
        assert_eq!(dequeued.payload, msg.payload);

        assert!(queue.is_empty().await);
    }

    #[tokio::test]
    async fn test_queue_overflow_drop_oldest() {
        let config = QueueConfig {
            max_size: 2,
            overflow_policy: OverflowPolicy::DropOldest,
            ..Default::default()
        };
        let queue = OfflineQueue::new(config);

        let msg1 = QueuedMessage::new("test/1".to_string(), b"1".to_vec(), QoS::AtMostOnce);
        let msg2 = QueuedMessage::new("test/2".to_string(), b"2".to_vec(), QoS::AtMostOnce);
        let msg3 = QueuedMessage::new("test/3".to_string(), b"3".to_vec(), QoS::AtMostOnce);

        queue.enqueue(msg1).await.unwrap();
        queue.enqueue(msg2).await.unwrap();
        queue.enqueue(msg3).await.unwrap(); // Should drop msg1

        assert_eq!(queue.size().await, 2);
        
        let first = queue.dequeue().await.unwrap();
        assert_eq!(first.topic, "test/2"); // msg1 was dropped
    }
}
