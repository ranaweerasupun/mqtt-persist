//! Main MQTT client implementation with offline queuing

use crate::connection::{ConnectionManager, ReconnectConfig};
use crate::error::{MqttError, Result};
use crate::queue::{OfflineQueue, QueueConfig, QueuedMessage};
use crate::ClientStats;

use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use url::Url;

/// Configuration for the MQTT client
#[derive(Debug, Clone)]
pub struct MqttConfig {
    /// Broker URL (e.g., "mqtt://localhost:1883")
    pub broker_url: String,
    /// Client ID for MQTT connection
    pub client_id: String,
    /// Keep alive interval
    pub keep_alive: Duration,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Queue configuration
    pub queue_config: QueueConfig,
    /// Reconnection configuration
    pub reconnect_config: ReconnectConfig,
    /// Maximum message size
    pub max_message_size: usize,
}

impl MqttConfig {
    pub fn new(broker_url: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            client_id: client_id.into(),
            keep_alive: Duration::from_secs(60),
            connection_timeout: Duration::from_secs(10),
            queue_config: QueueConfig::default(),
            reconnect_config: ReconnectConfig::default(),
            max_message_size: 1024 * 1024, // 1MB
        }
    }
}

/// Main MQTT client with offline queuing capability
pub struct MqttClient {
    config: MqttConfig,
    offline_queue: Arc<OfflineQueue>,
    connection_manager: Arc<Mutex<ConnectionManager>>,
    
    // Channels for internal communication
    publish_tx: mpsc::UnboundedSender<PublishRequest>,
    publish_rx: Option<mpsc::UnboundedReceiver<PublishRequest>>,
    shutdown_tx: broadcast::Sender<()>,
    
    // Client state
    stats: Arc<Mutex<ClientStats>>,
    running: Arc<Mutex<bool>>,
    
    // Background task handle
    task_handle: Option<JoinHandle<()>>,
}

/// Internal message for publish requests
#[derive(Debug)]
struct PublishRequest {
    topic: String,
    payload: Vec<u8>,
    qos: QoS,
    response_tx: Option<tokio::sync::oneshot::Sender<Result<()>>>,
}

impl From<tokio::sync::mpsc::error::SendError<PublishRequest>> for MqttError {
    fn from(_: tokio::sync::mpsc::error::SendError<PublishRequest>) -> Self {
        MqttError::ChannelError
    }
}

impl MqttClient {
    /// Create a new MQTT client
    pub async fn new(broker_url: impl Into<String>, client_id: impl Into<String>) -> Result<Self> {
        let config = MqttConfig::new(broker_url, client_id);
        Self::with_config(config).await
    }

    /// Create a new MQTT client with custom configuration
    pub async fn with_config(config: MqttConfig) -> Result<Self> {
        // Validate broker URL
        let _url = Url::parse(&config.broker_url)?;

        let offline_queue = Arc::new(OfflineQueue::new(config.queue_config.clone()));
        let connection_manager = Arc::new(Mutex::new(ConnectionManager::new(config.reconnect_config.clone())));
        let (publish_tx, publish_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, _) = broadcast::channel(1);
        let stats = Arc::new(Mutex::new(ClientStats::default()));

        Ok(Self {
            config,
            offline_queue,
            connection_manager,
            publish_tx,
            publish_rx: Some(publish_rx),
            shutdown_tx,
            stats,
            running: Arc::new(Mutex::new(false)),
            task_handle: None,
        })
    }

    /// Start the client (begins connection attempts and background processing)
    pub async fn start(&mut self) -> Result<()> {
        let mut running = self.running.lock().await;
        if *running {
            return Err(MqttError::ClientAlreadyStarted);
        }

        // Parse broker URL
        let url = Url::parse(&self.config.broker_url)?;
        let host = url.host_str().ok_or_else(|| {
            MqttError::InvalidConfig("Broker URL must have a host".to_string())
        })?;
        let port = url.port().unwrap_or(1883);

        // Create MQTT options
        let mut mqtt_options = MqttOptions::new(&self.config.client_id, host, port);
        mqtt_options.set_keep_alive(self.config.keep_alive);
        mqtt_options.set_clean_session(true);

        // Create client and event loop
        let (client, eventloop) = AsyncClient::new(mqtt_options, 10);

        // Take the receiver (can only start once)
        let publish_rx = self.publish_rx.take()
            .ok_or_else(|| MqttError::ClientAlreadyStarted)?;

        // Start background task
        let task_handle = self.start_background_task(
            client,
            eventloop,
            publish_rx,
        ).await;

        self.task_handle = Some(task_handle);
        *running = true;

        Ok(())
    }

    /// Stop the client
    pub async fn stop(&mut self) -> Result<()> {
        let mut running = self.running.lock().await;
        if !*running {
            return Ok(());
        }

        // Send shutdown signal
        let _ = self.shutdown_tx.send(());

        // Wait for background task to finish
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }

        *running = false;
        Ok(())
    }

    /// Publish a message
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        qos: QoS,
    ) -> Result<()> {
        let topic = topic.into();
        let payload = payload.into();

        // Validate inputs
        if topic.is_empty() {
            return Err(MqttError::InvalidTopic("Topic cannot be empty".to_string()));
        }
        
        if payload.len() > self.config.max_message_size {
            return Err(MqttError::PayloadTooLarge {
                size: payload.len(),
                max_size: self.config.max_message_size,
            });
        }

        let (tx, rx) = tokio::sync::oneshot::channel();
        let request = PublishRequest {
            topic,
            payload,
            qos,
            response_tx: Some(tx),
        };

        self.publish_tx.send(request)?;
        
        // Wait for result with timeout
        timeout(self.config.connection_timeout, rx).await
            .map_err(|_| MqttError::Timeout {
                timeout_ms: self.config.connection_timeout.as_millis() as u64,
            })??
    }

    /// Publish a message without waiting for acknowledgment
    pub async fn publish_async(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        qos: QoS,
    ) -> Result<()> {
        let request = PublishRequest {
            topic: topic.into(),
            payload: payload.into(),
            qos,
            response_tx: None,
        };

        self.publish_tx.send(request)?;
        Ok(())
    }

    /// Get current client statistics
    pub async fn stats(&self) -> ClientStats {
        let stats = self.stats.lock().await;
        stats.clone()
    }

    /// Check if client is currently connected
    pub async fn is_connected(&self) -> bool {
        let manager = self.connection_manager.lock().await;
        manager.is_connected()
    }

    /// Start the background task that handles MQTT events and message processing
    async fn start_background_task(
        &self,
        client: AsyncClient,
        mut eventloop: EventLoop,
        mut publish_rx: mpsc::UnboundedReceiver<PublishRequest>,
    ) -> JoinHandle<()> {
        let offline_queue = Arc::clone(&self.offline_queue);
        let connection_manager = Arc::clone(&self.connection_manager);
        let stats = Arc::clone(&self.stats);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut drain_interval = tokio::time::interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    // Handle shutdown signal
                    _ = shutdown_rx.recv() => {
                        break;
                    }

                    // Handle MQTT events
                    event = eventloop.poll() => {
                        match event {
                            Ok(Event::Incoming(packet)) => {
                                Self::handle_incoming_packet(packet, &connection_manager, &stats).await;
                            }
                            Ok(Event::Outgoing(_)) => {
                                // Message sent successfully
                                let mut stats = stats.lock().await;
                                stats.messages_sent += 1;
                            }
                            Err(rumqttc::ConnectionError::Io(_)) => {
                                // Connection lost
                                {
                                    let mut manager = connection_manager.lock().await;
                                    manager.disconnected();
                                    let mut stats = stats.lock().await;
                                    stats.connected = false;
                                    stats.reconnection_attempts = manager.stats().current_attempt;
                                }
                                
                                // Attempt reconnection
                                Self::handle_reconnection(&connection_manager).await;
                            }
                            Err(e) => {
                                eprintln!("MQTT error: {}", e);
                            }
                        }
                    }

                    // Handle publish requests
                    request = publish_rx.recv() => {
                        if let Some(request) = request {
                            Self::handle_publish_request(
                                request,
                                &client,
                                &offline_queue,
                                &connection_manager,
                                &stats,
                            ).await;
                        }
                    }

                    // Periodically drain offline queue
                    _ = drain_interval.tick() => {
                        let is_connected = {
                            let manager = connection_manager.lock().await;
                            manager.is_connected()
                        };
                        
                        if is_connected && !offline_queue.is_empty().await {
                            Self::drain_offline_queue(&client, &offline_queue, &stats).await;
                        }
                    }
                }
            }
        })
    }

    /// Handle incoming MQTT packets
    async fn handle_incoming_packet(
        packet: Packet,
        connection_manager: &Arc<Mutex<ConnectionManager>>,
        stats: &Arc<Mutex<ClientStats>>,
    ) {
        match packet {
            Packet::ConnAck(_) => {
                let mut manager = connection_manager.lock().await;
                manager.connected();
                let mut stats = stats.lock().await;
                stats.connected = true;
                stats.reconnection_attempts = 0;
                println!("Connected to MQTT broker");
            }
            Packet::Disconnect => {
                let mut manager = connection_manager.lock().await;
                manager.disconnected();
                let mut stats = stats.lock().await;
                stats.connected = false;
                println!("Disconnected from MQTT broker");
            }
            _ => {
                // Handle other packet types as needed
            }
        }
    }

    /// Handle reconnection logic
    async fn handle_reconnection(connection_manager: &Arc<Mutex<ConnectionManager>>) {
        let should_reconnect = {
            let mut manager = connection_manager.lock().await;
            if manager.should_reconnect() {
                manager.reconnecting();
                true
            } else {
                false
            }
        };

        if should_reconnect {
            {
                let manager = connection_manager.lock().await;
                manager.wait_for_reconnect().await;
            }
            println!("Attempting to reconnect...");
        }
    }

    /// Handle a publish request
    async fn handle_publish_request(
        request: PublishRequest,
        client: &AsyncClient,
        offline_queue: &Arc<OfflineQueue>,
        connection_manager: &Arc<Mutex<ConnectionManager>>,
        stats: &Arc<Mutex<ClientStats>>,
    ) {
        let is_connected = {
            let manager = connection_manager.lock().await;
            manager.is_connected()
        };

        let result = if is_connected {
            // Try to publish directly
            client.publish(&request.topic, request.qos, false, request.payload.clone()).await
                .map_err(|e| e.into())
        } else {
            // Queue for later delivery
            let queued_message = QueuedMessage::new(request.topic.clone(), request.payload, request.qos);
            let queue_result = offline_queue.enqueue(queued_message).await;
            
            if queue_result.is_ok() {
                let mut stats = stats.lock().await;
                stats.messages_queued += 1;
                stats.queue_size = offline_queue.size().await;
            }
            
            queue_result
        };

        // Send response if requested
        if let Some(response_tx) = request.response_tx {
            let _ = response_tx.send(result);
        }
    }

    /// Drain messages from the offline queue
    async fn drain_offline_queue(
        client: &AsyncClient,
        offline_queue: &Arc<OfflineQueue>,
        stats: &Arc<Mutex<ClientStats>>,
    ) {
        const MAX_DRAIN_PER_TICK: usize = 10; // Prevent overwhelming the broker

        for _ in 0..MAX_DRAIN_PER_TICK {
            if let Some(message) = offline_queue.dequeue().await {
                let topic = &message.topic;
                let qos = message.qos;
                let payload = message.payload.clone();
                
                match client.publish(topic, qos, false, payload).await {
                    Ok(_) => {
                        let mut stats = stats.lock().await;
                        stats.queue_size = offline_queue.size().await;
                        println!("Delivered queued message to {}", topic);
                    }
                    Err(e) => {
                        // Re-queue the message if publish failed
                        if let Err(queue_err) = offline_queue.enqueue(message).await {
                            eprintln!("Failed to re-queue message: {}", queue_err);
                        }
                        eprintln!("Failed to publish queued message: {}", e);
                        break; // Stop draining on error
                    }
                }
            } else {
                break; // No more messages
            }
        }
    }
}

impl Drop for MqttClient {
    fn drop(&mut self) {
        // Best effort cleanup
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }
    }
}
