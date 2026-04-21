//! Integration tests for mqtt-persist

use mqtt_persist::{MqttClient, QoS, MqttError};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_client_creation_and_start() -> Result<(), MqttError> {
    let mut client = MqttClient::new("mqtt://localhost:1883", "test_client").await?;
    
    // Should be able to start
    client.start().await?;
    
    // Should not be able to start again
    assert!(client.start().await.is_err());
    
    // Should be able to stop
    client.stop().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_offline_queuing() -> Result<(), MqttError> {
    // Use an invalid broker to ensure we're offline
    let mut client = MqttClient::new("mqtt://invalid-broker:1883", "test_offline").await?;
    
    client.start().await?;
    
    // Give it a moment to fail to connect
    sleep(Duration::from_millis(100)).await;
    
    // Should not be connected
    assert!(!client.is_connected().await);
    
    // Publish some messages (should be queued)
    for i in 0..5 {
        let payload = format!("message_{}", i);
        client.publish_async("test/topic", payload.as_bytes(), QoS::AtMostOnce).await?;
    }
    
    // Check that messages were queued
    let stats = client.stats().await;
    assert_eq!(stats.messages_queued, 5);
    assert_eq!(stats.queue_size, 5);
    assert_eq!(stats.messages_sent, 0);
    
    client.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_invalid_broker_url() {
    let result = MqttClient::new("not-a-url", "test").await;
    assert!(result.is_err());
    
    let result = MqttClient::new("http://example.com", "test").await;
    assert!(result.is_ok()); // HTTP URLs are valid, just might not work for MQTT
}

#[tokio::test]
async fn test_publish_validation() -> Result<(), MqttError> {
    let mut client = MqttClient::new("mqtt://localhost:1883", "test_validation").await?;
    client.start().await?;
    
    // Test empty topic
    let result = client.publish("", b"payload", QoS::AtMostOnce).await;
    assert!(matches!(result, Err(MqttError::InvalidTopic(_))));
    
    client.stop().await?;
    Ok(())
}

#[tokio::test] 
async fn test_client_stats() -> Result<(), MqttError> {
    let mut client = MqttClient::new("mqtt://localhost:1883", "test_stats").await?;
    client.start().await?;
    
    let initial_stats = client.stats().await;
    assert_eq!(initial_stats.messages_sent, 0);
    assert_eq!(initial_stats.messages_queued, 0);
    assert_eq!(initial_stats.queue_size, 0);
    assert_eq!(initial_stats.reconnection_attempts, 0);
    
    client.stop().await?;
    Ok(())
}

#[tokio::test]
async fn test_multiple_clients() -> Result<(), MqttError> {
    let mut client1 = MqttClient::new("mqtt://localhost:1883", "test_multi_1").await?;
    let mut client2 = MqttClient::new("mqtt://localhost:1883", "test_multi_2").await?;
    
    client1.start().await?;
    client2.start().await?;
    
    // Both should be able to operate independently
    let stats1 = client1.stats().await;
    let stats2 = client2.stats().await;
    
    // They should have independent state
    assert_eq!(stats1.messages_sent, 0);
    assert_eq!(stats2.messages_sent, 0);
    
    client1.stop().await?;
    client2.stop().await?;
    
    Ok(())
}
