//! Simple example showing basic mqtt-persist usage

use mqtt_persist::{MqttClient, QoS, MqttError};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), MqttError> {
    println!("Starting mqtt-persist simple example...");

    // Create client
    let mut client = MqttClient::new("mqtt://localhost:1883", "simple_example").await?;
    
    // Start the client (begins connection attempts)
    client.start().await?;
    
    // Give it a moment to connect
    sleep(Duration::from_secs(2)).await;
    
    // Publish some messages
    for i in 0..5 {
        let payload = format!("Hello, MQTT! Message #{}", i);
        
        match client.publish("test/simple", payload.as_bytes(), QoS::AtLeastOnce).await {
            Ok(_) => println!("Published: {}", payload),
            Err(e) => println!("Failed to publish: {}", e),
        }
        
        sleep(Duration::from_millis(500)).await;
    }
    
    // Show statistics
    let stats = client.stats().await;
    println!("\nClient Statistics:");
    println!("  Connected: {}", stats.connected);
    println!("  Queue size: {}", stats.queue_size);
    println!("  Messages sent: {}", stats.messages_sent);
    println!("  Messages queued: {}", stats.messages_queued);
    println!("  Reconnection attempts: {}", stats.reconnection_attempts);
    
    // Test offline behavior (stop your MQTT broker to see this in action)
    println!("\nTesting offline queuing (stop your broker to see queuing in action)...");
    for i in 5..10 {
        let payload = format!("Offline message #{}", i);
        
        // This will queue the message if we're offline
        client.publish_async("test/offline", payload.as_bytes(), QoS::AtLeastOnce).await?;
        println!("Queued: {}", payload);
        
        sleep(Duration::from_millis(200)).await;
    }
    
    // Show final statistics
    sleep(Duration::from_secs(1)).await;
    let final_stats = client.stats().await;
    println!("\nFinal Statistics:");
    println!("  Queue size: {}", final_stats.queue_size);
    println!("  Total messages sent: {}", final_stats.messages_sent);
    println!("  Total messages queued: {}", final_stats.messages_queued);
    
    println!("\nStopping client...");
    client.stop().await?;
    
    println!("Example completed!");
    Ok(())
}
