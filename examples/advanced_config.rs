//! Advanced example showing configuration options and monitoring

use mqtt_persist::{MqttClient, MqttConfigBuilder, QoS, OverflowPolicy, MqttError};
use std::time::Duration;
use tokio::time::{sleep, interval};

#[tokio::main]
async fn main() -> Result<(), MqttError> {
    println!("🔧 Advanced mqtt-persist configuration example\n");

    // Example 1: High-reliability configuration
    println!("📡 Creating high-reliability client...");
    let config = MqttConfigBuilder::high_reliability("mqtt://localhost:1883", "reliable_device")
        .keep_alive(Duration::from_secs(30))
        .connection_timeout(Duration::from_secs(5))
        .build();
    
    let mut reliable_client = MqttClient::with_config(config).await?;
    reliable_client.start().await?;
    
    println!("   ✅ High-reliability client started");
    
    // Example 2: Low-resource configuration
    println!("💾 Creating low-resource client...");
    let low_resource_config = MqttConfigBuilder::low_resource("mqtt://localhost:1883", "edge_device")
        .max_message_size(1024) // Only 1KB messages
        .build();
    
    let mut edge_client = MqttClient::with_config(low_resource_config).await?;
    edge_client.start().await?;
    
    println!("   ✅ Low-resource client started");

    // Example 3: Custom configuration
    println!("🛠️  Creating custom configured client...");
    let custom_config = MqttConfigBuilder::new("mqtt://localhost:1883", "custom_device")
        .max_queue_size(5000)
        .overflow_policy(OverflowPolicy::DropOldest)
        .max_retries(3)
        .initial_reconnect_delay(Duration::from_millis(500))
        .max_reconnect_delay(Duration::from_secs(60))
        .backoff_multiplier(2.0)
        .jitter(true)
        .keep_alive(Duration::from_secs(45))
        .build();
    
    let mut custom_client = MqttClient::with_config(custom_config).await?;
    custom_client.start().await?;
    
    println!("   ✅ Custom client started\n");

    // Wait for connections
    sleep(Duration::from_secs(2)).await;

    // Demonstrate different publishing patterns
    println!("📊 Testing different publishing patterns...\n");

    // High-frequency publishing to reliable client
    println!("🔄 Publishing to reliable client (high frequency)...");
    for i in 0..10 {
        let payload = format!(r#"{{"sensor":"temperature","value":{:.1},"sequence":{}}}"#, 20.0 + i as f32, i);
        reliable_client.publish_async("sensors/reliable/temp", payload.as_bytes(), QoS::AtLeastOnce).await?;
        
        if i % 3 == 0 {
            println!("   📈 Published batch #{}", i / 3 + 1);
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Low-resource client with small messages
    println!("\n💾 Publishing to low-resource client (small messages)...");
    for i in 0..5 {
        let payload = format!("{:.1}", 25.0 + i as f32); // Just the temperature value
        edge_client.publish_async("sensors/edge/temp", payload.as_bytes(), QoS::AtMostOnce).await?;
        println!("   📤 Edge message #{}: {}", i + 1, payload);
        sleep(Duration::from_millis(200)).await;
    }

    // Custom client with mixed QoS
    println!("\n🛠️  Publishing to custom client (mixed QoS)...");
    for i in 0..8 {
        let qos = if i % 2 == 0 { QoS::AtLeastOnce } else { QoS::AtMostOnce };
        let payload = format!(r#"{{"device":"custom","data":"value_{}","critical":{}}}"#, i, qos == QoS::AtLeastOnce);
        
        custom_client.publish_async("sensors/custom/data", payload.as_bytes(), qos).await?;
        println!("   📡 Custom message #{} (QoS {})", i + 1, if qos == QoS::AtLeastOnce { 1 } else { 0 });
        sleep(Duration::from_millis(150)).await;
    }

    // Monitor clients for a while
    println!("\n📊 Monitoring client statistics...\n");
    
    let mut monitor_interval = interval(Duration::from_secs(3));
    
    for round in 1..=5 {
        monitor_interval.tick().await;
        
        println!("=== MONITORING ROUND {} ===", round);
        
        // Reliable client stats
        let reliable_stats = reliable_client.stats().await;
        println!("🔒 Reliable Client:");
        println!("   Status: {}", if reliable_stats.connected { "🟢 Connected" } else { "🔴 Disconnected" });
        println!("   Sent: {} | Queued: {} | Queue Size: {}", 
                reliable_stats.messages_sent, reliable_stats.messages_queued, reliable_stats.queue_size);
        
        // Edge client stats  
        let edge_stats = edge_client.stats().await;
        println!("💾 Edge Client:");
        println!("   Status: {}", if edge_stats.connected { "🟢 Connected" } else { "🔴 Disconnected" });
        println!("   Sent: {} | Queued: {} | Queue Size: {}", 
                edge_stats.messages_sent, edge_stats.messages_queued, edge_stats.queue_size);
        
        // Custom client stats
        let custom_stats = custom_client.stats().await;
        println!("🛠️  Custom Client:");
        println!("   Status: {}", if custom_stats.connected { "🟢 Connected" } else { "🔴 Disconnected" });
        println!("   Sent: {} | Queued: {} | Queue Size: {}", 
                custom_stats.messages_sent, custom_stats.messages_queued, custom_stats.queue_size);
        
        println!();
        
        // Add some more messages during monitoring
        if round <= 3 {
            let payload = format!("monitoring_round_{}", round);
            reliable_client.publish_async("test/monitoring", payload.as_bytes(), QoS::AtLeastOnce).await?;
        }
    }

    println!("🏁 Stopping all clients...");
    
    reliable_client.stop().await?;
    edge_client.stop().await?;
    custom_client.stop().await?;
    
    println!("✅ All clients stopped successfully!");
    println!("\n🎉 Advanced configuration example completed!");
    
    Ok(())
}
