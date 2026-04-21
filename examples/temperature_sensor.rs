//! Temperature sensor simulation showing mqtt-persist in action
//!
//! This example simulates a temperature sensor that:
//! - Publishes readings every 5 seconds
//! - Shows queue statistics every 10 readings  
//! - Demonstrates offline behavior when broker is unavailable

use mqtt_persist::{MqttClient, QoS, MqttError};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration, interval};

#[derive(Debug)]
struct TemperatureReading {
    device_id: String,
    temperature: f32,
    humidity: f32,
    timestamp: u64,
    sequence: u32,
}

impl TemperatureReading {
    fn new(device_id: String, sequence: u32) -> Self {
        // Simulate sensor readings with some variation
        let base_temp = 22.0;
        let temp_variation = (sequence as f32 * 0.1).sin() * 3.0;
        let temperature = base_temp + temp_variation + (fastrand::f32() - 0.5) * 2.0;
        
        let base_humidity = 45.0;
        let humidity_variation = (sequence as f32 * 0.15).cos() * 10.0;
        let humidity = base_humidity + humidity_variation + (fastrand::f32() - 0.5) * 5.0;
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            device_id,
            temperature: (temperature * 100.0).round() / 100.0,
            humidity: (humidity * 100.0).round() / 100.0,
            timestamp,
            sequence,
        }
    }
    
    fn to_payload(&self) -> String {
        format!(
            "device_id={},temperature={:.2},humidity={:.2},timestamp={},sequence={}",
            self.device_id, self.temperature, self.humidity, self.timestamp, self.sequence
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), MqttError> {
    println!("  Starting temperature sensor simulation...");
    println!("   This example demonstrates mqtt-persist offline queuing");
    println!("   Try stopping/starting your MQTT broker to see queuing in action!\n");

    let device_id = "temp_sensor_001";
    let mut client = MqttClient::new("mqtt://localhost:1883", device_id).await?;
    
    // Start the client
    println!(" Starting MQTT client...");
    client.start().await?;
    
    // Give it a moment to connect
    sleep(Duration::from_secs(2)).await;
    
    let mut sequence = 0u32;
    let mut readings_interval = interval(Duration::from_secs(5));
    let mut stats_interval = interval(Duration::from_secs(50)); // Every 10 readings
    
    println!(" Beginning temperature readings (every 5 seconds)...\n");

    loop {
        tokio::select! {
            // Publish temperature readings
            _ = readings_interval.tick() => {
                sequence += 1;
                let reading = TemperatureReading::new(device_id.to_string(), sequence);
                
                let topic = format!("sensors/{}/temperature", device_id);
                let payload = reading.to_payload();
                
                // Get stats to check connection status
                let stats = client.stats().await;
                let status = if stats.connected { "🟢" } else { "🔴" };
                
                match client.publish_async(&topic, payload.as_bytes(), QoS::AtLeastOnce).await {
                    Ok(_) => {
                        let time_str = format_timestamp(reading.timestamp);
                        println!("{} #{:03} |   {:.1}°C |  {:.1}% | {} {}",
                            status, sequence, reading.temperature, reading.humidity, 
                            if stats.connected { "SENT" } else { "QUEUED" },
                            time_str
                        );
                    }
                    Err(e) => {
                        println!("❌ #{:03} | Failed to publish: {}", sequence, e);
                    }
                }
            }
            
            // Show statistics periodically
            _ = stats_interval.tick() => {
                let stats = client.stats().await;
                println!("\n === STATISTICS ===");
                println!("   Connection: {}", if stats.connected { "🟢 Online" } else { "🔴 Offline" });
                println!("   Messages sent: {}", stats.messages_sent);
                println!("   Messages queued: {}", stats.messages_queued);
                println!("   Current queue size: {}", stats.queue_size);
                println!("   Reconnection attempts: {}", stats.reconnection_attempts);
                
                if stats.queue_size > 0 {
                    println!("    {} messages waiting for delivery", stats.queue_size);
                }
                
                println!("==================\n");
                
                // Give some guidance to the user
                if sequence % 20 == 0 {
                    if stats.connected {
                        println!(" TIP: Try stopping your MQTT broker to see offline queuing in action!");
                    } else {
                        println!(" TIP: Start your MQTT broker to see messages drain from the queue!");
                    }
                    println!();
                }
            }
        }
    }
}

fn format_timestamp(timestamp: u64) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    match SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(timestamp)) {
        Some(_time) => {
            // Simple time formatting
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let age = now.saturating_sub(timestamp);
            
            if age < 60 {
                format!("{}s ago", age)
            } else if age < 3600 {
                format!("{}m ago", age / 60)
            } else {
                format!("{}h ago", age / 3600)
            }
        }
        None => "now".to_string(),
    }
}

// Simple random number generation for simulation
mod fastrand {
    use std::sync::atomic::{AtomicU32, Ordering};
    
    static STATE: AtomicU32 = AtomicU32::new(1);
    
    pub fn f32() -> f32 {
        let prev = STATE.load(Ordering::Relaxed);
        let next = prev.wrapping_mul(1664525).wrapping_add(1013904223);
        STATE.store(next, Ordering::Relaxed);
        (next as f32) / (u32::MAX as f32)
    }
}