//! Performance benchmark for mqtt-persist
//!
//! Tests throughput and memory usage under various conditions

use mqtt_persist::{MqttClient, MqttConfigBuilder, QoS, OverflowPolicy, MqttError};
use std::time::{Duration, Instant};
use tokio::time::sleep;

struct BenchmarkResults {
    messages_sent: usize,
    duration: Duration,
    throughput_msg_per_sec: f64,
    avg_latency_ms: f64,
    queue_peak_size: usize,
}

impl BenchmarkResults {
    fn new(messages_sent: usize, duration: Duration, queue_peak_size: usize) -> Self {
        let throughput = messages_sent as f64 / duration.as_secs_f64();
        let avg_latency = duration.as_millis() as f64 / messages_sent as f64;
        
        Self {
            messages_sent,
            duration,
            throughput_msg_per_sec: throughput,
            avg_latency_ms: avg_latency,
            queue_peak_size,
        }
    }
    
    fn print(&self, test_name: &str) {
        println!(" {} Results:", test_name);
        println!("   Messages sent: {}", self.messages_sent);
        println!("   Duration: {:.2}s", self.duration.as_secs_f64());
        println!("   Throughput: {:.1} msg/sec", self.throughput_msg_per_sec);
        println!("   Avg latency: {:.2}ms", self.avg_latency_ms);
        println!("   Peak queue size: {}", self.queue_peak_size);
        println!();
    }
}

async fn benchmark_connected_throughput() -> Result<BenchmarkResults, MqttError> {
    println!(" Benchmarking connected throughput...");
    
    let config = MqttConfigBuilder::high_throughput("mqtt://localhost:1883", "bench_throughput")
        .build();
    
    let mut client = MqttClient::with_config(config).await?;
    client.start().await?;
    
    // Wait for connection
    sleep(Duration::from_secs(1)).await;
    
    const MESSAGE_COUNT: usize = 1000;
    let start_time = Instant::now();
    let mut peak_queue_size = 0;
    
    // Send messages as fast as possible
    for i in 0..MESSAGE_COUNT {
        let payload = format!(r#"{{"id":{},"timestamp":{},"data":"benchmark_payload"}}"#, 
                             i, start_time.elapsed().as_millis());
        
        client.publish_async("bench/throughput", payload.as_bytes(), QoS::AtMostOnce).await?;
        
        // Sample queue size occasionally
        if i % 100 == 0 {
            let stats = client.stats().await;
            peak_queue_size = peak_queue_size.max(stats.queue_size);
        }
    }
    
    let duration = start_time.elapsed();
    client.stop().await?;
    
    Ok(BenchmarkResults::new(MESSAGE_COUNT, duration, peak_queue_size))
}

async fn benchmark_offline_queuing() -> Result<BenchmarkResults, MqttError> {
    println!(" Benchmarking offline queuing...");
    
    // Use invalid broker to force offline mode
    let config = MqttConfigBuilder::new("mqtt://invalid-broker:1883", "bench_offline")
        .max_queue_size(10000)
        .overflow_policy(OverflowPolicy::DropOldest)
        .build();
    
    let mut client = MqttClient::with_config(config).await?;
    client.start().await?;
    
    // Give it time to fail connection
    sleep(Duration::from_millis(100)).await;
    
    const MESSAGE_COUNT: usize = 5000;
    let start_time = Instant::now();
    let mut peak_queue_size = 0;
    
    // Fill up the offline queue
    for i in 0..MESSAGE_COUNT {
        let payload = format!("offline_message_{}", i);
        client.publish_async("bench/offline", payload.as_bytes(), QoS::AtLeastOnce).await?;
        
        // Sample queue size
        if i % 500 == 0 {
            let stats = client.stats().await;
            peak_queue_size = peak_queue_size.max(stats.queue_size);
        }
    }
    
    let duration = start_time.elapsed();
    
    // Verify all messages were queued
    let final_stats = client.stats().await;
    println!("   Final queue size: {}", final_stats.queue_size);
    
    client.stop().await?;
    
    Ok(BenchmarkResults::new(MESSAGE_COUNT, duration, peak_queue_size))
}

async fn benchmark_mixed_workload() -> Result<BenchmarkResults, MqttError> {
    println!(" Benchmarking mixed workload (connected + offline)...");
    
    let config = MqttConfigBuilder::high_reliability("mqtt://localhost:1883", "bench_mixed")
        .build();
    
    let mut client = MqttClient::with_config(config).await?;
    client.start().await?;
    
    // Wait for connection
    sleep(Duration::from_secs(1)).await;
    
    const MESSAGE_COUNT: usize = 2000;
    let start_time = Instant::now();
    let mut peak_queue_size = 0;
    
    for i in 0..MESSAGE_COUNT {
        let payload = format!(r#"{{"batch":{},"mixed_test":true}}"#, i);
        let qos = if i % 2 == 0 { QoS::AtMostOnce } else { QoS::AtLeastOnce };
        
        client.publish_async("bench/mixed", payload.as_bytes(), qos).await?;
        
        // Sample queue and add some delay to simulate real workload
        if i % 200 == 0 {
            let stats = client.stats().await;
            peak_queue_size = peak_queue_size.max(stats.queue_size);
            sleep(Duration::from_millis(1)).await;
        }
    }
    
    let duration = start_time.elapsed();
    client.stop().await?;
    
    Ok(BenchmarkResults::new(MESSAGE_COUNT, duration, peak_queue_size))
}

async fn benchmark_memory_usage() -> Result<(), MqttError> {
    println!(" Benchmarking memory usage...");
    
    let config = MqttConfigBuilder::new("mqtt://invalid-broker:1883", "bench_memory")
        .max_queue_size(50000)
        .overflow_policy(OverflowPolicy::Error)
        .build();
    
    let mut client = MqttClient::with_config(config).await?;
    client.start().await?;
    
    sleep(Duration::from_millis(100)).await;
    
    // Fill queue with different sized messages
    const LARGE_MESSAGE_SIZE: usize = 10240; // 10KB
    let large_payload = vec![b'A'; LARGE_MESSAGE_SIZE];
    
    println!("   Testing large messages (10KB each)...");
    for i in 0..1000 {
        client.publish_async("bench/memory/large", &large_payload[..], QoS::AtLeastOnce).await?;
        
        if i % 100 == 0 {
            let stats = client.stats().await;
            println!("   Queued {} large messages, queue size: {}", i + 1, stats.queue_size);
        }
    }
    
    println!("   Testing small messages (100B each)...");
    let small_payload = vec![b'B'; 100];
    for i in 0..10000 {
        client.publish_async("bench/memory/small", &small_payload[..], QoS::AtLeastOnce).await?;
        
        if i % 1000 == 0 {
            let stats = client.stats().await;
            println!("      Queued {} small messages, total queue size: {}", i + 1, stats.queue_size);
        }
    }
    
    let final_stats = client.stats().await;
    println!("    Final queue size: {} messages", final_stats.queue_size);
    println!("      Estimated memory: ~{:.1} MB", 
             (1000 * LARGE_MESSAGE_SIZE + 10000 * 100) as f64 / (1024.0 * 1024.0));
    
    client.stop().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), MqttError> {
    println!("⚡ mqtt-persist Performance Benchmarks\n");
    println!(" These benchmarks test various performance characteristics.");
    println!("   For connected tests, ensure you have a local MQTT broker running.\n");
    
    // Run throughput benchmark
    match benchmark_connected_throughput().await {
        Ok(results) => results.print("Connected Throughput"),
        Err(e) => println!("❌ Connected throughput test failed: {}\n", e),
    }
    
    // Run offline queuing benchmark
    match benchmark_offline_queuing().await {
        Ok(results) => results.print("Offline Queuing"),
        Err(e) => println!("❌ Offline queuing test failed: {}\n", e),
    }
    
    // Run mixed workload benchmark
    match benchmark_mixed_workload().await {
        Ok(results) => results.print("Mixed Workload"),
        Err(e) => println!("❌ Mixed workload test failed: {}\n", e),
    }
    
    // Run memory usage test
    if let Err(e) = benchmark_memory_usage().await {
        println!("❌ Memory usage test failed: {}\n", e);
    }
    
    println!("🎯 Benchmark Summary:");
    println!("   These results help understand mqtt-persist performance characteristics.");
    println!("   For production use, tune configuration based on your specific requirements.");
    println!("\n All benchmarks completed!");
    
    Ok(())
}
