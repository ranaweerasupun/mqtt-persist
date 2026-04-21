# mqtt-persist

[![Crates.io](https://img.shields.io/crates/v/mqtt-persist.svg)](https://crates.io/crates/mqtt-persist)
[![Documentation](https://docs.rs/mqtt-persist/badge.svg)](https://docs.rs/mqtt-persist)

A resilient MQTT client for Rust with offline message queuing and automatic reconnection.

## Features

- **Automatic Reconnection**: Connects automatically with exponential backoff
- **Offline Message Queuing**: Messages are queued in memory when disconnected
- **Async/Await**: Built on tokio for high performance
- **Resilient**: Designed for unreliable network conditions
- **Simple API**: Easy to use with sensible defaults
- **Built-in Statistics**: Monitor connection state and message flow

## Start Here ...

Add to your `Cargo.toml`:

```toml
[dependencies]
mqtt-persist = "0.1"
tokio = { version = "1.0", features = ["full"] }
```

Basic usage:

```rust
use mqtt_persist::{MqttClient, QoS, MqttError};

#[tokio::main]
async fn main() -> Result<(), MqttError> {
    // Create and start client
    let mut client = MqttClient::new("mqtt://localhost:1883", "my_device").await?;
    client.start().await?;
    
    // Publish works whether online or offline
    client.publish("sensors/temperature", b"23.5", QoS::AtLeastOnce).await?;
    
    // Get statistics
    let stats = client.stats().await;
    println!("Queue size: {}", stats.queue_size);
    
    Ok(())
}
```

## Working mechanism ...

**When connected**: Messages are published immediately to the broker.

**When disconnected**: Messages are queued in memory and automatically delivered when connection is restored.

**Reconnection**: Uses exponential backoff to avoid overwhelming the broker during reconnection.

## Examples

### Temperature Sensor Simulation

See a complete example of a simulated IoT temperature sensor:

```bash
cargo run --example temperature_sensor
```

This example demonstrates:
- Periodic sensor readings
- Offline queuing during network outages  
- Automatic message delivery when reconnected
- Real-time statistics

### Simple Usage

```bash
cargo run --example simple
```

A basic example showing the core functionality.

## Configuration

Customize client behavior with `MqttConfig`:

```rust
use mqtt_persist::{MqttClient, MqttConfig, QueueConfig, ReconnectConfig};
use std::time::Duration;

let config = MqttConfig {
    broker_url: "mqtt://broker.example.com:1883".to_string(),
    client_id: "my_device".to_string(),
    keep_alive: Duration::from_secs(60),
    connection_timeout: Duration::from_secs(10),
    queue_config: QueueConfig {
        max_size: 5000,
        max_retries: 3,
        overflow_policy: OverflowPolicy::DropOldest,
    },
    reconnect_config: ReconnectConfig {
        initial_delay: Duration::from_secs(1),
        max_delay: Duration::from_secs(300),
        backoff_multiplier: 2.0,
        max_attempts: 0, // Unlimited
        jitter: true,
    },
    max_message_size: 1024 * 1024, // 1MB
};

let mut client = MqttClient::with_config(config).await?;
```

## API Reference

### Core Methods

- `MqttClient::new(broker_url, client_id)` - Create a new client
- `client.start()` - Start the client and begin connection attempts  
- `client.publish(topic, payload, qos)` - Publish a message (blocks until sent or queued)
- `client.publish_async(topic, payload, qos)` - Publish without waiting
- `client.stats()` - Get current statistics
- `client.is_connected()` - Check connection status
- `client.stop()` - Stop the client

### Statistics

The `ClientStats` struct provides insight into client state:

```rust
let stats = client.stats().await;
println!("Connected: {}", stats.connected);
println!("Queue size: {}", stats.queue_size); 
println!("Messages sent: {}", stats.messages_sent);
println!("Messages queued: {}", stats.messages_queued);
println!("Reconnection attempts: {}", stats.reconnection_attempts);
```

## Testing

### Unit Tests

```bash
cargo test
```

### Integration Tests

Integration tests require a running MQTT broker:

```bash
# Start a local broker (using mosquitto)
mosquitto

# Run tests
cargo test --test integration
```

### Manual Testing

To test offline behavior:

1. Run an example: `cargo run --example temperature_sensor`
2. Stop your MQTT broker
3. Observe messages being queued
4. Restart your broker  
5. Watch messages drain from the queue

## Architecture

```
Application
    ↓
MqttClient
    ├── rumqttc (MQTT protocol handling)
    ├── OfflineQueue (in-memory message storage)
    └── ConnectionManager (reconnection logic)
```

The client runs a background task that:
- Handles MQTT events from rumqttc
- Processes publish requests from the application
- Drains the offline queue when connected
- Manages reconnection with exponential backoff

## Plans for Future vesions - If you contribute please keep to this roadmap

This is v0.1.0 with basic functionality. The new fuctionalities will be analoges to my python version [edge-mqtt](https://github.com/ranaweerasupun/resilient-edge-mqtt-client). Future versions will add:

- **v0.2.x**: SQLite persistence (messages survive restarts)
- **v0.3.x**: Priority queuing and inflight message tracking  
- **v0.4.x**: Structured logging and metrics
- **v0.5.x**: Compression, batching, and MQTT 5.0 features

## Comparison

| Feature | mqtt-persist | paho-mqtt | rumqttc |
|---------|--------------|-----------|---------|
| Offline queuing | In-memory | None |  None |
| Auto-reconnection | Exponential backoff |  Basic |  Basic |
| Message persistence | Memory only (v0.1) |  Optional |  None |
| Async/await |  Native | Available |  Native |
| Production ready |  Early stage |  Mature |  Mature |

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
