# Testing Guide for mqtt-persist

This guide covers how to test mqtt-persist in various scenarios and environments.

## Prerequisites

### MQTT Broker

For most tests, you'll need a running MQTT broker. We recommend using Mosquitto:

**Using Docker:**
```bash
docker run -it -p 1883:1883 eclipse-mosquitto:latest
```

**Using Package Manager:**
```bash
# Ubuntu/Debian
sudo apt-get install mosquitto

# macOS
brew install mosquitto

# Start the broker
mosquitto
```

**Using Online Broker (for testing only):**
```rust
// Use test.mosquitto.org for quick testing
let client = MqttClient::new("mqtt://test.mosquitto.org:1883", "test_client").await?;
```

## Running Examples

### 1. Basic Functionality Test

```bash
cargo run --example simple
```

This example:
- ✅ Tests basic connection
- ✅ Publishes messages when online
- ✅ Shows statistics
- ✅ Demonstrates async publishing

### 2. Offline Behavior Test

```bash
cargo run --example temperature_sensor
```

**To test offline queuing:**
1. Start the example
2. Stop your MQTT broker (`Ctrl+C` if running locally)
3. Observe messages being queued (🔴 status)
4. Restart your broker
5. Watch messages drain from queue (🟢 status)

### 3. Configuration Options Test

```bash
cargo run --example advanced_config
```

This demonstrates:
- Different configuration presets
- Multiple client instances
- Various QoS levels
- Statistics monitoring

### 4. Performance Testing

```bash
cargo run --example benchmark
```

**Benchmark Results Help:**
- Throughput: Messages per second when connected
- Latency: Average time per message
- Memory: Queue behavior under load
- Offline: Queuing performance when disconnected

## Unit Tests

Run the test suite:

```bash
cargo test
```

**Test Coverage:**
- Queue operations (enqueue, dequeue, overflow)
- Connection state management
- Error handling
- Configuration validation

## Integration Tests

Integration tests require a running broker:

```bash
# Start broker first
mosquitto

# In another terminal
cargo test --test integration
```

**What's Tested:**
- Real MQTT connections
- End-to-end message flow
- Client lifecycle management
- Error scenarios

## Manual Testing Scenarios

### Scenario 1: Network Interruption

**Setup:**
```bash
cargo run --example temperature_sensor
```

**Test Steps:**
1. Observe normal operation (🟢 connected)
2. Disconnect network or stop broker
3. Verify messages queue (🔴 offline, queue size increases)
4. Reconnect network/broker
5. Verify automatic reconnection and queue drainage

**Expected Behavior:**
- Seamless transition to offline mode
- Messages queued without loss
- Automatic reconnection with exponential backoff
- Queue drains in order when reconnected

### Scenario 2: Broker Restart

**Setup:**
```bash
# Terminal 1
cargo run --example simple

# Terminal 2
mosquitto
```

**Test Steps:**
1. Start example (connects to broker)
2. Stop broker (`Ctrl+C` in terminal 2)
3. Observe reconnection attempts
4. Restart broker
5. Verify successful reconnection

### Scenario 3: High Load

**Setup:**
```bash
cargo run --example benchmark
```

**Test Different Configs:**
```rust
// High throughput
let config = MqttConfigBuilder::high_throughput("mqtt://localhost:1883", "load_test");

// Low resource 
let config = MqttConfigBuilder::low_resource("mqtt://localhost:1883", "load_test");

// Custom tuning
let config = MqttConfigBuilder::new("mqtt://localhost:1883", "load_test")
    .max_queue_size(10000)
    .overflow_policy(OverflowPolicy::DropOldest)
    .build();
```

### Scenario 4: Queue Overflow

**Test Code:**
```rust
use mqtt_persist::{MqttClient, MqttConfigBuilder, OverflowPolicy, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Small queue that will overflow
    let config = MqttConfigBuilder::new("mqtt://invalid:1883", "overflow_test")
        .max_queue_size(10)
        .overflow_policy(OverflowPolicy::DropOldest) // or DropNewest or Error
        .build();
    
    let mut client = MqttClient::with_config(config).await?;
    client.start().await?;
    
    // Fill beyond capacity
    for i in 0..20 {
        client.publish_async("test/overflow", format!("msg_{}", i).as_bytes(), QoS::AtMostOnce).await?;
        let stats = client.stats().await;
        println!("Message {}: queue size = {}", i, stats.queue_size);
    }
    
    Ok(())
}
```

## Testing Different MQTT Brokers

### Mosquitto (Local)
```rust
let client = MqttClient::new("mqtt://localhost:1883", "test").await?;
```

### HiveMQ Cloud
```rust
let client = MqttClient::new("mqtt://your-cluster.s1.eu.hivemq.cloud:8883", "test").await?;
// Note: Add TLS support in future versions
```

### EMQX
```rust
let client = MqttClient::new("mqtt://localhost:1883", "test").await?;
```

### Test.mosquitto.org (Public)
```rust
// For testing only - not for production!
let client = MqttClient::new("mqtt://test.mosquitto.org:1883", "test").await?;
```

## Debugging Tips

### Enable Debug Logging

Add to your test:
```rust
use tracing_subscriber;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    // Your test code...
}
```

### Monitor with MQTT Clients

Use an MQTT client to monitor traffic:

```bash
# Subscribe to all topics
mosquitto_sub -h localhost -t '#' -v

# Subscribe to your test topics
mosquitto_sub -h localhost -t 'test/+' -v
```

### Common Issues

**Connection Refused:**
- Check broker is running: `netstat -an | grep 1883`
- Verify broker config allows connections
- Check firewall settings

**Messages Not Appearing:**
- Verify QoS levels match expectations
- Check topic names for typos
- Ensure subscriber is connected

**Performance Issues:**
- Tune `max_queue_size` for your use case
- Adjust `keep_alive` interval
- Consider QoS level impact

## Automated Testing Scripts

### Quick Smoke Test

```bash
#!/bin/bash
# test-smoke.sh

echo "🧪 Running smoke tests..."

echo "1. Starting broker..."
mosquitto &
BROKER_PID=$!
sleep 2

echo "2. Running basic test..."
cargo run --example simple || exit 1

echo "3. Running unit tests..."
cargo test || exit 1

echo "4. Running integration tests..."
cargo test --test integration || exit 1

echo "5. Cleaning up..."
kill $BROKER_PID

echo "✅ All smoke tests passed!"
```

### Performance Regression Test

```bash
#!/bin/bash
# test-performance.sh

echo "⚡ Running performance tests..."
mosquitto &
BROKER_PID=$!
sleep 2

cargo run --example benchmark > benchmark_results.txt
echo "📊 Benchmark results saved to benchmark_results.txt"

kill $BROKER_PID
echo "🏁 Performance test completed"
```

## Contributing Tests

When adding new features, please include:

1. **Unit tests** for core logic
2. **Integration tests** for end-to-end behavior  
3. **Example code** showing usage
4. **Documentation** updates

**Test Structure:**
```
tests/
├── integration.rs      # End-to-end tests
├── unit/
│   ├── queue.rs        # Queue-specific tests
│   ├── connection.rs   # Connection tests
│   └── config.rs       # Configuration tests
└── fixtures/           # Test data/helpers
```

This testing approach ensures mqtt-persist works reliably across different environments and use cases.
