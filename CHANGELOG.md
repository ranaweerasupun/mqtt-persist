# Changelog

All notable changes to mqtt-persist will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-XX

### 🎉 Initial Release

#### Added
- **Core MQTT Client** with async/await API built on rumqttc
- **In-memory offline queuing** for messages when disconnected
- **Automatic reconnection** with exponential backoff and jitter
- **Quality of Service** support for QoS 0 and 1
- **Thread-safe architecture** using tokio channels
- **Configuration system** with builder pattern and presets
- **Client statistics** for monitoring connection state and message flow
- **Comprehensive error handling** with custom error types
- **Multiple overflow policies** for queue management (DropOldest, DropNewest, Error)

#### Configuration Presets
- `high_reliability()` - For critical applications (10K queue, 5 retries)
- `low_resource()` - For constrained environments (100 queue, 2 retries)  
- `high_throughput()` - For high-volume applications (50K queue, fast reconnect)
- `development()` - For testing and development (deterministic behavior)

#### Examples
- **Simple usage** - Basic publish/subscribe functionality
- **Temperature sensor** - IoT device simulation with offline behavior
- **Advanced config** - Demonstrates configuration options and monitoring
- **Benchmark** - Performance testing across different scenarios

#### Documentation
- Comprehensive README with quick start guide
- API documentation with examples
- Testing guide for various scenarios
- Performance characteristics and tuning advice

### 📦 Dependencies
- `rumqttc ^0.25` - MQTT protocol implementation
- `tokio ^1.0` - Async runtime with full features
- `serde ^1.0` - Serialization for configuration
- `thiserror ^1.0` - Error handling
- `url ^2.0` - URL parsing for broker addresses

---

## [Unreleased]

### Planned for v0.1.1
- [ ] Improved reconnection logging
- [ ] Better connection timeout handling
- [ ] Message size validation improvements
- [ ] Additional configuration validation
- [ ] Performance optimizations for queue operations

### Planned for v0.2.0 - "Persistence"
- [ ] **SQLite persistence** - Messages survive application restarts
- [ ] **Hybrid memory/disk queue** - Fast access with durability
- [ ] **Configurable flush intervals** - Balance performance vs durability
- [ ] **Database migration system** - Schema evolution support
- [ ] **WAL mode optimization** - Better concurrent access
- [ ] **Corruption detection and recovery** - Graceful degradation

### Planned for v0.3.0 - "Priority & Inflight"
- [ ] **Priority queuing (1-10)** - Critical messages first
- [ ] **Inflight message tracking** - Separate QoS 1/2 handling
- [ ] **Message deduplication** - Prevent replays on restart
- [ ] **Priority-based eviction** - Smart queue management
- [ ] **QoS 2 exactly-once** - Complete message flow handling
- [ ] **Configuration hot-reload** - Runtime config updates

### Planned for v0.4.0 - "Observability"
- [ ] **Structured logging** - JSON logs with context
- [ ] **Metrics collection** - Counters, gauges, histograms
- [ ] **Prometheus export** - Standard metrics format
- [ ] **Health check endpoints** - HTTP status API
- [ ] **Tracing integration** - Distributed tracing support
- [ ] **Performance dashboard** - Real-time monitoring

### Planned for v0.5.0 - "Advanced Features"
- [ ] **Message compression** - gzip, lz4 support
- [ ] **Batch publishing** - Efficiency for high throughput
- [ ] **Flow control** - Backpressure handling
- [ ] **MQTT 5.0 features** - User properties, message expiry
- [ ] **Topic aliases** - Bandwidth optimization
- [ ] **Shared subscriptions** - Load balancing

### Future Considerations (v1.0+)
- [ ] **TLS/mTLS support** - Secure connections
- [ ] **Certificate rotation** - Automatic cert management
- [ ] **Multi-broker support** - Failover and load balancing
- [ ] **Sparkplug B compatibility** - Industrial IoT standard
- [ ] **WebSocket transport** - Browser compatibility
- [ ] **Cloud integrations** - AWS IoT, Azure IoT, GCP IoT
- [ ] **Language bindings** - C FFI for embedded use
- [ ] **WASM support** - Browser and edge runtimes

---

## Version History

### Pre-release Development
- Research phase: Analyzed existing MQTT libraries and identified gaps
- Architecture design: Selected rumqttc + tokio + async approach
- Proof of concept: Basic offline queuing with in-memory storage
- API design: Simple, ergonomic interface for developers
- Testing strategy: Comprehensive examples and integration tests

---

## Migration Guides

### From Other MQTT Libraries

#### From paho-mqtt (Python-style)
```rust
// Before (conceptual - this was Python)
// client = mqtt.Client()
// client.connect("localhost", 1883, 60)
// client.publish("topic", "payload")

// After (mqtt-persist)
let mut client = MqttClient::new("mqtt://localhost:1883", "client_id").await?;
client.start().await?;
client.publish("topic", "payload", QoS::AtMostOnce).await?;
```

#### From raw rumqttc
```rust
// Before
let mut mqttoptions = MqttOptions::new("client", "localhost", 1883);
let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
// ... manual event loop handling

// After  
let mut client = MqttClient::new("mqtt://localhost:1883", "client").await?;
client.start().await?; // Background event loop handled automatically
```

#### From Eclipse Paho Rust
```rust
// Before (file persistence)
let mut cli = mqtt::Client::new(create_opts)?;
cli.set_connection_lost_callback(|cli: &mqtt::Client| { /* manual reconnect */ });

// After (automatic resilience)
let mut client = MqttClient::new("mqtt://localhost:1883", "client").await?;
client.start().await?; // Automatic reconnection with backoff
```

---

## Contributors

- Initial implementation and architecture
- Example applications and benchmarks  
- Documentation and testing infrastructure
- Performance analysis and optimization

## License

This project is licensed under either of:
- Apache License, Version 2.0
- MIT license

at your option.
