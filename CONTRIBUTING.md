# Contributing to mqtt-persist

Thank you for your interest in contributing to mqtt-persist! This guide will help you get started.

## 🚀 Quick Start

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/mqtt-persist.git
   cd mqtt-persist
   ```

2. **Set up development environment**
   ```bash
   # Install Rust if you haven't already
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   
   # Install development dependencies
   rustup component add rustfmt clippy
   ```

3. **Run tests to verify setup**
   ```bash
   # Start MQTT broker for integration tests
   docker run -d -p 1883:1883 --name mqtt-test eclipse-mosquitto:latest
   
   # Run all tests
   cargo test
   
   # Clean up
   docker stop mqtt-test && docker rm mqtt-test
   ```

## 📋 Development Workflow

### Before Starting

1. **Check existing issues** to avoid duplicate work
2. **Create an issue** for new features or significant changes
3. **Discuss the approach** in the issue before implementing

### Making Changes

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make small, focused commits**
   ```bash
   git commit -m "Add offline queue size configuration
   
   - Added max_size parameter to QueueConfig
   - Updated builder pattern to support queue sizing  
   - Added validation for queue size limits"
   ```

3. **Follow coding standards**
   ```bash
   # Format code
   cargo fmt
   
   # Check for common issues
   cargo clippy
   
   # Run tests
   cargo test
   ```

## 🎯 Areas for Contribution

### High Priority
- **SQLite persistence** (v0.2.0) - Messages survive restart
- **Priority queuing** (v0.3.0) - Smart message ordering
- **Performance optimizations** - Reduce latency and memory usage
- **Documentation improvements** - More examples and guides
- **Testing coverage** - Edge cases and error conditions

### Medium Priority  
- **MQTT 5.0 features** - User properties, message expiry
- **TLS/SSL support** - Secure connections
- **Configuration validation** - Better error messages
- **Metrics and monitoring** - Prometheus integration
- **Additional broker compatibility** - Testing with different brokers

### Good First Issues
- **Additional examples** - Real-world use cases
- **Error message improvements** - More helpful diagnostics  
- **Configuration presets** - Industry-specific defaults
- **Documentation typos** - Always appreciated!
- **Test case additions** - Expand test coverage

## 📝 Code Guidelines

### Rust Style

Follow standard Rust conventions:

```rust
// ✅ Good
pub struct QueueConfig {
    pub max_size: usize,
    pub overflow_policy: OverflowPolicy,
}

impl QueueConfig {
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            overflow_policy: OverflowPolicy::DropOldest,
        }
    }
}

// ❌ Avoid
pub struct queueConfig {  // Use PascalCase
    maxSize: usize,       // Use snake_case
}
```

### Error Handling

Use `Result` types and descriptive errors:

```rust
// ✅ Good
fn parse_broker_url(url: &str) -> Result<BrokerConfig, MqttError> {
    let parsed = Url::parse(url)
        .map_err(|_| MqttError::InvalidUrl(format!("Invalid broker URL: {}", url)))?;
    
    Ok(BrokerConfig {
        host: parsed.host_str().unwrap_or("localhost").to_string(),
        port: parsed.port().unwrap_or(1883),
    })
}

// ❌ Avoid
fn parse_broker_url(url: &str) -> BrokerConfig {
    let parsed = Url::parse(url).unwrap(); // Don't panic!
    // ...
}
```

### Async/Await

Use async/await consistently:

```rust
// ✅ Good
pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
    let request = PublishRequest::new(topic, payload);
    self.sender.send(request).await?;
    Ok(())
}

// ❌ Avoid blocking calls in async context
pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<()> {
    std::thread::sleep(Duration::from_millis(100)); // Blocks executor!
    // ...
}
```

### Documentation

Document public APIs with examples:

```rust
/// Publish a message to the specified topic.
/// 
/// Messages are queued offline if not currently connected and delivered
/// automatically when connection is restored.
/// 
/// # Examples
/// 
/// ```rust
/// use mqtt_persist::{MqttClient, QoS};
/// 
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut client = MqttClient::new("mqtt://localhost:1883", "device").await?;
/// client.start().await?;
/// 
/// client.publish("sensors/temperature", b"23.5", QoS::AtLeastOnce).await?;
/// # Ok(())
/// # }
/// ```
/// 
/// # Errors
/// 
/// Returns `MqttError::InvalidTopic` if the topic is empty or contains
/// invalid characters.
pub async fn publish(&self, topic: &str, payload: &[u8], qos: QoS) -> Result<()> {
    // Implementation...
}
```

## 🧪 Testing Requirements

### Unit Tests

Test core logic in isolation:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_queue_overflow_drops_oldest() {
        let config = QueueConfig {
            max_size: 2,
            overflow_policy: OverflowPolicy::DropOldest,
        };
        let queue = OfflineQueue::new(config);
        
        // Fill queue to capacity
        queue.enqueue(message("first")).await.unwrap();
        queue.enqueue(message("second")).await.unwrap();
        
        // This should drop "first"
        queue.enqueue(message("third")).await.unwrap();
        
        assert_eq!(queue.dequeue().await.unwrap().payload, b"second");
        assert_eq!(queue.dequeue().await.unwrap().payload, b"third");
    }
}
```

### Integration Tests

Test end-to-end behavior:

```rust
#[tokio::test]
async fn test_offline_queue_drains_on_reconnect() {
    // Use invalid broker to force offline
    let mut client = MqttClient::new("mqtt://invalid:1883", "test").await?;
    client.start().await?;
    
    // Queue messages while offline
    for i in 0..5 {
        client.publish_async("test/topic", format!("msg_{}", i).as_bytes(), QoS::AtLeastOnce).await?;
    }
    
    let stats = client.stats().await;
    assert_eq!(stats.queue_size, 5);
    assert_eq!(stats.messages_sent, 0);
}
```

### Examples as Tests

Ensure examples work:

```bash
# Add to CI
cargo run --example simple
cargo run --example temperature_sensor &
sleep 10
pkill -f temperature_sensor
```

## 📊 Performance Considerations

### Benchmarking

Profile performance-critical changes:

```bash
# Run benchmarks before and after changes
cargo run --example benchmark > before.txt
# Make your changes
cargo run --example benchmark > after.txt
diff before.txt after.txt
```

### Memory Usage

Monitor memory consumption:

```rust
#[tokio::test]
async fn test_memory_bounds() {
    let config = QueueConfig { max_size: 1000, ..Default::default() };
    let queue = OfflineQueue::new(config);
    
    // Fill queue completely
    for i in 0..1000 {
        queue.enqueue(large_message()).await.unwrap();
    }
    
    // Verify memory stays bounded
    let mem_usage = get_memory_usage(); // Implement this
    assert!(mem_usage < MAX_EXPECTED_MEMORY);
}
```

### Async Performance

Avoid blocking the executor:

```rust
// ✅ Good - non-blocking
pub async fn process_queue(&self) -> Result<()> {
    while let Some(message) = self.queue.try_dequeue().await {
        self.handle_message(message).await?;
        tokio::task::yield_now().await; // Let other tasks run
    }
    Ok(())
}

// ❌ Bad - blocks executor
pub async fn process_queue(&self) -> Result<()> {
    loop {
        let message = self.queue.dequeue_blocking(); // Blocks!
        self.handle_message(message).await?;
    }
}
```

## 🔄 Pull Request Process

### Before Submitting

1. **Ensure tests pass**
   ```bash
   cargo test
   cargo clippy
   cargo fmt --check
   ```

2. **Update documentation** if needed
   - API docs for public functions
   - README for major features  
   - CHANGELOG for user-facing changes

3. **Add examples** for new features

### PR Description Template

```markdown
## Summary
Brief description of the change and motivation.

## Changes
- List of specific changes made
- New features added
- Bug fixes included  

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Examples work
- [ ] Manual testing performed

## Documentation  
- [ ] API docs updated
- [ ] README updated (if needed)
- [ ] CHANGELOG entry added

## Performance Impact
Description of any performance implications.

## Breaking Changes
List any breaking changes and migration path.
```

### Review Process

1. **Automated checks** must pass (CI/CD)
2. **Code review** by maintainer(s)
3. **Manual testing** for significant changes
4. **Documentation review** for user-facing features

## 🏗️ Architecture Overview

Understanding the codebase structure:

```
mqtt-persist/
├── src/
│   ├── lib.rs          # Public API and re-exports
│   ├── client.rs       # Main MqttClient implementation  
│   ├── queue.rs        # Offline message queuing
│   ├── connection.rs   # Connection state management
│   ├── config.rs       # Configuration and builder
│   └── error.rs        # Error types and handling
├── examples/           # Usage examples and demos
├── tests/              # Integration tests
└── docs/               # Additional documentation
```

### Key Design Principles

1. **Simplicity** - Easy to use API with sensible defaults
2. **Reliability** - Message loss prevention and error resilience  
3. **Performance** - Efficient async/await implementation
4. **Extensibility** - Configurable behavior and future expansion
5. **Compatibility** - Works with standard MQTT brokers

### Data Flow

```
Application
    ↓ publish()
MqttClient
    ↓ (via channel)
Background Task
    ├── Connected? → Direct publish  
    └── Offline? → Queue for later
        ↓ (when reconnected)
    Queue Drainer → Broker
```

## 🤝 Communication

- **Issues** - Bug reports and feature requests
- **Discussions** - General questions and ideas
- **Discord/Slack** - Real-time chat (link in README)
- **Email** - Maintainer contact for sensitive issues

## 📜 License

By contributing to mqtt-persist, you agree that your contributions will be licensed under either:
- Apache License, Version 2.0
- MIT license

The same as the project itself.

---

Thank you for contributing to mqtt-persist! 🙏
