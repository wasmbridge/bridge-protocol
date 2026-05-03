# Reverse Push

`reverse_push` is a Rust library providing a gRPC-based client for a "Reverse Push" architecture.

## What is it and what is it for?

Typically, if a central cloud server wants to send commands to a remote host, the host must expose an open port (acting as a server). However, hosts (agents, nodes, IoT devices) are often deployed behind firewalls or Network Address Translators (NAT), meaning they cannot accept incoming connections.

The **Reverse Push** architecture solves this by having the host act as a gRPC client. The host establishes a long-lived outbound connection (a bi-directional stream) to the central Cloud Control Plane. Because the connection is initiated outbound, it easily traverses firewalls and NATs. Once the connection is established, the server can use this open stream to "push" commands down to the host asynchronously. 

This library provides a robust, auto-reconnecting client to maintain this long-lived stream. It allows you to:
1. Connect to a gRPC Control Plane.
2. Automatically handle reconnections and backoffs if the network drops.
3. Push events and logs to the cloud.
4. Receive and execute commands pushed by the cloud.

## Features

* **Bi-directional gRPC Streaming**: Full duplex communication over a single connection.
* **Auto-reconnect**: Robust background loop with exponential backoff on disconnects.
* **Authentication**: Supports sending JWT Bearer tokens with every connection attempt.
* **TLS Support**: Can be configured with custom TLS certificates.

## How to use

Add this library to your project. Then, use the `ReversePushBuilder` to configure and instantiate the client.

### Example usage

```rust
use reverse_push::ReversePushBuilder;
use reverse_push::control_plane::{ClientEvent, client_event, LogMessage};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 1. Configure and build the client
    let mut client = ReversePushBuilder::new()
        .endpoint("http://127.0.0.1:50051")
        .client_id("my-agent-123")
        .keep_alive_interval(Duration::from_secs(30))
        // .with_token(Some("my-jwt-token".to_string())) // Optional auth
        .build_and_run()
        .await?;

    println!("Reverse Push Client started!");

    // 2. Send an initial event (e.g., a log message)
    let log_event = ClientEvent {
        event: Some(client_event::Event::Log(LogMessage {
            level: "INFO".to_string(),
            message: "Agent started successfully".to_string(),
        })),
    };
    client.send_event(log_event).await?;

    // 3. Listen for incoming commands from the cloud
    while let Some(command) = client.receive_command().await {
        println!("Received command from cloud: {:?}", command);
        
        // Handle the command (e.g., execute a plugin, update config)
        // ...
        
        // You can also send a response back using client.send_event(...)
    }

    println!("Connection closed or host terminated.");
    Ok(())
}
```

## Running the Mock Server

This project includes a mock server for local testing. You can run it with:

```bash
cargo run --bin mock_server
```

It will listen on `127.0.0.1:50051` and periodically push test commands to any connected client.
