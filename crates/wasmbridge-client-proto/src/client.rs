//! Client implementation for the Reverse Push architecture.
//!
//! This module provides a robust, reconnecting client that establishes
//! a bi-directional gRPC stream to the Control Plane. It allows the cloud
//! to send commands to the client securely over a single outbound connection.

use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;

use crate::control_plane::{
    ClientEvent, CloudCommand, RegistrationRequest, client_event,
    control_plane_client::ControlPlaneClient,
};

/// Builder for creating a configured [`ReversePushClient`].
///
/// Use this builder to set the endpoint, client identifier, keep-alive
/// settings, and authentication parameters (JWT and TLS) before establishing
/// the connection to the control plane.
pub struct ReversePushBuilder {
    endpoint: String,
    client_id: String,
    keep_alive_interval: Duration,
    jwt_token: Option<String>,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
}

impl Default for ReversePushBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ReversePushBuilder {
    pub fn new() -> Self {
        Self {
            endpoint: "http://127.0.0.1:50051".to_string(),
            client_id: "default-client".to_string(),
            keep_alive_interval: Duration::from_secs(30),
            jwt_token: None,
            tls_config: None,
        }
    }

    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    pub fn keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.jwt_token = token;
        self
    }

    pub fn with_tls_config(mut self, config: tonic::transport::ClientTlsConfig) -> Self {
        self.tls_config = Some(config);
        self
    }

    pub async fn build_and_run(
        self,
    ) -> Result<ReversePushClient, Box<dyn std::error::Error + Send + Sync>> {
        let (tx_events, rx_events) = mpsc::channel::<ClientEvent>(100);
        let (tx_commands, rx_commands) = mpsc::channel::<CloudCommand>(100);

        let endpoint_url = self.endpoint.clone();
        let client_id = self.client_id.clone();
        let keep_alive = self.keep_alive_interval;
        let jwt_token = self.jwt_token.clone();
        let tls_config = self.tls_config.clone();

        // Start the background connection keep-alive task
        tokio::spawn(async move {
            run_connection_loop(
                endpoint_url,
                client_id,
                keep_alive,
                jwt_token,
                tls_config,
                rx_events,
                tx_commands,
            )
            .await;
        });

        Ok(ReversePushClient {
            tx_events,
            rx_commands,
        })
    }
}

/// The main client handle for the Reverse Push architecture.
///
/// It provides channels to asynchronously send events to the cloud
/// and receive incoming commands from the cloud over the established stream.
pub struct ReversePushClient {
    tx_events: mpsc::Sender<ClientEvent>,
    rx_commands: mpsc::Receiver<CloudCommand>,
}

impl ReversePushClient {
    /// Sends an event or a command response to the cloud control plane.
    pub async fn send_event(
        &self,
        event: ClientEvent,
    ) -> Result<(), mpsc::error::SendError<ClientEvent>> {
        self.tx_events.send(event).await
    }

    /// Updates the JWT token for the current active stream without reconnecting.
    pub async fn refresh_token(
        &self,
        new_jwt: String,
    ) -> Result<(), mpsc::error::SendError<ClientEvent>> {
        let refresh_event = ClientEvent {
            event: Some(client_event::Event::Refresh(
                crate::control_plane::TokenRefresh { new_jwt },
            )),
        };
        self.send_event(refresh_event).await
    }

    /// Asynchronously awaits the next command pushed from the cloud.
    pub async fn receive_command(&mut self) -> Option<CloudCommand> {
        self.rx_commands.recv().await
    }
}

async fn run_connection_loop(
    endpoint_url: String,
    client_id: String,
    keep_alive: Duration,
    jwt_token: Option<String>,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
    mut rx_events: mpsc::Receiver<ClientEvent>,
    tx_commands: mpsc::Sender<CloudCommand>,
) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        match connect_to_server(
            &endpoint_url,
            keep_alive,
            jwt_token.clone(),
            tls_config.clone(),
        )
        .await
        {
            Ok(client) => {
                println!("[ReversePush] Connected to {}", endpoint_url);
                backoff = Duration::from_secs(1); // Reset backoff delay on successful connection

                let should_reconnect =
                    handle_bi_di_stream(client, &client_id, &mut rx_events, &tx_commands).await;

                if !should_reconnect {
                    eprintln!("[ReversePush] Host channel closed, terminating connection loop");
                    return;
                }
            }
            Err(e) => {
                eprintln!(
                    "[ReversePush] Connection failed: {}. Retrying in {:?}...",
                    e, backoff
                );
            }
        }

        // Wait before attempting to reconnect
        sleep(backoff).await;
        backoff = std::cmp::min(backoff * 2, max_backoff);
    }
}

async fn connect_to_server(
    endpoint_url: &str,
    keep_alive: Duration,
    jwt_token: Option<String>,
    tls_config: Option<tonic::transport::ClientTlsConfig>,
) -> Result<
    ControlPlaneClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            TokenInterceptor,
        >,
    >,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let mut endpoint = Endpoint::from_shared(endpoint_url.to_string())?
        .tcp_keepalive(Some(keep_alive))
        .http2_keep_alive_interval(keep_alive);

    if let Some(tls) = tls_config {
        endpoint = endpoint.tls_config(tls)?;
    }

    let channel = endpoint.connect().await?;

    let interceptor = TokenInterceptor { token: jwt_token };
    Ok(ControlPlaneClient::with_interceptor(channel, interceptor))
}

#[derive(Clone)]
struct TokenInterceptor {
    token: Option<String>,
}

impl tonic::service::Interceptor for TokenInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(token) = &self.token {
            let bearer_token = format!("Bearer {}", token);
            req.metadata_mut().insert(
                "authorization",
                bearer_token
                    .parse()
                    .map_err(|_| tonic::Status::invalid_argument("Invalid token format"))?,
            );
        }
        Ok(req)
    }
}

/// Handles the bi-directional gRPC stream loop.
///
/// Returns `true` if a reconnection is required (e.g., due to a network error or the server closing the stream).
/// Returns `false` if the host application disconnected its channels, indicating the loop should terminate gracefully.
async fn handle_bi_di_stream<S>(
    mut client: ControlPlaneClient<S>,
    client_id: &str,
    rx_events: &mut mpsc::Receiver<ClientEvent>,
    tx_commands: &mpsc::Sender<CloudCommand>,
) -> bool
where
    S: tonic::client::GrpcService<tonic::body::BoxBody>,
    S::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    <S::ResponseBody as tonic::codegen::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    let (tx_stream, rx_stream) = mpsc::channel::<ClientEvent>(100);

    let reg_event = ClientEvent {
        event: Some(client_event::Event::Register(RegistrationRequest {
            client_id: client_id.to_string(),
            version: "0.2.0".to_string(),
        })),
    };

    if tx_stream.send(reg_event).await.is_err() {
        eprintln!("[ReversePush] Failed to send registration event locally");
        return true;
    }

    let request = tonic::Request::new(ReceiverStream::new(rx_stream));

    let mut inbound_stream = match client.stream_commands(request).await {
        Ok(response) => response.into_inner(),
        Err(e) => {
            eprintln!("[ReversePush] Failed to start stream: {}", e);
            return true;
        }
    };

    println!("[ReversePush] Stream established successfully");

    loop {
        tokio::select! {
            msg = inbound_stream.message() => {
                match msg {
                    Ok(Some(cmd)) => {
                        if tx_commands.send(cmd).await.is_err() {
                            return false; // Host application has terminated
                        }
                    }
                    Ok(None) => {
                        eprintln!("[ReversePush] Server closed the stream");
                        return true;
                    }
                    Err(e) => {
                        eprintln!("[ReversePush] Stream error: {}", e);
                        return true;
                    }
                }
            }
            host_event = rx_events.recv() => {
                match host_event {
                    Some(event) => {
                        if tx_stream.send(event).await.is_err() {
                            eprintln!("[ReversePush] Failed to push event to local stream");
                            return true;
                        }
                    }
                    None => {
                        return false; // Host application has terminated
                    }
                }
            }
        }
    }
}
