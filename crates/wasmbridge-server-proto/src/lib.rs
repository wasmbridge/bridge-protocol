pub mod cert;
pub mod registry;
pub mod server;
pub mod auth;

pub mod control_plane {
    tonic::include_proto!("control_plane");
}

use std::sync::Arc;
use tonic::transport::{Server, ServerTlsConfig, Identity};
use crate::control_plane::control_plane_server::ControlPlaneServer;
use crate::registry::ClientRegistry;
use crate::server::CloudControlPlane;
use async_trait::async_trait;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub enum ConnectionEvent {
    Connected(String),
    Disconnected(String),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Claims {
    pub sub: String, // hardware_id / client_id
    pub exp: usize,  // Expiration timestamp
}

#[async_trait]
pub trait TokenValidator: Send + Sync + 'static {
    async fn validate(&self, token: &str) -> Option<Claims>;
}

pub struct DefaultValidator {
    pub token: String,
}

#[async_trait]
impl TokenValidator for DefaultValidator {
    async fn validate(&self, token: &str) -> Option<Claims> {
        if token == self.token {
            Some(Claims {
                sub: "debug-client".to_string(),
                exp: chrono::Utc::now().timestamp() as usize + 3600,
            })
        } else {
            None
        }
    }
}

pub struct CloudHandle {
    registry: Arc<ClientRegistry>,
    event_tx: broadcast::Sender<ConnectionEvent>,
}

impl CloudHandle {
    pub fn subscribe_events(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.event_tx.subscribe()
    }

    pub async fn send_command(&self, client_id: &str, command: crate::control_plane::CloudCommand) -> Result<(), String> {
        self.registry.send_command(client_id, command).await
    }

    pub async fn send_command_await(&self, client_id: &str, command: crate::control_plane::CloudCommand) -> Result<crate::control_plane::CommandResponse, String> {
        self.registry.send_command_await(client_id, command).await
    }

    pub fn list_clients(&self) -> Vec<String> {
        self.registry.list_clients()
    }
}

pub struct CloudServerBuilder {
    port: u16,
    validator: Arc<dyn TokenValidator>,
    cert_path: Option<String>,
    key_path: Option<String>,
    command_buffer_size: usize,
}

impl CloudServerBuilder {
    pub fn new() -> Self {
        #[cfg(not(debug_assertions))]
        let default_token = "".to_string(); // Force user to provide token in release
        #[cfg(debug_assertions)]
        let default_token = "debug-token-123".to_string();

        Self {
            port: 50051,
            validator: Arc::new(DefaultValidator { token: default_token }),
            cert_path: None,
            key_path: None,
            command_buffer_size: 100,
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn validator(mut self, validator: Arc<dyn TokenValidator>) -> Self {
        self.validator = validator;
        self
    }

    pub fn command_buffer_size(mut self, size: usize) -> Self {
        self.command_buffer_size = size;
        self
    }

    pub fn with_certificates(mut self, cert_pem_path: String, key_pem_path: String) -> Self {
        self.cert_path = Some(cert_pem_path);
        self.key_path = Some(key_pem_path);
        self
    }

    pub async fn build_and_spawn(self) -> Result<CloudHandle, Box<dyn std::error::Error + Send + Sync>> {
        let (event_tx, _) = broadcast::channel(100);
        let registry = Arc::new(ClientRegistry::new(event_tx.clone()));
        let registry_clone = registry.clone();
        
        let addr = format!("0.0.0.0:{}", self.port).parse()?;
        
        // TLS Setup
        let (cert, key) = if let (Some(c), Some(k)) = (self.cert_path, self.key_path) {
            (std::fs::read_to_string(c)?, std::fs::read_to_string(k)?)
        } else {
            let paths = cert::ensure_certificates()?;
            (std::fs::read_to_string(paths.cert)?, std::fs::read_to_string(paths.key)?)
        };

        let identity = Identity::from_pem(cert, key);
        let tls_config = ServerTlsConfig::new().identity(identity);

        let service = ControlPlaneServer::new(CloudControlPlane { 
            registry: registry_clone,
            validator: self.validator,
        });

        // Wrap service with GrpcWebLayer for WebSocket support
        let service = tonic_web::enable(service);

        println!("[CloudBridge] Starting Cloud Server on {}", addr);

        let registry_for_eviction = registry.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            loop {
                interval.tick().await;
                registry_for_eviction.evict_stale_clients(std::time::Duration::from_secs(120));
            }
        });

        tokio::spawn(async move {
            Server::builder()
                .accept_http1(true) // Required for tonic-web
                .tls_config(tls_config).unwrap()
                .add_service(service)
                .serve(addr)
                .await
                .expect("Cloud Server crashed");
        });

        Ok(CloudHandle { registry, event_tx })
    }
}
