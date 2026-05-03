use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use crate::control_plane::control_plane_server::ControlPlane;
use crate::control_plane::{ClientEvent, CloudCommand, client_event};
use crate::registry::ClientRegistry;

pub struct CloudControlPlane {
    pub registry: Arc<ClientRegistry>,
    pub validator: Arc<dyn crate::TokenValidator>,
}

#[tonic::async_trait]
impl ControlPlane for CloudControlPlane {
    type StreamCommandsStream = ReceiverStream<Result<CloudCommand, Status>>;

    async fn stream_commands(
        &self,
        request: Request<Streaming<ClientEvent>>,
    ) -> Result<Response<Self::StreamCommandsStream>, Status> {
        // Извлекаем токен из метаданных
        let token = match request.metadata().get("authorization") {
            Some(t) => {
                let s = t.to_str().map_err(|_| Status::unauthenticated("Invalid token format"))?;
                if s.starts_with("Bearer ") {
                    &s[7..]
                } else {
                    return Err(Status::unauthenticated("Bearer token required"));
                }
            }
            None => return Err(Status::unauthenticated("Missing authorization token")),
        };

        // Валидация токена
        let claims = match self.validator.validate(token).await {
            Some(c) => c,
            None => {
                println!("[CloudBridge] Rejected connection: Invalid token");
                return Err(Status::unauthenticated("Invalid token"));
            }
        };

        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(100);
        let registry = self.registry.clone();
        let validator = self.validator.clone();
        
        // Переменная для хранения ID клиента после его регистрации
        let mut current_client_id = String::new();
        let mut hardware_id_from_token = claims.sub.clone();

        // Spawn eviction task if it's the first connection (or just once)
        // Note: In a real app, this would be started once at server boot.
        // For minimal changes, we'll ensure the registry handles it.

        tokio::spawn(async move {
            while let Ok(Some(event)) = inbound.message().await {
                if let Some(inner_event) = event.event {
                    match inner_event {
                        client_event::Event::Register(reg) => {
                            // Верификация hardware_id (client_id) против токена
                            if reg.client_id != hardware_id_from_token {
                                println!("[CloudServer] Security Alert! client_id mismatch: '{}' vs token '{}'", reg.client_id, hardware_id_from_token);
                                break; // Close connection
                            }

                            current_client_id = reg.client_id.clone();
                            registry.register(reg.client_id, tx.clone());
                            
                            // Анонимизированный лог
                            let anonymized_id = if current_client_id.len() > 8 { &current_client_id[..8] } else { &current_client_id };
                            println!("[CloudServer] Client '{}...' registered (version: {})", anonymized_id, reg.version);
                        }
                        client_event::Event::Ping(_hb) => {
                            registry.update_activity(&current_client_id);
                        }
                        client_event::Event::Refresh(refresh) => {
                            // Обновление токена в процессе работы
                            if let Some(new_claims) = validator.validate(&refresh.new_jwt).await {
                                if new_claims.sub == hardware_id_from_token {
                                    println!("[CloudServer] Token refreshed for client {}", current_client_id);
                                    hardware_id_from_token = new_claims.sub;
                                } else {
                                    println!("[CloudServer] Token refresh failed: hardware_id mismatch");
                                }
                            }
                        }
                        client_event::Event::Response(resp) => {
                            registry.complete_command(resp);
                        }
                        client_event::Event::Log(log) => {
                            println!("[CloudServer] Log from {}: [{}] {}", current_client_id, log.level, log.message);
                        }
                    }
                }
            }
            
            // Если цикл завершился, удаляем клиента из реестра
            if !current_client_id.is_empty() {
                registry.unregister(&current_client_id);
            }
            println!("[CloudServer] Connection closed for client '{}'", current_client_id);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
