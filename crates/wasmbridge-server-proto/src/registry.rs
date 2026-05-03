use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;
use crate::control_plane::{CloudCommand, CommandResponse};

use std::collections::VecDeque;
use std::time::Instant;

pub type CommandSender = mpsc::Sender<Result<CloudCommand, Status>>;

pub struct ClientRegistry {
    // client_id -> sender
    clients: DashMap<String, CommandSender>,
    // client_id -> queue of commands for offline clients
    pending_commands: DashMap<String, VecDeque<CloudCommand>>,
    // client_id -> last activity time
    last_seen: DashMap<String, Instant>,
    // command_id -> response sender
    pending_responses: DashMap<String, oneshot::Sender<CommandResponse>>,
    event_tx: tokio::sync::broadcast::Sender<crate::ConnectionEvent>,
}

impl ClientRegistry {
    pub fn new(event_tx: tokio::sync::broadcast::Sender<crate::ConnectionEvent>) -> Self {
        Self {
            clients: DashMap::new(),
            pending_commands: DashMap::new(),
            last_seen: DashMap::new(),
            pending_responses: DashMap::new(),
            event_tx,
        }
    }

    pub fn register(&self, client_id: String, sender: CommandSender) {
        let anon_id = if client_id.len() > 8 { &client_id[..8] } else { &client_id };
        println!("[CloudBridge] Registering client: {}...", anon_id);
        self.clients.insert(client_id.clone(), sender.clone());
        self.last_seen.insert(client_id.clone(), Instant::now());
        
        // Flush pending commands
        if let Some((_, mut queue)) = self.pending_commands.remove(&client_id) {
            println!("[CloudBridge] Flushing {} pending commands for {}", queue.len(), client_id);
            while let Some(cmd) = queue.pop_front() {
                let _ = sender.try_send(Ok(cmd));
            }
        }

        let _ = self.event_tx.send(crate::ConnectionEvent::Connected(client_id));
    }

    pub fn unregister(&self, client_id: &str) {
        let anon_id = if client_id.len() > 8 { &client_id[..8] } else { &client_id };
        println!("[CloudBridge] Unregistering client: {}...", anon_id);
        self.last_seen.remove(client_id);
        if self.clients.remove(client_id).is_some() {
            let _ = self.event_tx.send(crate::ConnectionEvent::Disconnected(client_id.to_string()));
        }
    }

    pub fn update_activity(&self, client_id: &str) {
        self.last_seen.insert(client_id.to_string(), Instant::now());
    }

    pub fn evict_stale_clients(&self, timeout: std::time::Duration) {
        let now = Instant::now();
        let stale_ids: Vec<String> = self.last_seen
            .iter()
            .filter(|kv| now.duration_since(*kv.value()) > timeout)
            .map(|kv| kv.key().clone())
            .collect();

        for id in stale_ids {
            println!("[CloudBridge] Evicting stale client: {}", id);
            self.unregister(&id);
        }
    }

    pub async fn send_command(&self, client_id: &str, command: CloudCommand) -> Result<(), String> {
        if let Some(sender) = self.clients.get(client_id) {
            sender.send(Ok(command)).await.map_err(|e| format!("Failed to send: {}", e))
        } else {
            // Queue command for offline client
            println!("[CloudBridge] Client {} offline, queueing command", client_id);
            self.pending_commands
                .entry(client_id.to_string())
                .or_insert_with(VecDeque::new)
                .push_back(command);
            Ok(())
        }
    }

    pub async fn send_command_await(&self, client_id: &str, command: CloudCommand) -> Result<CommandResponse, String> {
        let (tx, rx) = oneshot::channel();
        let cmd_id = command.command_id.clone();
        
        self.pending_responses.insert(cmd_id.clone(), tx);
        
        if let Err(e) = self.send_command(client_id, command).await {
            self.pending_responses.remove(&cmd_id);
            return Err(e);
        }

        // Ждем ответа (с таймаутом 10 сек)
        match tokio::time::timeout(std::time::Duration::from_secs(10), rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err("Response channel closed".to_string()),
            Err(_) => {
                self.pending_responses.remove(&cmd_id);
                Err("Response timeout".to_string())
            }
        }
    }

    pub fn complete_command(&self, response: CommandResponse) {
        if let Some((_, tx)) = self.pending_responses.remove(&response.command_id) {
            let _ = tx.send(response);
        }
    }

    pub fn list_clients(&self) -> Vec<String> {
        self.clients.iter().map(|kv| kv.key().clone()).collect()
    }
}
