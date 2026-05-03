use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming, transport::Server};
use wasmbridge_client_proto::control_plane::cloud_command::Command;
use wasmbridge_client_proto::control_plane::control_plane_server::{
    ControlPlane, ControlPlaneServer,
};
use wasmbridge_client_proto::control_plane::{ClientEvent, CloudCommand, ExecutePluginAction};

#[derive(Default)]
pub struct MockControlPlane {}

#[tonic::async_trait]
impl ControlPlane for MockControlPlane {
    type StreamCommandsStream = ReceiverStream<Result<CloudCommand, Status>>;

    async fn stream_commands(
        &self,
        request: Request<Streaming<ClientEvent>>,
    ) -> Result<Response<Self::StreamCommandsStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(10);

        // Task for reading events from the client
        tokio::spawn(async move {
            while let Ok(Some(event)) = inbound.message().await {
                println!("[MockServer] Received from client: {:?}", event);
            }
            println!("[MockServer] Client disconnected");
        });

        // Task for sending mock commands to the client
        tokio::spawn(async move {
            println!("[MockServer] Waiting 5 seconds before sending mock command...");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            let cmd = CloudCommand {
                command_id: "test-cmd-123".to_string(),
                command: Some(Command::ExecutePlugin(ExecutePluginAction {
                    target_plugin: "example-plugin".to_string(),
                    action: "execute_command".to_string(),
                    payload: b"{\"task\": \"get_metrics\", \"args\": {}}".to_vec(),
                })),
            };

            println!("[MockServer] Sending ExecutePlugin command to client...");
            if tx.send(Ok(cmd)).await.is_err() {
                eprintln!("[MockServer] Failed to send command, client might have disconnected");
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let server = MockControlPlane::default();

    println!(
        "[MockServer] Starting Mock Control Plane Server on {}",
        addr
    );

    Server::builder()
        .add_service(ControlPlaneServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
