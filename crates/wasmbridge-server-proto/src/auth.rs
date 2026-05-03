use tonic::{Request, Status};

#[derive(Clone)]
pub struct AuthInterceptor {
    pub valid_token: String,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        // Проверка заголовка Authorization
        match request.metadata().get("authorization") {
            Some(token) if token.to_str().unwrap_or_default() == format!("Bearer {}", self.valid_token) => {
                Ok(request)
            }
            _ => {
                println!("[CloudBridge] Rejected connection: Invalid or missing token");
                Err(Status::unauthenticated("Invalid or missing Bearer token"))
            }
        }
    }
}
