use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use sync_core::chunk::ChunkError;

#[derive(Debug)]
#[allow(dead_code)]
pub enum ServerError {
    Io(std::io::Error),
    Chunk(ChunkError),
    NotFound(String),
    BadRequest(String),
    Conflict(String),
    Unauthorized,
    Internal(String),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {}", e),
            Self::Chunk(e) => write!(f, "chunk: {}", e),
            Self::NotFound(msg) => write!(f, "not found: {}", msg),
            Self::BadRequest(msg) => write!(f, "bad request: {}", msg),
            Self::Conflict(msg) => write!(f, "conflict: {}", msg),
            Self::Unauthorized => write!(f, "unauthorized"),
            Self::Internal(msg) => write!(f, "internal: {}", msg),
        }
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, body) = match &self {
            Self::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            Self::Chunk(ChunkError::NotFound(_)) => (StatusCode::NOT_FOUND, self.to_string()),
            Self::Chunk(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()),
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized".into()),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.clone()),
        };
        (status, body).into_response()
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ChunkError> for ServerError {
    fn from(e: ChunkError) -> Self {
        Self::Chunk(e)
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(e: serde_json::Error) -> Self {
        Self::BadRequest(e.to_string())
    }
}
