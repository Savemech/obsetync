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

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    async fn status_and_body(resp: Response) -> (StatusCode, String) {
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, String::from_utf8(body).unwrap_or_default())
    }

    #[tokio::test]
    async fn not_found_renders_404() {
        let (s, b) = status_and_body(ServerError::NotFound("vault X".into()).into_response()).await;
        assert_eq!(s, StatusCode::NOT_FOUND);
        assert!(b.contains("vault X"));
    }

    #[tokio::test]
    async fn bad_request_renders_400() {
        let (s, b) = status_and_body(ServerError::BadRequest("bad json".into()).into_response()).await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        assert!(b.contains("bad json"));
    }

    #[tokio::test]
    async fn conflict_renders_409() {
        let (s, b) = status_and_body(ServerError::Conflict("merge needed".into()).into_response()).await;
        assert_eq!(s, StatusCode::CONFLICT);
        assert!(b.contains("merge needed"));
    }

    #[tokio::test]
    async fn unauthorized_renders_401() {
        let (s, b) = status_and_body(ServerError::Unauthorized.into_response()).await;
        assert_eq!(s, StatusCode::UNAUTHORIZED);
        assert_eq!(b, "unauthorized");
    }

    #[tokio::test]
    async fn internal_renders_500() {
        let (s, b) = status_and_body(ServerError::Internal("oops".into()).into_response()).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(b.contains("oops"));
    }

    #[tokio::test]
    async fn io_error_renders_500() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let (s, b) = status_and_body(ServerError::Io(io_err).into_response()).await;
        assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(b.contains("io"));
    }

    #[tokio::test]
    async fn chunk_not_found_renders_404() {
        let resp =
            ServerError::Chunk(ChunkError::NotFound("aa".into())).into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn chunk_other_error_renders_400() {
        let resp =
            ServerError::Chunk(ChunkError::Deserialize("nope".into())).into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn from_io_error() {
        let io = std::io::Error::other("disk full");
        let se: ServerError = io.into();
        assert!(matches!(se, ServerError::Io(_)));
    }

    #[test]
    fn from_chunk_error() {
        let ce = ChunkError::NotFound("h".into());
        let se: ServerError = ce.into();
        assert!(matches!(se, ServerError::Chunk(_)));
    }

    #[test]
    fn from_serde_json_error_becomes_bad_request() {
        let err = serde_json::from_str::<i32>("not json").unwrap_err();
        let se: ServerError = err.into();
        assert!(matches!(se, ServerError::BadRequest(_)));
    }

    #[test]
    fn display_contains_variant_marker() {
        assert!(format!("{}", ServerError::Unauthorized).contains("unauthorized"));
        assert!(format!("{}", ServerError::NotFound("x".into())).contains("not found"));
        assert!(format!("{}", ServerError::BadRequest("y".into())).contains("bad request"));
        assert!(format!("{}", ServerError::Conflict("z".into())).contains("conflict"));
        assert!(format!("{}", ServerError::Internal("w".into())).contains("internal"));
    }
}
