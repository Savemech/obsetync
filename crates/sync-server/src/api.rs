use axum::{
    Router,
    extract::{Path, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::IntoResponse,
    routing::{get, post},
};
use sync_core::hash::{hash_bytes, hex_to_hash, hash_to_hex};
use crate::devices;
use crate::error::ServerError;
use crate::state::SharedState;
use crate::storage::{blob_exists, read_blob, write_blob};
use crate::bridge;

/// Bearer-token auth middleware. Every sync API route (except /health) requires
/// a valid `Authorization: Bearer <token>` header.
/// Desktop clients send their token alongside the mTLS cert (double auth).
/// Mobile clients (iOS) send the token only — no client cert from JS.
async fn require_bearer(
    State(state): State<SharedState>,
    request: Request,
    next: Next,
) -> impl IntoResponse {
    let token = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    match token {
        Some(t) if devices::lookup_token(&state.layout, t).is_some() => {
            next.run(request).await.into_response()
        }
        _ => StatusCode::UNAUTHORIZED.into_response(),
    }
}

pub fn sync_router(state: SharedState) -> Router {
    // All API routes require bearer token auth.
    let protected = Router::new()
        // Root management
        .route("/api/v1/root/{vault_id}", get(get_root).put(put_root))
        // Diff
        .route("/api/v1/diff/{vault_id}", post(post_diff))
        // Index chunks
        .route(
            "/api/v1/chunk/{hash}",
            get(get_chunk).put(put_chunk).head(head_chunk),
        )
        .route("/api/v1/chunks/check", post(post_chunks_check))
        // Content (small files)
        .route(
            "/api/v1/content/{hash}",
            get(get_content).put(put_content).head(head_content),
        )
        .route("/api/v1/content/check", post(post_content_check))
        // Content manifests (large files)
        .route(
            "/api/v1/content/manifest/{hash}",
            get(get_manifest).put(put_manifest),
        )
        // Content sub-file chunks
        .route(
            "/api/v1/content/chunk/{hash}",
            get(get_content_chunk).put(put_content_chunk),
        )
        .route(
            "/api/v1/content/chunks/check",
            post(post_content_chunks_check),
        )
        .layer(axum::middleware::from_fn_with_state(state.clone(), require_bearer));

    Router::new()
        // Health is public — used by ping() before enrollment.
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

// --- Health ---

async fn health() -> &'static str {
    "{\"ok\":true}"
}

// --- Root Management ---

async fn get_root(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash = state
        .vaults
        .get_current_root(&vault_id)
        .ok_or_else(|| ServerError::NotFound(format!("vault '{}' not found", vault_id)))?;

    let data = state
        .vaults
        .get_root(&vault_id, &hash)
        .ok_or_else(|| ServerError::NotFound("root data missing".into()))?;

    Ok((StatusCode::OK, data))
}

async fn put_root(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    // Parse the incoming root to validate it.
    let incoming_root = sync_core::chunk::RootNode::deserialize(&body)
        .map_err(|e| ServerError::BadRequest(format!("invalid root: {}", e)))?;

    let incoming_hash = incoming_root.hash();
    let incoming_bytes = body.to_vec();

    // Store the root in history.
    state
        .vaults
        .store_root(&vault_id, &incoming_hash, &incoming_bytes)?;

    // Also store it as an index chunk so merge/diff can find it.
    let idx_path = state.layout.index_path(&incoming_hash);
    write_blob(&idx_path, &incoming_bytes)?;

    // Get parent hash from header.
    let parent_hex = headers
        .get("x-parent-root")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let current_root_hash = state.vaults.get_current_root(&vault_id);

    match current_root_hash {
        None => {
            // First push — accept directly.
            state.vaults.set_current_root(&vault_id, &incoming_hash)?;
            Ok((
                StatusCode::OK,
                serde_json::json!({
                    "accepted": true,
                    "root_hash": hash_to_hex(&incoming_hash),
                })
                .to_string(),
            ))
        }
        Some(current_hash) => {
            let parent_hash = hex_to_hash(parent_hex)
                .map_err(|_| ServerError::BadRequest("invalid X-Parent-Root header".into()))?;

            if current_hash == parent_hash {
                // Fast-forward — parent matches current, accept directly.
                state.vaults.set_current_root(&vault_id, &incoming_hash)?;
                Ok((
                    StatusCode::OK,
                    serde_json::json!({
                        "accepted": true,
                        "root_hash": hash_to_hex(&incoming_hash),
                    })
                    .to_string(),
                ))
            } else {
                // Diverged — need to merge.
                let current_data = state
                    .vaults
                    .get_root(&vault_id, &current_hash)
                    .ok_or_else(|| ServerError::Internal("current root data missing".into()))?;

                let current_root = sync_core::chunk::RootNode::deserialize(&current_data)
                    .map_err(|e| ServerError::Internal(format!("corrupt current root: {}", e)))?;

                // Find the base (common ancestor).
                // For now, use the parent hash as the base.
                // TODO: walk parent chain to find true common ancestor.
                let base_data = state
                    .vaults
                    .get_root(&vault_id, &parent_hash)
                    .ok_or_else(|| {
                        ServerError::BadRequest(
                            "parent root not found in history — full rescan needed".into(),
                        )
                    })?;

                let base_root = sync_core::chunk::RootNode::deserialize(&base_data)
                    .map_err(|e| ServerError::Internal(format!("corrupt base root: {}", e)))?;

                // Run merge via the bridge (handles !Send).
                let index_base = state.layout.base.join("index");
                let merge_result = bridge::run_merge(
                    index_base,
                    base_root,
                    current_root,
                    incoming_root,
                )
                .await
                .map_err(|e| ServerError::Internal(format!("merge failed: {}", e)))?;

                let merged_hash = merge_result.new_root.hash();
                let merged_bytes = merge_result.new_root.serialize();

                // Store merged root.
                state
                    .vaults
                    .store_root(&vault_id, &merged_hash, &merged_bytes)?;
                let idx_path = state.layout.index_path(&merged_hash);
                write_blob(&idx_path, &merged_bytes)?;

                // Update current.
                state.vaults.set_current_root(&vault_id, &merged_hash)?;

                let conflicts: Vec<_> = merge_result
                    .file_conflicts
                    .iter()
                    .map(|c| {
                        serde_json::json!({
                            "path": c.path,
                            "base_hash": hash_to_hex(&c.base_hash),
                            "side_a_hash": hash_to_hex(&c.side_a_hash),
                            "side_b_hash": hash_to_hex(&c.side_b_hash),
                        })
                    })
                    .collect();

                Ok((
                    StatusCode::OK,
                    serde_json::json!({
                        "merged": true,
                        "root_hash": hash_to_hex(&merged_hash),
                        "conflicts": conflicts,
                        "auto_resolved": merge_result.auto_resolved_count,
                    })
                    .to_string(),
                ))
            }
        }
    }
}

// --- Diff ---

async fn post_diff(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let device_root_hex = headers
        .get("x-device-root")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ServerError::BadRequest("missing X-Device-Root header".into()))?;

    let device_root_hash = hex_to_hash(device_root_hex)
        .map_err(|_| ServerError::BadRequest("invalid X-Device-Root hash".into()))?;

    let current_hash = state
        .vaults
        .get_current_root(&vault_id)
        .ok_or_else(|| ServerError::NotFound(format!("vault '{}' not found", vault_id)))?;

    // Same root — no changes.
    if device_root_hash == current_hash {
        return Ok((StatusCode::NOT_MODIFIED, "[]".to_string()));
    }

    // Load both roots.
    let device_root_data = state
        .vaults
        .get_root(&vault_id, &device_root_hash)
        .ok_or_else(|| {
            ServerError::BadRequest("device root not found in history — full rescan needed".into())
        })?;

    let current_data = state
        .vaults
        .get_root(&vault_id, &current_hash)
        .ok_or_else(|| ServerError::Internal("current root data missing".into()))?;

    let from_root = sync_core::chunk::RootNode::deserialize(&device_root_data)
        .map_err(|e| ServerError::Internal(format!("corrupt device root: {}", e)))?;

    let to_root = sync_core::chunk::RootNode::deserialize(&current_data)
        .map_err(|e| ServerError::Internal(format!("corrupt current root: {}", e)))?;

    // Compute deltas via bridge.
    let index_base = state.layout.base.join("index");
    let deltas = bridge::run_diff(index_base, from_root, to_root)
        .await
        .map_err(|e| ServerError::Internal(format!("diff failed: {}", e)))?;

    let json = serde_json::to_string(&deltas)?;
    Ok((StatusCode::OK, json))
}

// --- Index Chunks ---

async fn get_chunk(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let path = state.layout.index_path(&hash);
    let data = read_blob(&path)
        .ok_or_else(|| ServerError::NotFound(format!("chunk {} not found", hash_hex)))?;
    Ok((StatusCode::OK, data))
}

async fn put_chunk(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let expected =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let actual = hash_bytes(&body);
    if expected != actual {
        return Err(ServerError::BadRequest(format!(
            "hash mismatch: expected {}, got {}",
            hash_hex,
            hash_to_hex(&actual)
        )));
    }
    let path = state.layout.index_path(&expected);
    write_blob(&path, &body)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn head_chunk(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    if blob_exists(&state.layout.index_path(&hash)) {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

async fn post_chunks_check(
    State(state): State<SharedState>,
    axum::Json(hashes): axum::Json<Vec<String>>,
) -> Result<impl IntoResponse, ServerError> {
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| !blob_exists(&state.layout.index_path(&hash)))
                .unwrap_or(false)
        })
        .collect();
    Ok(axum::Json(serde_json::json!({ "needed": needed })))
}

// --- Content (small files) ---

async fn get_content(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let path = state.layout.content_blob_path(&hash);
    let data = read_blob(&path)
        .ok_or_else(|| ServerError::NotFound(format!("content {} not found", hash_hex)))?;
    Ok((StatusCode::OK, data))
}

async fn put_content(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let expected =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let actual = hash_bytes(&body);
    if expected != actual {
        return Err(ServerError::BadRequest("hash mismatch".into()));
    }
    let path = state.layout.content_blob_path(&expected);
    write_blob(&path, &body)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn head_content(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    if blob_exists(&state.layout.content_blob_path(&hash)) {
        Ok(StatusCode::OK)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

async fn post_content_check(
    State(state): State<SharedState>,
    axum::Json(hashes): axum::Json<Vec<String>>,
) -> Result<impl IntoResponse, ServerError> {
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| !blob_exists(&state.layout.content_blob_path(&hash)))
                .unwrap_or(false)
        })
        .collect();
    Ok(axum::Json(serde_json::json!({ "needed": needed })))
}

// --- Content Manifests ---

async fn get_manifest(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let path = state.layout.content_manifest_path(&hash);
    let data = read_blob(&path)
        .ok_or_else(|| ServerError::NotFound(format!("manifest {} not found", hash_hex)))?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        data,
    ))
}

async fn put_manifest(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    // Validate it's valid JSON (stored as-is; hashes are hex strings from the plugin).
    let _: serde_json::Value = serde_json::from_slice(&body)?;
    let path = state.layout.content_manifest_path(&hash);
    write_blob(&path, &body)?;
    Ok(StatusCode::NO_CONTENT)
}

// --- Content Sub-File Chunks ---

async fn get_content_chunk(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let path = state.layout.content_chunk_path(&hash);
    let data = read_blob(&path)
        .ok_or_else(|| ServerError::NotFound(format!("content chunk {} not found", hash_hex)))?;
    Ok((StatusCode::OK, data))
}

async fn put_content_chunk(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let expected =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let actual = hash_bytes(&body);
    if expected != actual {
        return Err(ServerError::BadRequest("hash mismatch".into()));
    }
    let path = state.layout.content_chunk_path(&expected);
    write_blob(&path, &body)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_content_chunks_check(
    State(state): State<SharedState>,
    axum::Json(hashes): axum::Json<Vec<String>>,
) -> Result<impl IntoResponse, ServerError> {
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| !blob_exists(&state.layout.content_chunk_path(&hash)))
                .unwrap_or(false)
        })
        .collect();
    Ok(axum::Json(serde_json::json!({ "needed": needed })))
}
