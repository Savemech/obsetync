use crate::bridge;
use crate::devices;
use crate::error::ServerError;
use crate::secure;
use crate::state::SharedState;
use crate::storage::{blob_exists, read_blob, write_blob};
use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use sync_core::hash::{hash_bytes, hash_to_hex, hex_to_hash};
use x25519_dalek::StaticSecret;

/// Max body size we'll consume in one shot. Generous to accommodate the
/// occasional large-file blob upload (FastCDC caps chunks at ~1 MB each,
/// but the full-file manifest path can push a megabyte or two).
const MAX_BODY_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB

/// Option-B secure-envelope middleware. Every protected route body is a
/// sealed blob: `[ver | nonce | client_eph_pub | ciphertext+tag]`. We decrypt
/// the request using the server's long-term X25519 private key, validate the
/// bearer token found inside the plaintext, run the inner handler, then
/// encrypt its response back to the same client using the shared secret from
/// the request's ECDH.
async fn secure_envelope(
    State(state): State<SharedState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();

    let (parts, body) = request.into_parts();
    let body_bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::BAD_REQUEST, "request body read failed").into_response(),
    };

    let server_priv = StaticSecret::from(state.server_priv_bytes);
    let decrypted =
        match secure::decrypt_request(&body_bytes, &server_priv, method.as_str(), &path) {
            Ok(d) => d,
            Err(e) => {
                tracing::debug!("decrypt failed on {} {}: {}", method, path, e);
                return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
            }
        };

    let device_id = match devices::lookup_token(&state.layout, &decrypted.bearer_token) {
        Some(id) => id,
        None => return (StatusCode::UNAUTHORIZED, "unknown bearer token").into_response(),
    };
    if devices::is_revoked(&state.layout, &device_id) {
        return (StatusCode::FORBIDDEN, "device revoked").into_response();
    }
    let _ = devices::touch_last_seen(&state.layout, &device_id);

    // Reassemble request with the decrypted inner body for the real handler.
    let mut inner_request = Request::from_parts(parts, Body::from(decrypted.inner_body));
    inner_request
        .headers_mut()
        .remove(axum::http::header::CONTENT_LENGTH);

    let inner_response = next.run(inner_request).await;

    // Capture inner response, encrypt it.
    let (resp_parts, resp_body) = inner_response.into_parts();
    let resp_bytes = match axum::body::to_bytes(resp_body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "response body read failed")
                .into_response()
        }
    };

    let encrypted =
        match secure::encrypt_response(&resp_bytes, &decrypted.shared_secret, method.as_str(), &path) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("encrypt response failed: {}", e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "response encryption failed",
                )
                    .into_response();
            }
        };

    let mut out = Response::from_parts(resp_parts, Body::from(encrypted));
    out.headers_mut().remove(axum::http::header::CONTENT_LENGTH);
    out.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/octet-stream"),
    );
    out
}

pub fn sync_router(state: SharedState) -> Router {
    let protected = Router::new()
        // Root management
        .route("/api/v1/root/{vault_id}", get(get_root).put(put_root))
        // Diff
        .route("/api/v1/diff/{vault_id}", post(post_diff))
        // Index chunks
        .route("/api/v1/chunk/{hash}", get(get_chunk).put(put_chunk))
        .route("/api/v1/chunks/check", post(post_chunks_check))
        // Content (small files)
        .route(
            "/api/v1/content/{hash}",
            get(get_content).put(put_content),
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
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            secure_envelope,
        ));

    Router::new()
        // Health is public (plaintext) — clients ping it before enrollment to
        // check connectivity without needing the server's box pubkey.
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
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    // Option-B transport: parent hash is prepended to the body as a 64-byte
    // ASCII prefix (hex or empty, space-padded) so it's covered by the AEAD
    // envelope like the rest of the request.
    if body.len() < 64 {
        return Err(ServerError::BadRequest(
            "body too short for parent_root prefix".into(),
        ));
    }
    let parent_hex = std::str::from_utf8(&body[..64])
        .map_err(|_| ServerError::BadRequest("parent_root prefix not UTF-8".into()))?
        .trim()
        .to_owned();
    let root_bytes = &body[64..];

    let incoming_root = sync_core::chunk::RootNode::deserialize(root_bytes)
        .map_err(|e| ServerError::BadRequest(format!("invalid root: {}", e)))?;

    let incoming_hash = incoming_root.hash();
    let incoming_bytes = root_bytes.to_vec();

    state
        .vaults
        .store_root(&vault_id, &incoming_hash, &incoming_bytes)?;

    let idx_path = state.layout.index_path(&incoming_hash);
    write_blob(&idx_path, &incoming_bytes)?;

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
            let parent_hash = hex_to_hash(&parent_hex)
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
                let base_data =
                    state
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
                let merge_result =
                    bridge::run_merge(index_base, base_root, current_root, incoming_root)
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
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    // Option-B: device_root hash rides in the first 64 bytes of the encrypted
    // body (ASCII hex, space-padded) so it's covered by the AEAD envelope.
    if body.len() < 64 {
        return Err(ServerError::BadRequest(
            "body too short for device_root prefix".into(),
        ));
    }
    let device_root_hex = std::str::from_utf8(&body[..64])
        .map_err(|_| ServerError::BadRequest("device_root prefix not UTF-8".into()))?
        .trim();

    let device_root_hash = hex_to_hash(device_root_hex)
        .map_err(|_| ServerError::BadRequest("invalid device_root hash".into()))?;

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

async fn post_chunks_check(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let hashes: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("expected JSON array of hashes: {}", e)))?;
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

async fn post_content_check(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let hashes: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("expected JSON array of hashes: {}", e)))?;
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
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let hashes: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("expected JSON array of hashes: {}", e)))?;
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
