use crate::bridge;
use crate::devices;
use crate::error::ServerError;
use crate::secure;
use crate::state::SharedState;
use crate::storage::{blob_exists, read_blob, write_blob};
use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderMap, Method, StatusCode},
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

/// Secure-envelope middleware. Every protected route body is a sealed
/// blob: `[ver | nonce | client_eph_pub | ciphertext+tag]`. We decrypt
/// the request using the server's long-term X25519 private key, validate
/// the bearer token found inside the plaintext, run the inner handler,
/// then encrypt its response back to the same client using the shared
/// secret from the request's ECDH.
///
/// Protocol specification: ../../../docs/transport.md
async fn secure_envelope(
    State(state): State<SharedState>,
    request: Request,
    next: Next,
) -> Response {
    // The plugin uploads every request as HTTP POST (Obsidian's requestUrl
    // on iOS silently drops the body on GET, so we tunnel all verbs through
    // POST) and carries the *semantic* method in `X-Obsetync-Method`. That
    // same semantic method is baked into the AEAD AAD on the client, so we
    // MUST use it here — both to verify the envelope AND to restore the
    // inner request's HTTP method before axum's router dispatches.
    let path = request.uri().path().to_owned();
    let started = std::time::Instant::now();

    let wire_method = request.method().clone();
    let raw_header = request
        .headers()
        .get("X-Obsetync-Method")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    let semantic_method_str = raw_header
        .clone()
        .unwrap_or_else(|| wire_method.as_str().to_owned());
    tracing::debug!(
        wire = %wire_method,
        semantic = %semantic_method_str,
        header_present = raw_header.is_some(),
        path = %path,
        "secure_envelope: request received"
    );
    let method: axum::http::Method = match semantic_method_str.parse() {
        Ok(m) => m,
        Err(_) => {
            tracing::warn!(
                path = %path,
                header = %semantic_method_str,
                "invalid X-Obsetync-Method header"
            );
            return (StatusCode::BAD_REQUEST, "invalid X-Obsetync-Method").into_response();
        }
    };

    let (mut parts, body) = request.into_parts();
    let body_bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            tracing::warn!(method = %method, path = %path, "request body read failed");
            return (StatusCode::BAD_REQUEST, "request body read failed").into_response();
        }
    };
    let bytes_in = body_bytes.len();

    let server_priv = StaticSecret::from(state.server_priv_bytes);
    let decrypted = match secure::decrypt_request(&body_bytes, &server_priv, method.as_str(), &path)
    {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                method = %method,
                path = %path,
                bytes_in = bytes_in,
                reason = %e,
                "unauthorized: decrypt failed"
            );
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    };

    let device_id = match devices::lookup_token(&state.layout, &decrypted.bearer_token) {
        Some(id) => id,
        None => {
            tracing::warn!(
                method = %method,
                path = %path,
                reason = "unknown_bearer",
                "unauthorized: bearer token not in devices index"
            );
            return (StatusCode::UNAUTHORIZED, "unknown bearer token").into_response();
        }
    };
    if devices::is_revoked(&state.layout, &device_id) {
        tracing::warn!(
            device = %&device_id[..device_id.len().min(12)],
            method = %method,
            path = %path,
            "revoked device attempted request"
        );
        return (StatusCode::FORBIDDEN, "device revoked").into_response();
    }
    let _ = devices::touch_last_seen(&state.layout, &device_id);

    let device_short = device_id[..device_id.len().min(12)].to_owned();
    let inner_body_len = decrypted.inner_body.len();

    // Restore the semantic HTTP method so axum's per-method routing
    // (`get(...)` / `put(...)` / `post(...)`) dispatches to the right
    // handler. Without this the router would only ever see POST.
    parts.method = method.clone();
    let mut inner_request = Request::from_parts(parts, Body::from(decrypted.inner_body));
    inner_request
        .headers_mut()
        .remove(axum::http::header::CONTENT_LENGTH);

    let inner_response = next.run(inner_request).await;
    let inner_status = inner_response.status();

    // Capture inner response, encrypt it.
    let (mut resp_parts, resp_body) = inner_response.into_parts();

    // HTTP/1.1 forbids a body on 1xx, 204, and 304 responses. Hyper enforces
    // this at serialization time and silently drops the encrypted envelope,
    // leaving the client with zero bytes where it expects an AEAD-sealed
    // blob. Promote these statuses to 200 OK so the envelope survives the
    // wire. Semantics are preserved — `put_*` handlers meant "success, no
    // body"; 200 with an empty *decrypted* body conveys the same thing.
    if resp_parts.status == StatusCode::NO_CONTENT || resp_parts.status == StatusCode::NOT_MODIFIED
    {
        resp_parts.status = StatusCode::OK;
    }

    let resp_bytes = match axum::body::to_bytes(resp_body, MAX_BODY_BYTES).await {
        Ok(b) => b,
        Err(_) => {
            tracing::error!(device = %device_short, method = %method, path = %path, "response body read failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "response body read failed",
            )
                .into_response();
        }
    };
    let bytes_out = resp_bytes.len();

    let encrypted = match secure::encrypt_response(
        &resp_bytes,
        &decrypted.shared_secret,
        method.as_str(),
        &path,
        &decrypted.nonce_req,
    ) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(device = %device_short, method = %method, path = %path, reason = %e, "response encryption failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "response encryption failed",
            )
                .into_response();
        }
    };

    let elapsed_ms = started.elapsed().as_millis();
    tracing::debug!(
        device   = %device_short,
        method   = %method,
        path     = %path,
        status   = inner_status.as_u16(),
        in_body  = inner_body_len,
        out_body = bytes_out,
        elapsed_ms = elapsed_ms as u64,
        "sync request"
    );

    let mut out = Response::from_parts(resp_parts, Body::from(encrypted));
    out.headers_mut().remove(axum::http::header::CONTENT_LENGTH);
    out.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/octet-stream"),
    );
    out
}

pub fn sync_router(state: SharedState) -> Router {
    // Why every per-method path also has a `.post(...)` dispatcher:
    //
    // The plugin tunnels every verb through HTTP POST (iOS' requestUrl drops
    // the body on GET, so we can't rely on semantic verbs on the wire). Axum's
    // MethodRouter dispatches *before* our secure-envelope layer runs — when a
    // POST hits a GET-only route, the MethodRouter routes it to its internal
    // 405 fallback, and our middleware's `next.run(...)` just re-returns that
    // 405 regardless of any method rewrite we perform on `parts.method`. Fix:
    // register an explicit POST handler on each per-method path that reads
    // `X-Obsetync-Method` and delegates to the right semantic handler.
    let protected = Router::new()
        .route(
            "/api/v1/root/{vault_id}",
            get(get_root).put(put_root).post(root_dispatcher),
        )
        .route("/api/v1/diff/{vault_id}", post(post_diff))
        .route(
            "/api/v1/chunk/{hash}",
            get(get_chunk).put(put_chunk).post(chunk_dispatcher),
        )
        .route("/api/v1/chunks/check", post(post_chunks_check))
        .route(
            "/api/v1/content/{hash}",
            get(get_content).put(put_content).post(content_dispatcher),
        )
        .route("/api/v1/content/check", post(post_content_check))
        .route(
            "/api/v1/content/manifest/{hash}",
            get(get_manifest)
                .put(put_manifest)
                .post(manifest_dispatcher),
        )
        .route(
            "/api/v1/content/manifests/check",
            post(post_manifests_check),
        )
        .route(
            "/api/v1/content/chunk/{hash}",
            get(get_content_chunk)
                .put(put_content_chunk)
                .post(content_chunk_dispatcher),
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

// --- Semantic-method dispatchers ---
//
// These handlers are registered for `POST /api/v1/<path>` and read the
// semantic method from `X-Obsetync-Method` to delegate to the real handler.
// They keep the plaintext-verb API (direct GET/PUT via curl still works),
// while letting the plugin tunnel everything as wire-POST for iOS.

fn semantic_method(headers: &HeaderMap) -> Option<Method> {
    headers
        .get("X-Obsetync-Method")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<Method>().ok())
}

async fn consume_body(request: Request) -> Result<axum::body::Bytes, Response> {
    axum::body::to_bytes(request.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|_| (StatusCode::BAD_REQUEST, "body read failed").into_response())
}

fn method_not_allowed(path_desc: &str) -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        format!("invalid or missing X-Obsetync-Method for {}", path_desc),
    )
        .into_response()
}

async fn root_dispatcher(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    request: Request,
) -> Response {
    match semantic_method(request.headers()) {
        Some(ref m) if m == Method::GET => match get_root(State(state), Path(vault_id)).await {
            Ok(r) => r.into_response(),
            Err(e) => e.into_response(),
        },
        Some(ref m) if m == Method::PUT => {
            let body = match consume_body(request).await {
                Ok(b) => b,
                Err(r) => return r,
            };
            match put_root(State(state), Path(vault_id), body).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        _ => method_not_allowed("/api/v1/root"),
    }
}

async fn chunk_dispatcher(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    request: Request,
) -> Response {
    match semantic_method(request.headers()) {
        Some(ref m) if m == Method::GET => match get_chunk(State(state), Path(hash)).await {
            Ok(r) => r.into_response(),
            Err(e) => e.into_response(),
        },
        Some(ref m) if m == Method::PUT => {
            let body = match consume_body(request).await {
                Ok(b) => b,
                Err(r) => return r,
            };
            match put_chunk(State(state), Path(hash), body).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        _ => method_not_allowed("/api/v1/chunk"),
    }
}

async fn content_dispatcher(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    request: Request,
) -> Response {
    match semantic_method(request.headers()) {
        Some(ref m) if m == Method::GET => match get_content(State(state), Path(hash)).await {
            Ok(r) => r.into_response(),
            Err(e) => e.into_response(),
        },
        Some(ref m) if m == Method::PUT => {
            let body = match consume_body(request).await {
                Ok(b) => b,
                Err(r) => return r,
            };
            match put_content(State(state), Path(hash), body).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        _ => method_not_allowed("/api/v1/content"),
    }
}

async fn manifest_dispatcher(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    request: Request,
) -> Response {
    match semantic_method(request.headers()) {
        Some(ref m) if m == Method::GET => match get_manifest(State(state), Path(hash)).await {
            Ok(r) => r.into_response(),
            Err(e) => e.into_response(),
        },
        Some(ref m) if m == Method::PUT => {
            let body = match consume_body(request).await {
                Ok(b) => b,
                Err(r) => return r,
            };
            match put_manifest(State(state), Path(hash), body).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        _ => method_not_allowed("/api/v1/content/manifest"),
    }
}

async fn content_chunk_dispatcher(
    State(state): State<SharedState>,
    Path(hash): Path<String>,
    request: Request,
) -> Response {
    match semantic_method(request.headers()) {
        Some(ref m) if m == Method::GET => {
            match get_content_chunk(State(state), Path(hash)).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        Some(ref m) if m == Method::PUT => {
            let body = match consume_body(request).await {
                Ok(b) => b,
                Err(r) => return r,
            };
            match put_content_chunk(State(state), Path(hash), body).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        _ => method_not_allowed("/api/v1/content/chunk"),
    }
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
    // Parent hash is prepended to the body as a 64-byte ASCII prefix (hex
    // or empty, space-padded) so it's covered by the AEAD envelope like
    // the rest of the request — keeping it out of an HTTP header means a
    // MITM can't swap it without failing the GCM tag.
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
            tracing::info!(
                vault = %vault_id,
                root  = %&hash_to_hex(&incoming_hash)[..16],
                bytes = incoming_bytes.len(),
                "put_root: first push accepted"
            );
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
                tracing::info!(
                    vault = %vault_id,
                    root  = %&hash_to_hex(&incoming_hash)[..16],
                    parent = %&hash_to_hex(&parent_hash)[..16],
                    bytes = incoming_bytes.len(),
                    "put_root: fast-forward accepted"
                );
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

                tracing::info!(
                    vault = %vault_id,
                    root  = %&hash_to_hex(&merged_hash)[..16],
                    parent = %&hash_to_hex(&parent_hash)[..16],
                    current = %&hash_to_hex(&current_hash)[..16],
                    auto_resolved = merge_result.auto_resolved_count,
                    conflicts = conflicts.len(),
                    "put_root: merged divergent roots"
                );

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
    // device_root hash rides in the first 64 bytes of the encrypted body
    // (ASCII hex, space-padded) so it's covered by the AEAD envelope
    // rather than being tamperable in an HTTP header.
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
        tracing::debug!(
            vault = %vault_id,
            root  = %&hash_to_hex(&current_hash)[..16],
            "post_diff: device in sync (no delta)"
        );
        return Ok((StatusCode::NOT_MODIFIED, "[]".to_string()));
    }

    let current_data = state
        .vaults
        .get_root(&vault_id, &current_hash)
        .ok_or_else(|| ServerError::Internal("current root data missing".into()))?;

    let to_root = sync_core::chunk::RootNode::deserialize(&current_data)
        .map_err(|e| ServerError::Internal(format!("corrupt current root: {}", e)))?;

    // A device_root of all zeros is the client signalling "fresh sync — I
    // have nothing locally, give me every file as an addition". This is how
    // first-time enrolled clients (iPhone via BRAT, new desktop install)
    // bootstrap without needing to know an existing server root.
    let from_root = if device_root_hash == [0u8; 32] {
        sync_core::chunk::RootNode {
            vault_id: vault_id.clone(),
            created_ms: 0,
            version: 1,
            children: vec![],
            total_files: 0,
            parent_hash: None,
            device_id: "fresh-client".to_string(),
        }
    } else {
        let device_root_data = state
            .vaults
            .get_root(&vault_id, &device_root_hash)
            .ok_or_else(|| {
                ServerError::BadRequest(
                    "device root not found in history — full rescan needed".into(),
                )
            })?;
        sync_core::chunk::RootNode::deserialize(&device_root_data)
            .map_err(|e| ServerError::Internal(format!("corrupt device root: {}", e)))?
    };

    // Compute deltas via bridge.
    let index_base = state.layout.base.join("index");
    let deltas = bridge::run_diff(index_base, from_root, to_root)
        .await
        .map_err(|e| ServerError::Internal(format!("diff failed: {}", e)))?;

    tracing::info!(
        vault = %vault_id,
        from = %&hash_to_hex(&device_root_hash)[..16],
        to   = %&hash_to_hex(&current_hash)[..16],
        deltas = deltas.len(),
        "post_diff: computed delta"
    );

    // sync-core's `FileDelta.hash` is `[u8; 32]`, which serde encodes as a
    // JSON number array (`[172,42,...]`). The plugin expects hex strings
    // in its DTO. Convert at the wire boundary so the plugin can just
    // interpolate `delta.hash` into URLs like `/api/v1/content/{hash}`.
    let wire_deltas: Vec<WireDelta> = deltas.iter().map(WireDelta::from).collect();
    let json = serde_json::to_string(&wire_deltas)?;
    Ok((StatusCode::OK, json))
}

#[derive(serde::Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum WireDelta {
    Added {
        path: String,
        hash: String,
        size: u64,
    },
    Modified {
        path: String,
        hash: String,
        size: u64,
    },
    Deleted {
        path: String,
    },
    Renamed {
        path: String,
        old_path: String,
        hash: String,
    },
}

impl From<&sync_core::diff::FileDelta> for WireDelta {
    fn from(d: &sync_core::diff::FileDelta) -> Self {
        use sync_core::diff::FileDelta as F;
        match d {
            F::Added { path, hash, size } => WireDelta::Added {
                path: path.clone(),
                hash: hash_to_hex(hash),
                size: *size,
            },
            F::Modified { path, hash, size } => WireDelta::Modified {
                path: path.clone(),
                hash: hash_to_hex(hash),
                size: *size,
            },
            F::Deleted { path } => WireDelta::Deleted { path: path.clone() },
            F::Renamed {
                path,
                old_path,
                hash,
            } => WireDelta::Renamed {
                path: path.clone(),
                old_path: old_path.clone(),
                hash: hash_to_hex(hash),
            },
        }
    }
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
    tracing::debug!(
        hash = %&hash_hex[..hash_hex.len().min(16)],
        bytes = body.len(),
        "put_chunk: index chunk stored"
    );
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
    tracing::debug!(
        hash = %&hash_hex[..hash_hex.len().min(16)],
        bytes = body.len(),
        "put_content: small-file blob stored"
    );
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
    tracing::debug!(
        hash = %&hash_hex[..hash_hex.len().min(16)],
        bytes = body.len(),
        "put_manifest: large-file manifest stored"
    );
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
    tracing::debug!(
        hash = %&hash_hex[..hash_hex.len().min(16)],
        bytes = body.len(),
        "put_content_chunk: sub-file chunk stored"
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn post_manifests_check(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let hashes: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("expected JSON array of hashes: {}", e)))?;
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| !blob_exists(&state.layout.content_manifest_path(&hash)))
                .unwrap_or(false)
        })
        .collect();
    Ok(axum::Json(serde_json::json!({ "needed": needed })))
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

// ---------------------------------------------------------------------------
// Integration tests — exercise the FULL stack: sync_router + middleware +
// dispatchers + hyper response serialization. Catches the class of bugs
// where handler return values (like 204 No Content) get mangled before
// reaching the client.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::box_key;
    use crate::config::ServerConfig;
    use crate::devices;
    use crate::secure::{encrypt_request_for_tests, RESPONSE_HEADER_LEN, TAG_LEN, WIRE_VERSION};
    use crate::state::AppState;
    use crate::storage::StorageLayout;
    use axum::http::Request as HttpRequest;
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use sync_core::hash::{hash_bytes, hash_to_hex};
    use tempfile::TempDir;
    use tower::util::ServiceExt;
    use x25519_dalek::{PublicKey, StaticSecret};

    const TEST_BEARER_LEN: usize = 64;

    struct Env {
        _tmp: TempDir,
        state: SharedState,
        server_pub: PublicKey,
        client_priv: StaticSecret,
        bearer: String,
    }

    fn setup() -> Env {
        let tmp = TempDir::new().unwrap();
        let layout = StorageLayout::new(tmp.path());
        layout.init_directories().unwrap();
        let (_server_priv, server_pub) = box_key::init_box_keypair(&layout).unwrap();

        // Enroll a fake device so the middleware's bearer check passes.
        let bearer: String = (0..TEST_BEARER_LEN)
            .map(|i| std::char::from_digit((i % 16) as u32, 16).unwrap())
            .collect();
        let device_id: String = "c0ffee".repeat(5) + "deadbeef";
        devices::register_device(&layout, &device_id, "test-device", &bearer).unwrap();

        let config = ServerConfig::new(tmp.path().to_path_buf());
        let state = Arc::new(AppState::new(config));

        use rand::TryRngCore;
        let mut seed = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
        let client_priv = StaticSecret::from(seed);

        Env {
            _tmp: tmp,
            state,
            server_pub,
            client_priv,
            bearer,
        }
    }

    async fn send(
        env: &Env,
        semantic_method: &str,
        path: &str,
        inner_body: &[u8],
    ) -> (StatusCode, Vec<u8>) {
        let wire_body = encrypt_request_for_tests(
            &env.client_priv,
            &env.server_pub,
            &env.bearer,
            semantic_method,
            path,
            inner_body,
        );

        let req = HttpRequest::builder()
            .method("POST")
            .uri(path)
            .header("X-Obsetync-Method", semantic_method)
            .header("Content-Type", "application/octet-stream")
            .body(Body::from(wire_body))
            .unwrap();

        let router = sync_router(env.state.clone());
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, body)
    }

    /// Regression guard for the 204-strip bug. put_chunk used to return 204
    /// No Content → hyper dropped the AEAD envelope → client's
    /// decryptResponse saw 0 bytes and threw. Middleware now promotes 204→200
    /// before encryption so the envelope reaches the wire.
    #[tokio::test]
    async fn put_chunk_response_carries_aead_envelope() {
        let env = setup();

        let payload = b"hello, chunk world".to_vec();
        let hash = hash_bytes(&payload);
        let path = format!("/api/v1/chunk/{}", hash_to_hex(&hash));

        let (status, body) = send(&env, "PUT", &path, &payload).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "PUT chunk must return 200 (204 would strip body)"
        );
        assert!(
            body.len() >= RESPONSE_HEADER_LEN + TAG_LEN,
            "response body must carry AEAD envelope (got {} bytes, need ≥ {})",
            body.len(),
            RESPONSE_HEADER_LEN + TAG_LEN
        );
        assert_eq!(body[0], WIRE_VERSION, "envelope wire version byte");
    }

    /// Wire-POST + `X-Obsetync-Method: PUT` used to hit MethodRouter's 405
    /// fallback before the middleware could rewrite the method. The explicit
    /// POST dispatcher on each per-method route now handles this.
    #[tokio::test]
    async fn wire_post_dispatches_to_semantic_put_via_header() {
        let env = setup();

        let payload = b"x".repeat(512);
        let hash = hash_bytes(&payload);
        let path = format!("/api/v1/chunk/{}", hash_to_hex(&hash));

        let (status, _) = send(&env, "PUT", &path, &payload).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "wire POST + X-Obsetync-Method: PUT must route to put_chunk, not 405"
        );
    }

    #[tokio::test]
    async fn wire_post_dispatches_to_semantic_get() {
        let env = setup();

        // Pre-populate a chunk so the GET has something to return.
        let payload = b"preloaded".to_vec();
        let hash = hash_bytes(&payload);
        let path = format!("/api/v1/chunk/{}", hash_to_hex(&hash));
        let (put_status, _) = send(&env, "PUT", &path, &payload).await;
        assert_eq!(put_status, StatusCode::OK);

        let (status, body) = send(&env, "GET", &path, &[]).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "wire POST + semantic GET must route to get_chunk"
        );
        assert!(body.len() >= RESPONSE_HEADER_LEN + TAG_LEN);
    }

    /// An envelope encrypted against the wrong server pubkey must 401 (AEAD
    /// decrypt failure), not panic and not leak routing info.
    #[tokio::test]
    async fn wrong_server_key_is_unauthorized() {
        let env = setup();
        use rand::TryRngCore;
        let mut seed = [0u8; 32];
        rand::rngs::OsRng.try_fill_bytes(&mut seed).unwrap();
        let other_priv = StaticSecret::from(seed);
        let other_pub = PublicKey::from(&other_priv);

        let wire_body = encrypt_request_for_tests(
            &env.client_priv,
            &other_pub,
            &env.bearer,
            "PUT",
            "/api/v1/chunk/aa",
            b"hi",
        );
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/api/v1/chunk/aa")
            .header("X-Obsetync-Method", "PUT")
            .body(Body::from(wire_body))
            .unwrap();

        let router = sync_router(env.state.clone());
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "envelope encrypted against wrong server key must 401"
        );
    }

    /// /health stays plaintext — must survive without the envelope machinery.
    #[tokio::test]
    async fn health_is_public_and_plaintext() {
        let env = setup();
        let router = sync_router(env.state.clone());
        let req = HttpRequest::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(text.contains("\"ok\":true"));
    }
}
