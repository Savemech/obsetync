use crate::bridge;
use crate::bulk::{
    self, ObjectKind, UploadRecord, UploadStatus, DOWNLOAD_HEADER_BYTES, PACK_HEADER_BYTES,
    RECORD_HEADER_BYTES,
};
use crate::devices;
use crate::error::ServerError;
use crate::perf::{DiffSample, RequestPhase, ServerPerfCounters};
use crate::secure;
use crate::state::SharedState;
use crate::storage_writer::{
    DurableObject, StorageObjectKind, StoreError, StoreOutcome, StoreResult,
};
use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use sync_core::hash::{hash_bytes, hash_to_hex, hex_to_hash, FileHash};
use x25519_dalek::StaticSecret;

/// Max body size we'll consume in one shot. Generous to accommodate the
/// occasional large-file blob upload (FastCDC caps chunks at 4 MiB each,
/// while large manifests and root batches can also exceed small defaults).
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024; // 64 MiB

/// Authenticated bulk-v1 limits. The legacy sealed middleware accepts larger
/// single-object requests, while the buffering fast path stays deliberately
/// small enough for old mobile renderers and bounded server allocations.
const BULK_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const BULK_OBJECTS: usize = 256;
const BULK_OBJECT_BYTES: usize = 1024 * 1024 - 1;
const WS_FRAME_BYTES: usize = 4 * 1024 * 1024;
const DIFF_PAGE_BYTES: usize = 2 * 1024 * 1024;

fn storage_writer_error(error: StoreError) -> ServerError {
    match error {
        StoreError::InvalidObject(message) => ServerError::BadRequest(message),
        StoreError::Busy | StoreError::Closed | StoreError::Io(_) => {
            ServerError::ServiceUnavailable(error.to_string())
        }
    }
}

async fn store_one_object(
    state: &SharedState,
    kind: StorageObjectKind,
    hash: FileHash,
    bytes: Vec<u8>,
) -> Result<StoreOutcome, ServerError> {
    let mut results = state
        .storage_writer
        .store_batch(vec![DurableObject { kind, hash, bytes }])
        .await
        .map_err(storage_writer_error)?;
    results
        .pop()
        .ok_or_else(|| ServerError::Internal("storage writer returned no object result".into()))?
        .map_err(storage_writer_error)
}

fn read_stored_object(
    state: &SharedState,
    kind: StorageObjectKind,
    hash: &FileHash,
) -> Result<Option<Vec<u8>>, ServerError> {
    state
        .storage_writer
        .read(kind, hash)
        .map_err(|error| ServerError::Internal(error.to_string()))
}

/// Authenticated device id, inserted into request extensions by
/// `secure_envelope` after bearer validation.
#[derive(Clone)]
pub struct DeviceIdExt(pub String);

/// Historical wire-v1 implementation kept temporarily as migration context;
/// cfg(false) guarantees wire v2 is the sole compiled middleware.
#[cfg(any())]
async fn secure_envelope_v1_retired(
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

    // Hand the authenticated device identity to inner handlers that need it
    // (e.g. ws-ticket minting binds the ticket to the requesting device).
    parts.extensions.insert(DeviceIdExt(device_id.clone()));

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

/// Transport-v2 middleware. Every post-decrypt outcome is returned as an
/// encrypted semantic status over wire HTTP 200; failures that prevent opening
/// the request collapse to one constant 256-byte decoy.
async fn secure_envelope(
    State(state): State<SharedState>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_owned();
    let started = std::time::Instant::now();
    state.perf.request_started();
    state
        .perf
        .record_request_phase(RequestPhase::QueueWait, std::time::Duration::ZERO);
    if request.method() != Method::POST {
        state.perf.record_request_error();
        tracing::warn!(path = %path, "transport-v2 request used a non-POST wire method");
        return decrypt_failure_decoy(state.perf.as_ref());
    }
    let Some(method) = request
        .headers()
        .get("X-Obsetync-Method")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<Method>().ok())
    else {
        state.perf.record_request_error();
        tracing::warn!(path = %path, "transport-v2 request omitted/invalidated semantic method");
        return decrypt_failure_decoy(state.perf.as_ref());
    };

    let (mut parts, body) = request.into_parts();
    let body_bytes = match axum::body::to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => {
            state.perf.record_request_error();
            tracing::warn!(method = %method, path = %path, "transport-v2 request body unavailable");
            return decrypt_failure_decoy(state.perf.as_ref());
        }
    };
    state
        .perf
        .record_wire_request_bytes(body_bytes.len() as u64);
    let server_private = StaticSecret::from(state.server_priv_bytes);
    let envelope_started = std::time::Instant::now();
    let decrypted_result = {
        let eph = state.eph.read().expect("eph state poisoned");
        secure::decrypt_request(&body_bytes, &server_private, &eph, method.as_str(), &path)
    };
    let envelope_elapsed = envelope_started.elapsed();
    state
        .perf
        .record_request_phase(RequestPhase::EnvelopeOpen, envelope_elapsed);
    let mut decrypted = match decrypted_result {
        Ok(value) => value,
        Err(error) => {
            state.perf.record_request_error();
            tracing::warn!(method = %method, path = %path, reason = %error, "transport-v2 envelope rejected");
            return decrypt_failure_decoy(state.perf.as_ref());
        }
    };
    state
        .perf
        .record_plaintext_request_bytes(decrypted.inner_body.len() as u64);

    let auth_started = std::time::Instant::now();
    let device_short = if decrypted.mode == secure::TransportMode::Bootstrap {
        // This read-only endpoint intentionally carries no bearer or sequence:
        // putting a credential inside the single-DH bootstrap would expose it
        // to a future compromise of the long-term server key.
        "bootstrap".to_owned()
    } else {
        let device_id = match devices::lookup_token(&state.layout, &decrypted.bearer_token) {
            Some(device_id) => device_id,
            None => {
                state
                    .perf
                    .record_request_phase(RequestPhase::TokenReplay, auth_started.elapsed());
                state.perf.record_request_error();
                tracing::warn!(method = %method, path = %path, "transport-v2 unknown bearer");
                return encrypted_semantic_response(
                    &decrypted,
                    StatusCode::UNAUTHORIZED,
                    br#"{"error":"unknown_bearer"}"#,
                    &method,
                    &path,
                    state.perf.as_ref(),
                );
            }
        };
        let device_short = device_id[..device_id.len().min(12)].to_owned();
        if devices::is_revoked(&state.layout, &device_id) {
            state
                .perf
                .record_request_phase(RequestPhase::TokenReplay, auth_started.elapsed());
            state.perf.record_request_error();
            tracing::warn!(device = %device_short, method = %method, path = %path, "revoked device attempted request");
            return encrypted_semantic_response(
                &decrypted,
                StatusCode::FORBIDDEN,
                br#"{"error":"revoked"}"#,
                &method,
                &path,
                state.perf.as_ref(),
            );
        }

        match state
            .sequences
            .check_and_record(&device_id, decrypted.sequence)
        {
            Ok(crate::seq_tracker::ReplayDecision::Accepted) => {}
            Ok(crate::seq_tracker::ReplayDecision::Replay { greatest_seen }) => {
                let body = serde_json::json!({
                    "error": "replay",
                    "last_seen_seq": greatest_seen,
                })
                .to_string();
                tracing::warn!(device = %device_short, sequence = decrypted.sequence, greatest_seen, "transport-v2 replay rejected");
                state
                    .perf
                    .record_request_phase(RequestPhase::TokenReplay, auth_started.elapsed());
                state.perf.record_request_error();
                return encrypted_semantic_response(
                    &decrypted,
                    StatusCode::UNAUTHORIZED,
                    body.as_bytes(),
                    &method,
                    &path,
                    state.perf.as_ref(),
                );
            }
            Err(error) => {
                state
                    .perf
                    .record_request_phase(RequestPhase::TokenReplay, auth_started.elapsed());
                state.perf.record_request_error();
                tracing::error!(device = %device_short, reason = %error, "anti-replay state persistence failed");
                return encrypted_semantic_response(
                    &decrypted,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    br#"{"error":"anti_replay_unavailable"}"#,
                    &method,
                    &path,
                    state.perf.as_ref(),
                );
            }
        }
        let _ = devices::touch_last_seen(&state.layout, &device_id);
        parts.extensions.insert(DeviceIdExt(device_id));
        device_short
    };
    let auth_elapsed = auth_started.elapsed();
    state
        .perf
        .record_request_phase(RequestPhase::TokenReplay, auth_elapsed);

    parts.method = method.clone();
    let inner_body_len = decrypted.inner_body.len();
    let inner_body = std::mem::take(&mut decrypted.inner_body);
    let mut inner_request = Request::from_parts(parts, Body::from(inner_body));
    inner_request
        .headers_mut()
        .remove(axum::http::header::CONTENT_LENGTH);
    let handler_started = std::time::Instant::now();
    let inner_response = next.run(inner_request).await;
    let semantic_status = inner_response.status();
    let (_, response_body) = inner_response.into_parts();
    let response_bytes = if semantic_status == StatusCode::NO_CONTENT
        || semantic_status == StatusCode::NOT_MODIFIED
    {
        axum::body::Bytes::new()
    } else {
        match axum::body::to_bytes(response_body, MAX_BODY_BYTES).await {
            Ok(bytes) => bytes,
            Err(_) => {
                state
                    .perf
                    .record_request_phase(RequestPhase::Handler, handler_started.elapsed());
                state.perf.record_request_error();
                tracing::error!(device = %device_short, method = %method, path = %path, "handler response body unavailable");
                return encrypted_semantic_response(
                    &decrypted,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    br#"{"error":"internal"}"#,
                    &method,
                    &path,
                    state.perf.as_ref(),
                );
            }
        }
    };
    let handler_elapsed = handler_started.elapsed();
    state
        .perf
        .record_request_phase(RequestPhase::Handler, handler_elapsed);
    if !semantic_status.is_success() {
        state.perf.record_request_error();
    }

    tracing::debug!(
        device = %device_short,
        method = %method,
        path = %path,
        sequence = decrypted.sequence,
        status = semantic_status.as_u16(),
        in_body = inner_body_len,
        out_body = response_bytes.len(),
        envelope_open_us = envelope_elapsed.as_micros() as u64,
        token_replay_us = auth_elapsed.as_micros() as u64,
        handler_us = handler_elapsed.as_micros() as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "transport-v2 sync request"
    );
    encrypted_semantic_response(
        &decrypted,
        semantic_status,
        &response_bytes,
        &method,
        &path,
        state.perf.as_ref(),
    )
}

fn encrypted_semantic_response(
    request: &secure::DecryptedRequest,
    status: StatusCode,
    body: &[u8],
    method: &Method,
    path: &str,
    perf: &ServerPerfCounters,
) -> Response {
    let seal_started = std::time::Instant::now();
    match secure::encrypt_response(
        status.as_u16(),
        body,
        &request.key_material,
        request.mode,
        method.as_str(),
        path,
        &request.nonce_req,
    ) {
        Ok(encrypted) => {
            let seal_elapsed = seal_started.elapsed();
            perf.record_request_phase(RequestPhase::ResponseSeal, seal_elapsed);
            perf.record_response_bytes(encrypted.len() as u64, body.len() as u64);
            tracing::trace!(
                response_seal_us = seal_elapsed.as_micros() as u64,
                "transport-v2 response sealed"
            );
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                encrypted,
            )
                .into_response()
        }
        Err(error) => {
            perf.record_request_phase(RequestPhase::ResponseSeal, seal_started.elapsed());
            perf.record_response_seal_failure();
            tracing::error!(reason = %error, "transport-v2 response encryption failed");
            decrypt_failure_decoy(perf)
        }
    }
}

fn decrypt_failure_decoy(perf: &ServerPerfCounters) -> Response {
    perf.record_response_bytes(256, 0);
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        vec![0u8; 256],
    )
        .into_response()
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
        .route("/api/v1/server-eph", post(get_server_eph))
        .route("/api/v1/capabilities", post(post_capabilities))
        .route("/api/v1/bulk/check", post(post_bulk_check))
        .route("/api/v1/bulk/put", post(post_bulk_put))
        .route("/api/v1/bulk/get", post(post_bulk_get))
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
        .route(
            "/api/v1/history/{vault_id}",
            get(get_history).post(history_dispatcher),
        )
        .route("/api/v1/rollback/{vault_id}", post(post_rollback))
        .route("/api/v1/ws-ticket", post(post_ws_ticket))
        // Ph4 CRDT: fetch a note's durable update log (bootstrap a hot doc);
        // replace it with a compacted snapshot. Live deltas flow over the WS
        // `ops` frame; these two ride the sealed HTTP channel.
        .route("/api/v1/crdt/{vault_id}", post(post_crdt_get))
        .route("/api/v1/crdt/{vault_id}/compact", post(post_crdt_compact))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            secure_envelope,
        ));

    Router::new()
        // Health is public (plaintext) — clients ping it before enrollment to
        // check connectivity without needing the server's box pubkey.
        .route("/health", get(health))
        // The notify WebSocket is a sibling of the sealed routes: it
        // self-authenticates with a single-use ticket (minted over the
        // sealed /api/v1/ws-ticket) because secure_envelope buffers whole
        // responses and cannot wrap a stream. Wire-v2 frames are separately
        // sealed; root notifications carry hashes and presence carries paths.
        .route("/api/v1/ws", get(crate::ws::ws_route))
        .merge(protected)
        .with_state(state)
}

async fn get_server_eph(State(state): State<SharedState>) -> impl IntoResponse {
    let (public, valid_until) = crate::eph_rotation::current_bundle(&state.eph);
    let mut bundle = capability_bundle();
    let object = bundle
        .as_object_mut()
        .expect("capability bundle is always an object");
    object.insert("Es_pub".into(), serde_json::json!(public));
    object.insert(
        "rotation_timestamp".into(),
        serde_json::json!(valid_until.saturating_sub(crate::eph_rotation::ROTATION_PERIOD_SECONDS)),
    );
    object.insert("valid_until".into(), serde_json::json!(valid_until));
    object.insert(
        "rotation_period_seconds".into(),
        serde_json::json!(crate::eph_rotation::ROTATION_PERIOD_SECONDS),
    );
    object.insert(
        "grace_seconds".into(),
        serde_json::json!(crate::eph_rotation::GRACE_SECONDS),
    );
    axum::Json(bundle)
}

fn capability_bundle() -> serde_json::Value {
    serde_json::json!({
        // Never advertise a future fast path before this binary can serve it.
        "capabilities": ["bulk-http-v1"],
        "limits": {
            "bulk_request_bytes": BULK_REQUEST_BYTES,
            "bulk_objects": BULK_OBJECTS,
            "ws_frame_bytes": WS_FRAME_BYTES,
            "diff_page_bytes": DIFF_PAGE_BYTES,
        }
    })
}

async fn post_capabilities() -> impl IntoResponse {
    axum::Json(capability_bundle())
}

// --- Semantic-method dispatchers ---
//
// These handlers are registered for `POST /api/v1/<path>` and read the
// semantic method from `X-Obsetync-Method` to delegate to the real handler.
// They let the authenticated middleware tunnel semantic verbs through the
// wire-POST route selected before middleware dispatch on iOS.

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
            // The device id (injected by secure_envelope) lives in the request
            // extensions — grab it before consuming the body.
            let device = request
                .extensions()
                .get::<DeviceIdExt>()
                .cloned()
                .unwrap_or_else(|| DeviceIdExt(String::new()));
            let body = match consume_body(request).await {
                Ok(b) => b,
                Err(r) => return r,
            };
            match put_root(State(state), Path(vault_id), axum::Extension(device), body).await {
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

/// How many history entries `get_history` returns at most. Every root file
/// must be read to sort by creation time, so the read cost is the full
/// history regardless — the cap only bounds the response size.
const HISTORY_LIMIT: usize = 50;

/// GET /api/v1/history/{vault_id} — the vault's recent root history, newest
/// first, decoded from the stored RootNode metadata. This is what powers the
/// plugin's rollback UI: pick a point in time, roll the vault back to it.
async fn get_history(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    if !state.vaults.vault_exists(&vault_id) {
        return Err(ServerError::NotFound(format!(
            "vault '{}' not found",
            vault_id
        )));
    }
    let current = state.vaults.get_current_root(&vault_id);
    let current_hex = current.map(|h| hash_to_hex(&h));

    let roots_dir = state.layout.vault_roots_dir(&vault_id);
    let mut entries: Vec<serde_json::Value> = Vec::new();
    if let Ok(dir) = std::fs::read_dir(&roots_dir) {
        for e in dir.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().to_string();
            if !name.ends_with(".bin") {
                continue;
            }
            let Ok(bytes) = std::fs::read(e.path()) else {
                continue;
            };
            let Ok(root) = sync_core::chunk::RootNode::deserialize(&bytes) else {
                continue; // skip corrupt entries rather than failing the listing
            };
            let hex = hash_to_hex(&root.hash());
            entries.push(serde_json::json!({
                "root": hex,
                "parent": root.parent_hash.map(|h| hash_to_hex(&h)),
                "created_ms": root.created_ms,
                "device_id": root.device_id,
                "total_files": root.total_files,
                "current": Some(&hex) == current_hex.as_ref(),
            }));
        }
    }
    entries.sort_by(|a, b| {
        b["created_ms"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&a["created_ms"].as_u64().unwrap_or(0))
    });
    entries.truncate(HISTORY_LIMIT);

    Ok((
        StatusCode::OK,
        serde_json::json!({ "roots": entries }).to_string(),
    ))
}

/// POST-tunnel dispatcher for /api/v1/history (see sync_router comment).
async fn history_dispatcher(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    request: Request,
) -> Response {
    match semantic_method(request.headers()) {
        Some(ref m) if m == Method::GET => match get_history(State(state), Path(vault_id)).await {
            Ok(r) => r.into_response(),
            Err(e) => e.into_response(),
        },
        _ => (StatusCode::METHOD_NOT_ALLOWED, "unsupported method").into_response(),
    }
}

/// POST /api/v1/ws-ticket — mint a single-use, short-TTL ticket bound to the
/// authenticated device, spendable once on the /api/v1/ws handshake. Sealed
/// like every sync route; the ticket itself carries no standing authority.
///
/// v2 (sealed frames): the body carries `{"client_eph_pub": "<base64>"}` —
/// the server answers with its own ephemeral pubkey and both sides derive
/// the per-session directional AEAD keys. An empty body mints a legacy v1
/// (plaintext, root-frames-only) ticket for older clients.
async fn post_ws_ticket(
    State(state): State<SharedState>,
    axum::Extension(device): axum::Extension<DeviceIdExt>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let client_eph_pub: Option<[u8; 32]> = if body.is_empty() {
        None
    } else {
        let v: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|_| ServerError::BadRequest("ws-ticket body must be JSON".into()))?;
        match v.get("client_eph_pub").and_then(|p| p.as_str()) {
            Some(b64) => {
                use base64::prelude::*;
                let bytes = BASE64_STANDARD
                    .decode(b64)
                    .map_err(|_| ServerError::BadRequest("client_eph_pub: bad base64".into()))?;
                let arr: [u8; 32] = bytes.try_into().map_err(|_| {
                    ServerError::BadRequest("client_eph_pub must be 32 bytes".into())
                })?;
                Some(arr)
            }
            None => None,
        }
    };

    let mut outcome = crate::ws_ticket::mint(&state.layout, &device.0, client_eph_pub)
        .map_err(|e| ServerError::Internal(format!("ticket mint failed: {}", e)))?;
    let response = serde_json::json!({
        "ticket": outcome.ticket.ticket,
        "expires_at": outcome.ticket.expires_at,
        "server_eph_pub": outcome.server_eph_pub_b64,
    })
    .to_string();
    zeroize::Zeroize::zeroize(&mut outcome.ticket.c2s_key_hex);
    zeroize::Zeroize::zeroize(&mut outcome.ticket.s2c_key_hex);
    Ok((StatusCode::OK, response))
}

/// POST /api/v1/crdt/{vault_id} — body is the note path (UTF-8). Returns the
/// note's raw durable update log (`[u32 len][blob]`… frames) so a device can
/// bootstrap the Yjs doc by applying each frame. Empty body on a fresh note.
async fn post_crdt_get(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let note = std::str::from_utf8(&body)
        .map_err(|_| ServerError::BadRequest("note path not UTF-8".into()))?;
    let log = crate::crdt::read_log(&state.layout, &vault_id, note)
        .map_err(|e| ServerError::Internal(format!("crdt read: {}", e)))?;
    Ok((StatusCode::OK, log))
}

/// POST /api/v1/crdt/{vault_id}/compact — body is
/// `[u16 LE path_len][note path][snapshot update]`. Replaces the note's log
/// with one compacted Yjs update (client sends encodeStateAsUpdateV2 when the
/// log grows or the note goes cold). Atomic replace.
async fn post_crdt_compact(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    if body.len() < 2 {
        return Err(ServerError::BadRequest("compact body too short".into()));
    }
    let path_len = u16::from_le_bytes([body[0], body[1]]) as usize;
    if body.len() < 2 + path_len {
        return Err(ServerError::BadRequest(
            "compact body: bad path length".into(),
        ));
    }
    let note = std::str::from_utf8(&body[2..2 + path_len])
        .map_err(|_| ServerError::BadRequest("note path not UTF-8".into()))?
        .to_owned();
    let snapshot = &body[2 + path_len..];
    crate::crdt::compact(&state.layout, &vault_id, &note, snapshot)
        .map_err(|e| ServerError::Internal(format!("crdt compact: {}", e)))?;
    Ok((
        StatusCode::OK,
        serde_json::json!({ "ok": true }).to_string(),
    ))
}

/// POST /api/v1/rollback/{vault_id} — body is the 64-hex target root hash.
/// Moves `current` back to a root already in history. Deliberately BYPASSES
/// the stale-tree guard: a rollback is an explicit, human-initiated revert —
/// the exact operation the guard exists to stop when it happens silently.
/// Devices converge on their next pull (the diff is ancestry-agnostic).
async fn post_rollback(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let hex = std::str::from_utf8(&body)
        .map_err(|_| ServerError::BadRequest("rollback body must be a hex root hash".into()))?
        .trim();
    let hash = hex_to_hash(hex).map_err(|_| ServerError::BadRequest("invalid root hash".into()))?;

    state
        .vaults
        .get_root(&vault_id, &hash)
        .ok_or_else(|| ServerError::NotFound("root not found in history".into()))?;

    // Same per-vault write lock as put_root — a rollback racing an in-flight
    // push is a lost update.
    let vault_lock = state.vault_lock(&vault_id);
    let _vault_guard = vault_lock.lock().await;

    state.vaults.set_current_root(&vault_id, &hash)?;
    state.notify_root_changed(&vault_id, &hash_to_hex(&hash));

    tracing::warn!(
        vault = %vault_id,
        root = %&hash_to_hex(&hash)[..16],
        "rollback via sync API — vault current moved backwards on request"
    );

    Ok((
        StatusCode::OK,
        serde_json::json!({ "ok": true, "root_hash": hash_to_hex(&hash) }).to_string(),
    ))
}

/// Run the stale-tree guard scan and, depending on OBSETYNC_GUARD, refuse
/// the commit (409), log a warning, or do nothing. Called before every
/// `set_current_root` in `put_root` — both fast-forward and merge commits.
async fn enforce_guard(
    state: &SharedState,
    vault_id: &str,
    device_id: &str,
    current: sync_core::chunk::RootNode,
    candidate: sync_core::chunk::RootNode,
    branch: &'static str,
) -> Result<(), ServerError> {
    let cfg = crate::guard::config();
    if cfg.mode == crate::guard::GuardMode::Off {
        return Ok(());
    }
    let scan = crate::guard::scan(state.storage_writer.clone(), current, candidate)
        .await
        .map_err(|e| ServerError::Internal(format!("guard scan failed: {}", e)))?;
    if let Some(reason) = scan.triggered(cfg) {
        tracing::warn!(
            vault = %vault_id,
            branch,
            reason,
            deletions = scan.deletions,
            content_changes = scan.content_changes,
            mtime_regressions = scan.mtime_regressions,
            current_total = scan.current_total,
            enforced = (cfg.mode == crate::guard::GuardMode::Enforce),
            "put_root: guard tripwire"
        );
        if cfg.mode == crate::guard::GuardMode::Enforce {
            // One-time, admin-approved bypass for an INTENTIONAL bulk change the
            // guard can't distinguish from a runaway (e.g. deleting a build
            // tree). Consumed here so it applies to exactly this one push.
            if crate::devices::consume_deletion_bypass(&state.layout, device_id) {
                tracing::warn!(
                    vault = %vault_id,
                    branch,
                    device = %&device_id[..device_id.len().min(12)],
                    reason,
                    deletions = scan.deletions,
                    "put_root: guard BYPASSED (admin-approved one-time deletion)"
                );
                return Ok(());
            }
            return Err(ServerError::Conflict(scan.reject_body(reason)));
        }
    }
    Ok(())
}

async fn put_root(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    axum::Extension(device): axum::Extension<DeviceIdExt>,
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

    let mut incoming_root = sync_core::chunk::RootNode::deserialize(root_bytes)
        .map_err(|e| ServerError::BadRequest(format!("invalid root: {}", e)))?;

    if incoming_root.vault_id != vault_id {
        return Err(ServerError::BadRequest(
            "root vault_id does not match request path".into(),
        ));
    }

    // Root bytes and every referenced index/content object are untrusted
    // until this walk succeeds. Besides blocking traversal-shaped paths, the
    // count check prevents a false total_files field from weakening the
    // blast-radius guard. The walk reads compact index nodes, not file data.
    let entries = bridge::run_validate_root(state.storage_writer.clone(), incoming_root.clone())
        .await
        .map_err(|e| ServerError::BadRequest(format!("invalid root structure: {}", e)))?;
    for entry in &entries {
        let present = if sync_core::fastcdc_chunker::should_chunk(entry.size_bytes) {
            stored_manifest_is_usable(&state, &entry.hash)
        } else {
            state
                .storage_writer
                .contains(StorageObjectKind::Content, &entry.hash)
        };
        if !present {
            return Err(ServerError::BadRequest(format!(
                "root references missing content for {:?}",
                entry.path
            )));
        }
    }
    drop(entries);

    let incoming_hash = incoming_root.hash();

    // Serialize the read-modify-write on `current` per vault. Without this,
    // two concurrent pushes both observe the same current, both pass the
    // fast-forward check (or merge against a stale current), and the second
    // set_current_root silently drops the first push. Held across the guard
    // scan and merge awaits by design — pushes to ONE vault are serialized
    // (ms for fast-forwards, worst-case seconds for a huge merge), different
    // vaults never contend.
    let vault_lock = state.vault_lock(&vault_id);
    let _vault_guard = vault_lock.lock().await;

    let current_root_hash = state.vaults.get_current_root(&vault_id);
    // History metadata is server-authored. It is not part of the semantic
    // root hash, so trusting client values would let a replay rewrite the
    // displayed author, timestamp, or parent of an existing state.
    incoming_root.created_ms = unix_time_ms();
    incoming_root.device_id = device.0.clone();

    match current_root_hash {
        None => {
            // First push — accept directly.
            incoming_root.parent_hash = None;
            let incoming_bytes = incoming_root.serialize();
            state
                .vaults
                .store_root(&vault_id, &incoming_hash, &incoming_bytes)?;
            state.vaults.set_current_root(&vault_id, &incoming_hash)?;
            state.notify_root_changed(&vault_id, &hash_to_hex(&incoming_hash));
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
            incoming_root.parent_hash = Some(parent_hash);
            let incoming_bytes = incoming_root.serialize();

            if current_hash == parent_hash {
                // Fast-forward — parent matches current. The parent claim is
                // self-asserted (a client with a stale tree but a fresh root
                // poller satisfies this check while reverting the vault —
                // incident 2026-07-13), so gate the commit on a content scan.
                let current_data = state
                    .vaults
                    .get_root(&vault_id, &current_hash)
                    .ok_or_else(|| ServerError::Internal("current root data missing".into()))?;
                let current_root = sync_core::chunk::RootNode::deserialize(&current_data)
                    .map_err(|e| ServerError::Internal(format!("corrupt current root: {}", e)))?;
                enforce_guard(
                    &state,
                    &vault_id,
                    &device.0,
                    current_root,
                    incoming_root.clone(),
                    "ff",
                )
                .await?;

                state
                    .vaults
                    .store_root(&vault_id, &incoming_hash, &incoming_bytes)?;
                state.vaults.set_current_root(&vault_id, &incoming_hash)?;
                state.notify_root_changed(&vault_id, &hash_to_hex(&incoming_hash));
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

                // Run merge via the bridge (handles !Send). The packed store
                // supplies both tree nodes and small-file merge content.
                let incoming_for_merge = incoming_root;
                let merge_result = bridge::run_merge(
                    state.storage_writer.clone(),
                    base_root,
                    current_root,
                    incoming_for_merge,
                )
                .await
                .map_err(|e| ServerError::Internal(format!("merge failed: {}", e)))?;

                let merged_hash = merge_result.new_root.hash();
                let merged_bytes = merge_result.new_root.serialize();

                // A merge with a poisoned base (claimed parent newer than the
                // pusher's real tree epoch) silently reverts every file the
                // stale side "didn't change since base" — scan the outcome
                // against current before committing it.
                let current_root_again = sync_core::chunk::RootNode::deserialize(&current_data)
                    .map_err(|e| ServerError::Internal(format!("corrupt current root: {}", e)))?;
                enforce_guard(
                    &state,
                    &vault_id,
                    &device.0,
                    current_root_again,
                    merge_result.new_root.clone(),
                    "merge",
                )
                .await?;

                // Only accepted states enter history. Root nodes are kept in
                // per-vault history, not the global byte-addressed index:
                // their hash excludes history metadata by design.
                state
                    .vaults
                    .store_root(&vault_id, &incoming_hash, &incoming_bytes)?;
                state
                    .vaults
                    .store_root(&vault_id, &merged_hash, &merged_bytes)?;

                // Update current.
                state.vaults.set_current_root(&vault_id, &merged_hash)?;
                state.notify_root_changed(&vault_id, &hash_to_hex(&merged_hash));

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
                    text_merged = merge_result.text_merged_count,
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
                        "text_merged": merge_result.text_merged_count,
                    })
                    .to_string(),
                ))
            }
        }
    }
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// --- Diff ---

async fn post_diff(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let diff_started = std::time::Instant::now();
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
        state.perf.record_diff(DiffSample {
            nodes_visited: 1,
            nodes_skipped: 1,
            entries_materialized: 0,
            serialized_bytes: 2,
            pages: 1,
            elapsed: diff_started.elapsed(),
        });
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
    let diff_result = bridge::run_diff_with_stats(state.storage_writer.clone(), from_root, to_root)
        .await
        .map_err(|e| ServerError::Internal(format!("diff failed: {}", e)))?;
    let compute_elapsed = diff_started.elapsed();
    let deltas = diff_result.deltas;

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
    state.perf.record_request_objects(deltas.len() as u64);
    state.perf.record_diff(DiffSample {
        nodes_visited: diff_result.stats.nodes_visited,
        nodes_skipped: diff_result.stats.nodes_skipped,
        entries_materialized: diff_result.stats.entries_materialized,
        serialized_bytes: json.len() as u64,
        pages: 1,
        elapsed: compute_elapsed,
    });
    Ok((StatusCode::OK, json))
}

#[derive(serde::Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum WireDelta {
    Added {
        path: String,
        hash: String,
        size: u64,
        /// Server-side mtime of the entry, so pulling clients can rebase
        /// their local Merkle tree to byte-identical leaf metadata (leaf
        /// hashes cover mtime). Older clients ignore the extra field.
        mtime_ms: u64,
    },
    Modified {
        path: String,
        hash: String,
        size: u64,
        mtime_ms: u64,
    },
    Deleted {
        path: String,
    },
    Renamed {
        path: String,
        old_path: String,
        hash: String,
        size: u64,
        mtime_ms: u64,
    },
}

impl From<&sync_core::diff::FileDelta> for WireDelta {
    fn from(d: &sync_core::diff::FileDelta) -> Self {
        use sync_core::diff::FileDelta as F;
        match d {
            F::Added {
                path,
                hash,
                size,
                mtime_ms,
            } => WireDelta::Added {
                path: path.clone(),
                hash: hash_to_hex(hash),
                size: *size,
                mtime_ms: *mtime_ms,
            },
            F::Modified {
                path,
                hash,
                size,
                mtime_ms,
            } => WireDelta::Modified {
                path: path.clone(),
                hash: hash_to_hex(hash),
                size: *size,
                mtime_ms: *mtime_ms,
            },
            F::Deleted { path, .. } => WireDelta::Deleted { path: path.clone() },
            F::Renamed {
                path,
                old_path,
                hash,
                size,
                mtime_ms,
            } => WireDelta::Renamed {
                path: path.clone(),
                old_path: old_path.clone(),
                hash: hash_to_hex(hash),
                size: *size,
                mtime_ms: *mtime_ms,
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
    let data = read_stored_object(&state, StorageObjectKind::IndexChunk, &hash)?
        .ok_or_else(|| ServerError::NotFound(format!("chunk {} not found", hash_hex)))?;
    state.perf.record_request_objects(1);
    Ok((StatusCode::OK, data))
}

async fn put_chunk(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let expected =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let hash_started = std::time::Instant::now();
    let actual = hash_bytes(&body);
    state
        .perf
        .record_hash_verify(body.len() as u64, hash_started.elapsed());
    if expected != actual {
        return Err(ServerError::BadRequest(format!(
            "hash mismatch: expected {}, got {}",
            hash_hex,
            hash_to_hex(&actual)
        )));
    }
    store_one_object(
        &state,
        StorageObjectKind::IndexChunk,
        expected,
        body.to_vec(),
    )
    .await?;
    state.perf.record_request_objects(1);
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
    state.perf.record_request_objects(hashes.len() as u64);
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| {
                    !state
                        .storage_writer
                        .contains(StorageObjectKind::IndexChunk, &hash)
                })
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
    let data = read_stored_object(&state, StorageObjectKind::Content, &hash)?
        .ok_or_else(|| ServerError::NotFound(format!("content {} not found", hash_hex)))?;
    state.perf.record_request_objects(1);
    Ok((StatusCode::OK, data))
}

async fn put_content(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let expected =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let hash_started = std::time::Instant::now();
    let actual = hash_bytes(&body);
    state
        .perf
        .record_hash_verify(body.len() as u64, hash_started.elapsed());
    if expected != actual {
        return Err(ServerError::BadRequest("hash mismatch".into()));
    }
    store_one_object(&state, StorageObjectKind::Content, expected, body.to_vec()).await?;
    state.perf.record_request_objects(1);
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
    state.perf.record_request_objects(hashes.len() as u64);
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| {
                    !state
                        .storage_writer
                        .contains(StorageObjectKind::Content, &hash)
                })
                .unwrap_or(false)
        })
        .collect();
    Ok(axum::Json(serde_json::json!({ "needed": needed })))
}

// --- Content Manifests ---

const MAX_CONTENT_CHUNK_BYTES: u32 = 4 * 1024 * 1024;
const MAX_MANIFEST_CHUNKS: usize = 1_000_000;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct WireContentManifest {
    file_hash: String,
    total_size: u64,
    chunks: Vec<WireContentChunkRef>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct WireContentChunkRef {
    hash: String,
    offset: u64,
    size: u32,
}

/// Validate the manifest itself without reading chunk contents. This cheap
/// pass is used by GET/check so malformed JSON or a missing referenced object
/// becomes repairable without re-hashing every large file on every sync.
fn validate_content_manifest_structure(
    expected_file_hash: &FileHash,
    manifest: &WireContentManifest,
) -> Result<Vec<FileHash>, String> {
    let declared_hash =
        hex_to_hash(&manifest.file_hash).map_err(|_| "invalid manifest file_hash".to_string())?;
    if &declared_hash != expected_file_hash {
        return Err("manifest file_hash does not match request path".into());
    }
    if manifest.chunks.len() > MAX_MANIFEST_CHUNKS {
        return Err("manifest has too many chunks".into());
    }
    if manifest.total_size > 0 && manifest.chunks.is_empty() {
        return Err("non-empty manifest has no chunks".into());
    }

    let mut expected_offset = 0u64;
    let mut chunk_hashes = Vec::with_capacity(manifest.chunks.len());
    for (index, chunk) in manifest.chunks.iter().enumerate() {
        if chunk.offset != expected_offset
            || chunk.size == 0
            || chunk.size > MAX_CONTENT_CHUNK_BYTES
        {
            return Err(format!("invalid manifest chunk layout at index {index}"));
        }
        expected_offset = expected_offset
            .checked_add(u64::from(chunk.size))
            .ok_or_else(|| "manifest size overflow".to_string())?;
        if expected_offset > manifest.total_size {
            return Err("manifest chunks exceed total_size".into());
        }
        chunk_hashes.push(
            hex_to_hash(&chunk.hash).map_err(|_| format!("invalid chunk hash at index {index}"))?,
        );
    }
    if expected_offset != manifest.total_size {
        return Err("manifest chunks do not cover total_size".into());
    }
    if manifest.total_size == 0 && *expected_file_hash != hash_bytes(&[]) {
        return Err("empty manifest does not match file_hash".into());
    }
    Ok(chunk_hashes)
}

/// Validate both manifest structure and the content it names. Chunk PUTs
/// already verify their individual addresses; rechecking here also detects
/// disk corruption and proves the ordered concatenation hashes to the
/// manifest/file-tree hash without ever holding the whole file in memory.
fn validate_content_manifest(
    state: &SharedState,
    expected_file_hash: &FileHash,
    manifest: &WireContentManifest,
) -> Result<(), ServerError> {
    validate_content_manifest_with_loader(
        expected_file_hash,
        manifest,
        state.perf.as_ref(),
        |chunk_hash| {
            state
                .storage_writer
                .read(StorageObjectKind::ContentChunk, chunk_hash)
                .ok()
                .flatten()
                .map(std::borrow::Cow::Owned)
        },
    )
}

/// Full manifest validation with a caller-supplied chunk source. Bulk upload
/// uses already hash-verified earlier records before their group is published;
/// legacy upload and reads resolve every chunk from the loose mirror.
fn validate_content_manifest_with_loader<'a>(
    expected_file_hash: &FileHash,
    manifest: &WireContentManifest,
    perf: &crate::perf::ServerPerfCounters,
    mut load_chunk: impl FnMut(&FileHash) -> Option<std::borrow::Cow<'a, [u8]>>,
) -> Result<(), ServerError> {
    let chunk_hashes = validate_content_manifest_structure(expected_file_hash, manifest)
        .map_err(ServerError::BadRequest)?;
    let mut full_hasher = blake3::Hasher::new();
    for (index, (chunk, chunk_hash)) in manifest.chunks.iter().zip(chunk_hashes.iter()).enumerate()
    {
        let bytes = load_chunk(chunk_hash).ok_or_else(|| {
            ServerError::BadRequest(format!("manifest chunk {} is missing", index))
        })?;
        let hash_started = std::time::Instant::now();
        let chunk_matches = hash_bytes(bytes.as_ref()) == *chunk_hash;
        perf.record_hash_check(bytes.len() as u64, hash_started.elapsed(), chunk_matches);
        if bytes.len() != chunk.size as usize || !chunk_matches {
            return Err(ServerError::BadRequest(format!(
                "manifest chunk {} failed size/hash validation",
                index
            )));
        }
        let full_hash_started = std::time::Instant::now();
        full_hasher.update(bytes.as_ref());
        perf.record_hash_verify(bytes.len() as u64, full_hash_started.elapsed());
    }

    if full_hasher.finalize().as_bytes() != expected_file_hash {
        return Err(ServerError::BadRequest(
            "manifest chunks do not hash to file_hash".into(),
        ));
    }
    Ok(())
}

async fn get_manifest(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
) -> Result<impl IntoResponse, ServerError> {
    let hash =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let data = read_stored_object(&state, StorageObjectKind::Manifest, &hash)?
        .ok_or_else(|| ServerError::NotFound(format!("manifest {} not found", hash_hex)))?;
    let manifest: WireContentManifest = serde_json::from_slice(&data)
        .map_err(|e| ServerError::Internal(format!("corrupt manifest {}: {}", hash_hex, e)))?;
    let chunk_hashes = validate_content_manifest_structure(&hash, &manifest)
        .map_err(|e| ServerError::Internal(format!("corrupt manifest {}: {}", hash_hex, e)))?;
    if chunk_hashes.iter().any(|chunk| {
        !state
            .storage_writer
            .contains(StorageObjectKind::ContentChunk, chunk)
    }) {
        return Err(ServerError::Internal(format!(
            "manifest {} references a missing chunk",
            hash_hex,
        )));
    }
    state.perf.record_request_objects(1);
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
    let manifest: WireContentManifest = serde_json::from_slice(&body)?;
    validate_content_manifest(&state, &hash, &manifest)?;
    // Store a canonical representation only after all referenced bytes and
    // the full-file content address have been verified.
    let encoded = serde_json::to_vec(&manifest)?;
    let encoded_len = encoded.len();
    store_one_object(&state, StorageObjectKind::Manifest, hash, encoded).await?;
    state.perf.record_request_objects(1);
    tracing::debug!(
        hash = %&hash_hex[..hash_hex.len().min(16)],
        bytes = encoded_len,
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
    let data = read_stored_object(&state, StorageObjectKind::ContentChunk, &hash)?
        .ok_or_else(|| ServerError::NotFound(format!("content chunk {} not found", hash_hex)))?;
    state.perf.record_request_objects(1);
    Ok((StatusCode::OK, data))
}

async fn put_content_chunk(
    State(state): State<SharedState>,
    Path(hash_hex): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let expected =
        hex_to_hash(&hash_hex).map_err(|_| ServerError::BadRequest("invalid hash".into()))?;
    let hash_started = std::time::Instant::now();
    let actual = hash_bytes(&body);
    state
        .perf
        .record_hash_verify(body.len() as u64, hash_started.elapsed());
    if expected != actual {
        return Err(ServerError::BadRequest("hash mismatch".into()));
    }
    store_one_object(
        &state,
        StorageObjectKind::ContentChunk,
        expected,
        body.to_vec(),
    )
    .await?;
    state.perf.record_request_objects(1);
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
    state.perf.record_request_objects(hashes.len() as u64);
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| !stored_manifest_is_usable(&state, &hash))
                .unwrap_or(false)
        })
        .collect();
    Ok(axum::Json(serde_json::json!({ "needed": needed })))
}

fn stored_manifest_is_usable(state: &SharedState, file_hash: &FileHash) -> bool {
    let Ok(Some(data)) = state
        .storage_writer
        .read(StorageObjectKind::Manifest, file_hash)
    else {
        return false;
    };
    let Ok(manifest) = serde_json::from_slice::<WireContentManifest>(&data) else {
        return false;
    };
    validate_content_manifest_structure(file_hash, &manifest).is_ok_and(|chunk_hashes| {
        chunk_hashes.iter().all(|chunk| {
            state
                .storage_writer
                .contains(StorageObjectKind::ContentChunk, chunk)
        })
    })
}

async fn post_content_chunks_check(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let hashes: Vec<String> = serde_json::from_slice(&body)
        .map_err(|e| ServerError::BadRequest(format!("expected JSON array of hashes: {}", e)))?;
    state.perf.record_request_objects(hashes.len() as u64);
    let needed: Vec<String> = hashes
        .into_iter()
        .filter(|h| {
            hex_to_hash(h)
                .map(|hash| {
                    !state
                        .storage_writer
                        .contains(StorageObjectKind::ContentChunk, &hash)
                })
                .unwrap_or(false)
        })
        .collect();
    Ok(axum::Json(serde_json::json!({ "needed": needed })))
}

// --- Bounded binary bulk HTTP v1 -----------------------------------------

fn storage_object_kind(kind: ObjectKind) -> StorageObjectKind {
    match kind {
        ObjectKind::Content => StorageObjectKind::Content,
        ObjectKind::ContentChunk => StorageObjectKind::ContentChunk,
        ObjectKind::IndexChunk => StorageObjectKind::IndexChunk,
        ObjectKind::Manifest => StorageObjectKind::Manifest,
    }
}

fn upload_status(result: &StoreResult) -> UploadStatus {
    match result {
        Ok(StoreOutcome::Stored) => UploadStatus::Stored,
        Ok(StoreOutcome::AlreadyPresent) => UploadStatus::AlreadyPresent,
        Err(StoreError::InvalidObject(_)) => UploadStatus::BadHash,
        Err(StoreError::Busy | StoreError::Closed | StoreError::Io(_)) => {
            UploadStatus::RetryableStorageError
        }
    }
}

fn bulk_object_is_usable(state: &SharedState, kind: ObjectKind, hash: &FileHash) -> bool {
    match kind {
        ObjectKind::Manifest => stored_manifest_is_usable(state, hash),
        _ => state
            .storage_writer
            .contains(storage_object_kind(kind), hash),
    }
}

async fn post_bulk_check(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    if body.len() > BULK_REQUEST_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "bulk check exceeds advertised byte limit".into(),
        ));
    }
    let request = bulk::decode_check_request(&body, BULK_OBJECTS)
        .map_err(|error| ServerError::BadRequest(error.to_string()))?;
    state
        .perf
        .record_request_objects(request.hashes.len() as u64);
    let needed: Vec<bool> = request
        .hashes
        .iter()
        .map(|hash| !bulk_object_is_usable(&state, request.kind, hash))
        .collect();
    let response = bulk::encode_check_response(&needed)
        .map_err(|error| ServerError::Internal(error.to_string()))?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        response,
    ))
}

async fn post_bulk_put(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    let pack =
        bulk::decode_upload_pack(&body, BULK_OBJECTS, BULK_REQUEST_BYTES).map_err(|error| {
            if body.len() > BULK_REQUEST_BYTES {
                ServerError::PayloadTooLarge(error.to_string())
            } else {
                ServerError::BadRequest(error.to_string())
            }
        })?;
    state.perf.record_request_objects(pack.records.len() as u64);

    let mut statuses = vec![None; pack.records.len()];
    let mut writer_objects = Vec::with_capacity(pack.records.len());
    let mut record_for_writer_object = Vec::with_capacity(pack.records.len());
    // Earlier valid chunk records can satisfy a later manifest before the
    // group is materialized. Values also carry the writer-result position so
    // a publication error propagates a retryable ACK to the dependent
    // manifest even though both records are already durable in the journal.
    let mut pending_chunks: std::collections::HashMap<FileHash, (&[u8], usize)> =
        std::collections::HashMap::new();
    let mut manifest_dependencies: Vec<(usize, usize, Vec<usize>)> = Vec::new();

    for (record_index, record) in pack.records.into_iter().enumerate() {
        let plain_len = record.plain_len as usize;
        if record.flags != 0 || plain_len != record.bytes.len() || plain_len > BULK_OBJECT_BYTES {
            statuses[record_index] = Some(UploadStatus::RejectedLimit);
            continue;
        }

        let (stored_bytes, referenced_chunks) = if record.kind == ObjectKind::Manifest {
            let manifest = match serde_json::from_slice::<WireContentManifest>(record.bytes) {
                Ok(manifest) => manifest,
                Err(_) => {
                    statuses[record_index] = Some(UploadStatus::BadHash);
                    continue;
                }
            };
            let referenced = match validate_content_manifest_structure(&record.hash, &manifest) {
                Ok(chunks) => chunks,
                Err(_) => {
                    statuses[record_index] = Some(UploadStatus::BadHash);
                    continue;
                }
            };
            if validate_content_manifest_with_loader(
                &record.hash,
                &manifest,
                state.perf.as_ref(),
                |chunk_hash| {
                    if let Some((bytes, _)) = pending_chunks.get(chunk_hash) {
                        Some(std::borrow::Cow::Borrowed(*bytes))
                    } else {
                        state
                            .storage_writer
                            .read(StorageObjectKind::ContentChunk, chunk_hash)
                            .ok()
                            .flatten()
                            .map(std::borrow::Cow::Owned)
                    }
                },
            )
            .is_err()
            {
                statuses[record_index] = Some(UploadStatus::BadHash);
                continue;
            }
            let encoded = match serde_json::to_vec(&manifest) {
                Ok(encoded) => encoded,
                Err(_) => {
                    statuses[record_index] = Some(UploadStatus::BadHash);
                    continue;
                }
            };
            (encoded, referenced)
        } else {
            let hash_started = std::time::Instant::now();
            let actual = hash_bytes(record.bytes);
            state
                .perf
                .record_hash_verify(record.bytes.len() as u64, hash_started.elapsed());
            if actual != record.hash {
                statuses[record_index] = Some(UploadStatus::BadHash);
                continue;
            }
            (record.bytes.to_vec(), Vec::new())
        };

        if bulk_object_is_usable(&state, record.kind, &record.hash) {
            statuses[record_index] = Some(UploadStatus::AlreadyPresent);
            continue;
        }

        let writer_index = writer_objects.len();
        if record.kind == ObjectKind::ContentChunk {
            pending_chunks.insert(record.hash, (record.bytes, writer_index));
        }
        if record.kind == ObjectKind::Manifest {
            let dependencies = referenced_chunks
                .iter()
                .filter_map(|hash| pending_chunks.get(hash).map(|(_, index)| *index))
                .collect();
            manifest_dependencies.push((writer_index, record_index, dependencies));
        }
        record_for_writer_object.push(record_index);
        writer_objects.push(DurableObject {
            kind: storage_object_kind(record.kind),
            hash: record.hash,
            bytes: stored_bytes,
        });
    }

    let writer_results: Vec<StoreResult> = if writer_objects.is_empty() {
        Vec::new()
    } else {
        match state.storage_writer.store_batch(writer_objects).await {
            Ok(results) => results,
            Err(error) => vec![Err(error); record_for_writer_object.len()],
        }
    };
    if writer_results.len() != record_for_writer_object.len() {
        return Err(ServerError::Internal(
            "storage writer returned the wrong result count".into(),
        ));
    }
    for (writer_index, result) in writer_results.iter().enumerate() {
        statuses[record_for_writer_object[writer_index]] = Some(upload_status(result));
    }
    for (manifest_writer_index, manifest_record_index, dependencies) in manifest_dependencies {
        if writer_results[manifest_writer_index].is_ok()
            && dependencies
                .iter()
                .any(|dependency| writer_results[*dependency].is_err())
        {
            statuses[manifest_record_index] = Some(UploadStatus::RetryableStorageError);
        }
    }
    let statuses: Vec<UploadStatus> = statuses
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| ServerError::Internal("bulk writer left an ACK unset".into()))?;

    let response = bulk::encode_upload_ack(&statuses)
        .map_err(|error| ServerError::Internal(error.to_string()))?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        response,
    ))
}

/// Load and validate one stored object for a bulk download. Missing objects
/// are represented by `Ok(None)`; corrupt objects fail closed exactly like
/// their legacy single-object GET endpoint.
fn read_bulk_object(
    state: &SharedState,
    kind: ObjectKind,
    hash: &FileHash,
) -> Result<Option<Vec<u8>>, ServerError> {
    let Some(bytes) = read_stored_object(state, storage_object_kind(kind), hash)? else {
        return Ok(None);
    };
    if kind == ObjectKind::Manifest {
        let manifest: WireContentManifest = serde_json::from_slice(&bytes)
            .map_err(|error| ServerError::Internal(format!("corrupt manifest: {error}")))?;
        let chunks = validate_content_manifest_structure(hash, &manifest)
            .map_err(|error| ServerError::Internal(format!("corrupt manifest: {error}")))?;
        if chunks.iter().any(|chunk| {
            !state
                .storage_writer
                .contains(StorageObjectKind::ContentChunk, chunk)
        }) {
            return Err(ServerError::Internal(
                "manifest references a missing content chunk".into(),
            ));
        }
        return Ok(Some(bytes));
    }

    Ok(Some(bytes))
}

async fn post_bulk_get(
    State(state): State<SharedState>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ServerError> {
    if body.len() > BULK_REQUEST_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "bulk get request exceeds advertised byte limit".into(),
        ));
    }
    let request = bulk::decode_get_request(&body, BULK_OBJECTS)
        .map_err(|error| ServerError::BadRequest(error.to_string()))?;
    let response_budget = request.max_response_bytes as usize;
    if response_budget == 0 || response_budget > BULK_REQUEST_BYTES {
        return Err(ServerError::PayloadTooLarge(
            "bulk get response budget exceeds advertised limit".into(),
        ));
    }
    let bitmap_len = bulk::bitmap_bytes(request.hashes.len())
        .map_err(|error| ServerError::BadRequest(error.to_string()))?;
    let fixed_bytes = DOWNLOAD_HEADER_BYTES
        .checked_add(bitmap_len)
        .and_then(|value| value.checked_add(PACK_HEADER_BYTES))
        .ok_or_else(|| ServerError::BadRequest("bulk get response length overflow".into()))?;
    if fixed_bytes > response_budget {
        return Err(ServerError::PayloadTooLarge(
            "bulk get response budget is below protocol overhead".into(),
        ));
    }

    let start = request.cursor as usize;
    let mut next_cursor = start;
    let mut encoded_bytes = fixed_bytes;
    let mut owned_records: Vec<(FileHash, Vec<u8>)> = Vec::new();
    for hash in request.hashes.iter().skip(start) {
        let Some(bytes) = read_bulk_object(&state, request.kind, hash)? else {
            // Missing is deliberately distinct from "not inspected yet": at
            // completion the client detects any requested hash absent from
            // the returned record set and raises the same failure as GET 404.
            next_cursor += 1;
            continue;
        };
        if bytes.len() > BULK_OBJECT_BYTES {
            return Err(ServerError::PayloadTooLarge(
                "object exceeds bulk-v1 per-object limit; use single GET".into(),
            ));
        }
        let record_bytes = RECORD_HEADER_BYTES
            .checked_add(bytes.len())
            .ok_or_else(|| ServerError::BadRequest("bulk record length overflow".into()))?;
        if encoded_bytes
            .checked_add(record_bytes)
            .is_none_or(|total| total > response_budget)
        {
            if next_cursor == start {
                return Err(ServerError::PayloadTooLarge(
                    "bulk get response budget cannot fit the next object".into(),
                ));
            }
            break;
        }
        encoded_bytes += record_bytes;
        owned_records.push((*hash, bytes));
        next_cursor += 1;
    }

    let mut remaining = vec![0u8; bitmap_len];
    for index in next_cursor..request.hashes.len() {
        bulk::bitmap_set(&mut remaining, index);
    }
    let records: Vec<UploadRecord<'_>> = owned_records
        .iter()
        .map(|(hash, bytes)| UploadRecord {
            kind: request.kind,
            flags: 0,
            hash: *hash,
            plain_len: bytes.len() as u32,
            bytes,
        })
        .collect();
    let response =
        bulk::encode_download_response(request.hashes.len(), next_cursor, &remaining, &records)
            .map_err(|error| ServerError::Internal(error.to_string()))?;
    debug_assert!(response.len() <= response_budget);
    state
        .perf
        .record_request_objects((next_cursor - start) as u64);
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        response,
    ))
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
    use crate::secure::{
        decrypt_response_for_tests, encrypt_request_for_tests, DecryptedRequest,
        RESPONSE_HEADER_LEN, TAG_LEN, WIRE_VERSION,
    };
    use crate::state::AppState;
    use crate::storage::StorageLayout;
    use axum::http::Request as HttpRequest;
    use http_body_util::BodyExt;
    use std::sync::atomic::{AtomicU64, Ordering};
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
        sequence: AtomicU64,
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
            sequence: AtomicU64::new(1),
        }
    }

    fn seal(
        env: &Env,
        semantic_method: &str,
        path: &str,
        inner_body: &[u8],
    ) -> (Vec<u8>, DecryptedRequest) {
        let wire_body = {
            let eph = env.state.eph.read().unwrap();
            encrypt_request_for_tests(
                &env.client_priv,
                &env.server_pub,
                Some(&PublicKey::from(eph.current.public)),
                &env.bearer,
                env.sequence.fetch_add(1, Ordering::Relaxed),
                semantic_method,
                path,
                inner_body,
            )
        };
        let server_private = StaticSecret::from(env.state.server_priv_bytes);
        let opened = {
            let eph = env.state.eph.read().unwrap();
            secure::decrypt_request(&wire_body, &server_private, &eph, semantic_method, path)
                .unwrap()
        };
        (wire_body, opened)
    }

    async fn dispatch_wire(
        env: &Env,
        semantic_method: &str,
        path: &str,
        wire_body: Vec<u8>,
    ) -> (StatusCode, Vec<u8>) {
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

    async fn send(
        env: &Env,
        semantic_method: &str,
        path: &str,
        inner_body: &[u8],
    ) -> (StatusCode, Vec<u8>) {
        let (wire_body, _) = seal(env, semantic_method, path, inner_body);
        dispatch_wire(env, semantic_method, path, wire_body).await
    }

    async fn send_semantic(
        env: &Env,
        semantic_method: &str,
        path: &str,
        inner_body: &[u8],
    ) -> (u16, Vec<u8>) {
        let (wire_body, opened_request) = seal(env, semantic_method, path, inner_body);
        let (wire_status, wire_response) =
            dispatch_wire(env, semantic_method, path, wire_body).await;
        assert_eq!(wire_status, StatusCode::OK);
        decrypt_response_for_tests(
            &wire_response,
            &opened_request.key_material,
            opened_request.mode,
            semantic_method,
            path,
            &opened_request.nonce_req,
        )
    }

    /// Regression guard for the 204-strip bug. The semantic 204 now lives
    /// inside ciphertext while the actual wire response is HTTP 200, so hyper
    /// cannot discard the encrypted envelope as a no-content response.
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

        let perf = env.state.perf.snapshot();
        assert_eq!(perf.requests.total, 1);
        assert_eq!(perf.requests.errors, 0);
        assert_eq!(perf.requests.objects, 1);
        assert_eq!(perf.requests.plaintext_bytes_in, payload.len() as u64);
        assert_eq!(perf.requests.plaintext_bytes_out, 0);
        assert!(perf.requests.wire_bytes_in > perf.requests.plaintext_bytes_in);
        assert!(perf.requests.wire_bytes_out > 0);
        assert!(perf.requests.envelope_open_ns > 0);
        assert!(perf.requests.token_replay_ns > 0);
        assert!(perf.requests.handler_ns > 0);
        assert!(perf.requests.response_seal_ns > 0);
        assert_eq!(perf.storage.loose_writes, 0);
        assert_eq!(perf.storage.pack_appends, 1);
        assert_eq!(perf.storage.fdatasyncs, 1);
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
    async fn content_chunk_above_axum_default_body_limit_is_accepted() {
        let env = setup();
        // FastCDC permits chunks up to 4 MiB. Axum's Bytes extractor defaults
        // to 2 MiB, so the decrypted inner request must explicitly inherit the
        // transport's larger bounded limit.
        let payload = vec![0x5a; 3 * 1024 * 1024];
        let hash = hash_bytes(&payload);
        let path = format!("/api/v1/content/chunk/{}", hash_to_hex(&hash));
        let (wire_body, opened_request) = seal(&env, "PUT", &path, &payload);

        let (wire_status, wire_response) = dispatch_wire(&env, "PUT", &path, wire_body).await;
        let (semantic_status, _) = decrypt_response_for_tests(
            &wire_response,
            &opened_request.key_material,
            opened_request.mode,
            "PUT",
            &path,
            &opened_request.nonce_req,
        );

        assert_eq!(wire_status, StatusCode::OK);
        assert_eq!(semantic_status, StatusCode::NO_CONTENT.as_u16());
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::ContentChunk, &hash));
        assert!(!env.state.layout.content_chunk_path(&hash).exists());
    }

    #[tokio::test]
    async fn accepted_root_uses_server_history_metadata_not_global_index_cas() {
        let env = setup();
        let vault = "root-metadata";
        let submitted = sync_core::chunk::RootNode {
            vault_id: vault.into(),
            created_ms: 1,
            version: 1,
            children: vec![],
            total_files: 0,
            parent_hash: Some(hash_bytes(b"forged-parent")),
            device_id: "forged-device".into(),
        };
        let root_hash = submitted.hash();
        let mut payload = "0".repeat(64).into_bytes();
        payload.extend_from_slice(&submitted.serialize());
        let path = format!("/api/v1/root/{vault}");
        let (wire_body, opened_request) = seal(&env, "PUT", &path, &payload);

        let (wire_status, wire_response) = dispatch_wire(&env, "PUT", &path, wire_body).await;
        let (semantic_status, _) = decrypt_response_for_tests(
            &wire_response,
            &opened_request.key_material,
            opened_request.mode,
            "PUT",
            &path,
            &opened_request.nonce_req,
        );
        assert_eq!(wire_status, StatusCode::OK);
        assert_eq!(semantic_status, StatusCode::OK.as_u16());

        let stored = sync_core::chunk::RootNode::deserialize(
            &env.state.vaults.get_root(vault, &root_hash).unwrap(),
        )
        .unwrap();
        assert_eq!(stored.device_id, "c0ffeec0ffeec0ffeec0ffeec0ffeedeadbeef");
        assert!(stored.created_ms > 1);
        assert_eq!(stored.parent_hash, None);
        assert!(
            !env.state.layout.index_path(&root_hash).exists(),
            "semantic root bytes must not pollute the global byte-addressed index",
        );
    }

    #[tokio::test]
    async fn nonempty_root_validation_reads_index_and_content_from_pack_segments() {
        let env = setup();
        let vault = "packed-root";
        let content = b"packed root content".to_vec();
        let content_hash = hash_bytes(&content);
        let content_path = format!("/api/v1/content/{}", hash_to_hex(&content_hash));
        assert_eq!(
            send(&env, "PUT", &content_path, &content).await.0,
            StatusCode::OK
        );
        let entry = sync_core::chunk::FileEntry::new(
            "note.md".into(),
            content_hash,
            1_800_000_000_000,
            content.len() as u64,
        );
        let root = sync_core::tree::build_tree(
            &env.state.storage_writer,
            vec![entry],
            vault,
            "packed-test",
        )
        .await
        .unwrap();
        let mut payload = "0".repeat(64).into_bytes();
        payload.extend_from_slice(&root.serialize());
        let path = format!("/api/v1/root/{vault}");
        let (status, _) = send_semantic(&env, "PUT", &path, &payload).await;

        assert_eq!(status, StatusCode::OK.as_u16());
        assert_eq!(env.state.vaults.get_current_root(vault), Some(root.hash()));
        assert!(!env.state.layout.content_blob_path(&content_hash).exists());
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::Content, &content_hash));
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

    #[tokio::test]
    async fn manifest_is_accepted_only_after_full_content_validation() {
        let env = setup();
        let first = b"verified ".to_vec();
        let second = b"chunks".to_vec();
        let all = [first.as_slice(), second.as_slice()].concat();
        let first_hash = hash_bytes(&first);
        let second_hash = hash_bytes(&second);
        let file_hash = hash_bytes(&all);

        for (hash, bytes) in [(first_hash, first), (second_hash, second)] {
            let path = format!("/api/v1/content/chunk/{}", hash_to_hex(&hash));
            let (status, _) = send(&env, "PUT", &path, &bytes).await;
            assert_eq!(status, StatusCode::OK);
        }

        let body = serde_json::to_vec(&serde_json::json!({
            "file_hash": hash_to_hex(&file_hash),
            "total_size": all.len(),
            "chunks": [
                { "hash": hash_to_hex(&first_hash), "offset": 0, "size": 9 },
                { "hash": hash_to_hex(&second_hash), "offset": 9, "size": 6 }
            ]
        }))
        .unwrap();
        let path = format!("/api/v1/content/manifest/{}", hash_to_hex(&file_hash));
        let (status, _) = send(&env, "PUT", &path, &body).await;
        assert_eq!(status, StatusCode::OK);
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::Manifest, &file_hash));
        assert!(!env.state.layout.content_manifest_path(&file_hash).exists());
    }

    #[tokio::test]
    async fn manifest_with_a_hole_is_rejected() {
        let env = setup();
        let bytes = b"chunk".to_vec();
        let chunk_hash = hash_bytes(&bytes);
        let file_hash = hash_bytes(&bytes);
        let chunk_path = format!("/api/v1/content/chunk/{}", hash_to_hex(&chunk_hash));
        assert_eq!(
            send(&env, "PUT", &chunk_path, &bytes).await.0,
            StatusCode::OK
        );

        let body = serde_json::to_vec(&serde_json::json!({
            "file_hash": hash_to_hex(&file_hash),
            "total_size": bytes.len() + 1,
            "chunks": [{
                "hash": hash_to_hex(&chunk_hash),
                "offset": 1,
                "size": bytes.len()
            }]
        }))
        .unwrap();
        let path = format!("/api/v1/content/manifest/{}", hash_to_hex(&file_hash));
        let (status, _) = send(&env, "PUT", &path, &body).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "semantic errors stay inside the envelope"
        );
        assert!(!env.state.layout.content_manifest_path(&file_hash).exists());
    }

    #[tokio::test]
    async fn manifest_check_requests_reupload_for_corrupt_json() {
        let env = setup();
        let file_hash = hash_bytes(b"large file identity");
        let manifest_path = env.state.layout.content_manifest_path(&file_hash);
        std::fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        std::fs::write(&manifest_path, b"{not-json").unwrap();

        let path = "/api/v1/content/manifests/check";
        let request_body = serde_json::to_vec(&vec![hash_to_hex(&file_hash)]).unwrap();
        let (wire_body, opened_request) = seal(&env, "POST", path, &request_body);
        let (wire_status, wire_response) = dispatch_wire(&env, "POST", path, wire_body).await;
        let (semantic_status, body) = decrypt_response_for_tests(
            &wire_response,
            &opened_request.key_material,
            opened_request.mode,
            "POST",
            path,
            &opened_request.nonce_req,
        );

        assert_eq!(wire_status, StatusCode::OK);
        assert_eq!(semantic_status, StatusCode::OK.as_u16());
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(response["needed"][0], hash_to_hex(&file_hash));
    }

    #[tokio::test]
    async fn capability_bundle_advertises_only_implemented_bulk_path_and_hard_limits() {
        let env = setup();
        let (status, body) = send_semantic(&env, "POST", "/api/v1/capabilities", &[]).await;
        assert_eq!(status, StatusCode::OK.as_u16());
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["capabilities"], serde_json::json!(["bulk-http-v1"]));
        assert_eq!(value["limits"]["bulk_request_bytes"], BULK_REQUEST_BYTES);
        assert_eq!(value["limits"]["bulk_objects"], BULK_OBJECTS);
        assert!(!value["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|capability| capability == "ws-data-v1"));
    }

    #[tokio::test]
    async fn bulk_put_check_and_get_preserve_order_mixed_ack_and_hash_integrity() {
        let env = setup();
        let first = b"first bulk object";
        let second = b"second bulk object";
        let first_hash = hash_bytes(first);
        let second_hash = hash_bytes(second);
        let declared_bad_hash = hash_bytes(b"not the supplied bytes");
        let records = [
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: first_hash,
                plain_len: first.len() as u32,
                bytes: first,
            },
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: first_hash,
                plain_len: first.len() as u32,
                bytes: first,
            },
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: declared_bad_hash,
                plain_len: second.len() as u32,
                bytes: second,
            },
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: second_hash,
                plain_len: second.len() as u32,
                bytes: second,
            },
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 1,
                hash: second_hash,
                plain_len: second.len() as u32,
                bytes: second,
            },
        ];
        let pack = bulk::encode_upload_pack(&records).unwrap();
        let (put_status, ack) = send_semantic(&env, "POST", "/api/v1/bulk/put", &pack).await;
        assert_eq!(put_status, StatusCode::OK.as_u16());
        assert_eq!(&ack[..4], bulk::PACK_ACK_MAGIC);
        assert_eq!(&ack[8..], &[0, 1, 2, 0, 3]);
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::Content, &first_hash));
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::Content, &second_hash));
        assert!(!env
            .state
            .storage_writer
            .contains(StorageObjectKind::Content, &declared_bad_hash));
        assert!(!env.state.layout.content_blob_path(&first_hash).exists());
        assert!(!env.state.layout.content_blob_path(&second_hash).exists());
        let storage = env.state.perf.snapshot().storage;
        assert_eq!(storage.pack_appends, 2);
        assert_eq!(storage.fdatasyncs, 1);
        assert_eq!(storage.loose_writes, 0);

        let missing_hash = hash_bytes(b"missing");
        let check = bulk::encode_check_request(
            ObjectKind::Content,
            &[first_hash, missing_hash, second_hash],
        )
        .unwrap();
        let (check_status, check_ack) =
            send_semantic(&env, "POST", "/api/v1/bulk/check", &check).await;
        assert_eq!(check_status, StatusCode::OK.as_u16());
        assert_eq!(&check_ack[..4], bulk::CHECK_ACK_MAGIC);
        assert_eq!(check_ack[8], 0b0000_0010);

        let get = bulk::encode_get_request(
            ObjectKind::Content,
            &[second_hash, first_hash],
            0,
            BULK_REQUEST_BYTES as u32,
        )
        .unwrap();
        let (get_status, download) = send_semantic(&env, "POST", "/api/v1/bulk/get", &get).await;
        assert_eq!(get_status, StatusCode::OK.as_u16());
        assert_eq!(&download[..4], bulk::DOWNLOAD_MAGIC);
        assert_eq!(u32::from_le_bytes(download[8..12].try_into().unwrap()), 2);
        let bitmap_len = bulk::bitmap_bytes(2).unwrap();
        assert_eq!(download[12], 0);
        let returned = bulk::decode_upload_pack(
            &download[DOWNLOAD_HEADER_BYTES + bitmap_len..],
            BULK_OBJECTS,
            BULK_REQUEST_BYTES,
        )
        .unwrap();
        assert_eq!(returned.records.len(), 2);
        assert_eq!(returned.records[0].hash, second_hash);
        assert_eq!(returned.records[0].bytes, second);
        assert_eq!(returned.records[1].hash, first_hash);
        assert_eq!(returned.records[1].bytes, first);
    }

    #[tokio::test]
    async fn bulk_download_pages_at_byte_budget_without_repeating_records() {
        let env = setup();
        let first = vec![1u8; 64];
        let second = vec![2u8; 64];
        let hashes = [hash_bytes(&first), hash_bytes(&second)];
        let records = [
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: hashes[0],
                plain_len: first.len() as u32,
                bytes: &first,
            },
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: hashes[1],
                plain_len: second.len() as u32,
                bytes: &second,
            },
        ];
        let pack = bulk::encode_upload_pack(&records).unwrap();
        assert_eq!(
            send_semantic(&env, "POST", "/api/v1/bulk/put", &pack)
                .await
                .0,
            StatusCode::OK.as_u16()
        );

        let bitmap_len = bulk::bitmap_bytes(hashes.len()).unwrap();
        let one_record_budget = DOWNLOAD_HEADER_BYTES
            + bitmap_len
            + PACK_HEADER_BYTES
            + RECORD_HEADER_BYTES
            + first.len();
        let first_request =
            bulk::encode_get_request(ObjectKind::Content, &hashes, 0, one_record_budget as u32)
                .unwrap();
        let (_, first_page) = send_semantic(&env, "POST", "/api/v1/bulk/get", &first_request).await;
        assert_eq!(u32::from_le_bytes(first_page[8..12].try_into().unwrap()), 1);
        assert_eq!(first_page[12], 0b0000_0010);
        let first_pack = bulk::decode_upload_pack(
            &first_page[DOWNLOAD_HEADER_BYTES + bitmap_len..],
            BULK_OBJECTS,
            BULK_REQUEST_BYTES,
        )
        .unwrap();
        assert_eq!(first_pack.records.len(), 1);
        assert_eq!(first_pack.records[0].hash, hashes[0]);

        let second_request =
            bulk::encode_get_request(ObjectKind::Content, &hashes, 1, one_record_budget as u32)
                .unwrap();
        let (_, second_page) =
            send_semantic(&env, "POST", "/api/v1/bulk/get", &second_request).await;
        assert_eq!(
            u32::from_le_bytes(second_page[8..12].try_into().unwrap()),
            2
        );
        assert_eq!(second_page[12], 0);
        let second_pack = bulk::decode_upload_pack(
            &second_page[DOWNLOAD_HEADER_BYTES + bitmap_len..],
            BULK_OBJECTS,
            BULK_REQUEST_BYTES,
        )
        .unwrap();
        assert_eq!(second_pack.records.len(), 1);
        assert_eq!(second_pack.records[0].hash, hashes[1]);
    }

    #[tokio::test]
    async fn bulk_pack_can_store_chunks_then_validate_manifest_in_one_request() {
        let env = setup();
        let chunk = b"bulk manifest content";
        let chunk_hash = hash_bytes(chunk);
        let file_hash = chunk_hash;
        let manifest = serde_json::to_vec(&serde_json::json!({
            "file_hash": hash_to_hex(&file_hash),
            "total_size": chunk.len(),
            "chunks": [{
                "hash": hash_to_hex(&chunk_hash),
                "offset": 0,
                "size": chunk.len(),
            }],
        }))
        .unwrap();
        let records = [
            UploadRecord {
                kind: ObjectKind::ContentChunk,
                flags: 0,
                hash: chunk_hash,
                plain_len: chunk.len() as u32,
                bytes: chunk,
            },
            UploadRecord {
                kind: ObjectKind::Manifest,
                flags: 0,
                hash: file_hash,
                plain_len: manifest.len() as u32,
                bytes: &manifest,
            },
        ];
        let pack = bulk::encode_upload_pack(&records).unwrap();
        let (status, ack) = send_semantic(&env, "POST", "/api/v1/bulk/put", &pack).await;
        assert_eq!(status, StatusCode::OK.as_u16());
        assert_eq!(&ack[8..], &[0, 0]);
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::ContentChunk, &chunk_hash));
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::Manifest, &file_hash));
        assert!(!env.state.layout.content_chunk_path(&chunk_hash).exists());
        assert!(!env.state.layout.content_manifest_path(&file_hash).exists());
        let storage = env.state.perf.snapshot().storage;
        assert_eq!(storage.pack_appends, 2);
        assert_eq!(storage.fdatasyncs, 1);
        assert_eq!(storage.loose_writes, 0);
    }

    #[tokio::test]
    async fn bulk_pack_retry_is_idempotent_without_another_segment_append() {
        let env = setup();
        let first = b"first idempotent packed object".to_vec();
        let first_hash = hash_bytes(&first);
        let second = b"second idempotent packed object".to_vec();
        let second_hash = hash_bytes(&second);

        let records = [
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: first_hash,
                plain_len: first.len() as u32,
                bytes: &first,
            },
            UploadRecord {
                kind: ObjectKind::Content,
                flags: 0,
                hash: second_hash,
                plain_len: second.len() as u32,
                bytes: &second,
            },
        ];
        let pack = bulk::encode_upload_pack(&records).unwrap();
        let (status, first_ack) = send_semantic(&env, "POST", "/api/v1/bulk/put", &pack).await;
        assert_eq!(status, StatusCode::OK.as_u16());
        assert_eq!(&first_ack[8..], &[0, 0]);
        let after_first = env.state.perf.snapshot().storage;
        assert_eq!(after_first.pack_appends, 2);
        assert_eq!(after_first.fdatasyncs, 1);

        let (_, retry_ack) = send_semantic(&env, "POST", "/api/v1/bulk/put", &pack).await;
        assert_eq!(&retry_ack[8..], &[1, 1]);
        let after_retry = env.state.perf.snapshot().storage;
        assert_eq!(after_retry.pack_appends, 2);
        assert_eq!(after_retry.fdatasyncs, 1);
        assert_eq!(
            env.state
                .storage_writer
                .read(StorageObjectKind::Content, &first_hash)
                .unwrap(),
            Some(first)
        );
        assert_eq!(
            env.state
                .storage_writer
                .read(StorageObjectKind::Content, &second_hash)
                .unwrap(),
            Some(second)
        );
    }

    #[tokio::test]
    async fn bulk_manifest_and_chunk_retry_are_group_idempotent() {
        let env = setup();
        let chunk = b"manifest waits for durable chunk";
        let chunk_hash = hash_bytes(chunk);
        let file_hash = chunk_hash;
        let manifest = serde_json::to_vec(&serde_json::json!({
            "file_hash": hash_to_hex(&file_hash),
            "total_size": chunk.len(),
            "chunks": [{
                "hash": hash_to_hex(&chunk_hash),
                "offset": 0,
                "size": chunk.len(),
            }],
        }))
        .unwrap();
        let records = [
            UploadRecord {
                kind: ObjectKind::ContentChunk,
                flags: 0,
                hash: chunk_hash,
                plain_len: chunk.len() as u32,
                bytes: chunk,
            },
            UploadRecord {
                kind: ObjectKind::Manifest,
                flags: 0,
                hash: file_hash,
                plain_len: manifest.len() as u32,
                bytes: &manifest,
            },
        ];
        let pack = bulk::encode_upload_pack(&records).unwrap();
        let (_, first_ack) = send_semantic(&env, "POST", "/api/v1/bulk/put", &pack).await;
        assert_eq!(&first_ack[8..], &[0, 0]);
        assert!(stored_manifest_is_usable(&env.state, &file_hash));

        let (_, retry_ack) = send_semantic(&env, "POST", "/api/v1/bulk/put", &pack).await;
        assert_eq!(&retry_ack[8..], &[1, 1]);
        assert_eq!(env.state.perf.snapshot().storage.fdatasyncs, 1);
        assert!(env
            .state
            .storage_writer
            .contains(StorageObjectKind::ContentChunk, &chunk_hash));
        assert!(stored_manifest_is_usable(&env.state, &file_hash));
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
            Some(&PublicKey::from(
                env.state.eph.read().unwrap().current.public,
            )),
            &env.bearer,
            env.sequence.fetch_add(1, Ordering::Relaxed),
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
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.len(), 256);
        assert!(body.iter().all(|byte| *byte == 0));
    }

    #[tokio::test]
    async fn exact_same_endpoint_replay_is_rejected_inside_the_envelope() {
        let env = setup();
        let path = "/api/v1/root/replay-vault";
        let (wire_body, opened_request) = seal(&env, "GET", path, &[]);

        let (first_wire_status, first_wire_body) =
            dispatch_wire(&env, "GET", path, wire_body.clone()).await;
        let (first_status, _) = decrypt_response_for_tests(
            &first_wire_body,
            &opened_request.key_material,
            opened_request.mode,
            "GET",
            path,
            &opened_request.nonce_req,
        );
        assert_eq!(first_wire_status, StatusCode::OK);
        assert_eq!(first_status, StatusCode::NOT_FOUND.as_u16());

        let (replay_wire_status, replay_wire_body) =
            dispatch_wire(&env, "GET", path, wire_body).await;
        let (replay_status, replay_body) = decrypt_response_for_tests(
            &replay_wire_body,
            &opened_request.key_material,
            opened_request.mode,
            "GET",
            path,
            &opened_request.nonce_req,
        );
        assert_eq!(replay_wire_status, StatusCode::OK);
        assert_eq!(replay_status, StatusCode::UNAUTHORIZED.as_u16());
        let replay_json: serde_json::Value = serde_json::from_slice(&replay_body).unwrap();
        assert_eq!(replay_json["error"], "replay");
        assert_eq!(replay_json["last_seen_seq"], 4096);
    }

    #[tokio::test]
    async fn bootstrap_is_credential_free_and_bypasses_replay_state() {
        let env = setup();
        let path = secure::BOOTSTRAP_PATH;
        let wire_body = encrypt_request_for_tests(
            &env.client_priv,
            &env.server_pub,
            None,
            &env.bearer,
            999,
            "POST",
            path,
            b"",
        );
        let server_private = StaticSecret::from(env.state.server_priv_bytes);
        let opened_request = {
            let eph = env.state.eph.read().unwrap();
            secure::decrypt_request(&wire_body, &server_private, &eph, "POST", path).unwrap()
        };
        assert!(opened_request.bearer_token.is_empty());
        assert_eq!(opened_request.sequence, 0);

        let (wire_status, wire_response) = dispatch_wire(&env, "POST", path, wire_body).await;
        let (semantic_status, body) = decrypt_response_for_tests(
            &wire_response,
            &opened_request.key_material,
            opened_request.mode,
            "POST",
            path,
            &opened_request.nonce_req,
        );
        assert_eq!(wire_status, StatusCode::OK);
        assert_eq!(semantic_status, StatusCode::OK.as_u16());
        let bundle: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(bundle["Es_pub"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
        assert!(bundle["valid_until"].as_u64().is_some());
        assert_eq!(bundle["capabilities"], serde_json::json!(["bulk-http-v1"]));
        assert_eq!(bundle["limits"]["bulk_request_bytes"], BULK_REQUEST_BYTES);
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
