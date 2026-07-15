use crate::devices;
use crate::enrollment;
use crate::state::SharedState;
use axum::{
    extract::{Form, Path, State},
    http::{Method, StatusCode},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
    Router,
};
use sync_core::hash::hash_to_hex;
use tower_http::cors::CorsLayer;

pub fn admin_router(state: SharedState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::POST]);

    Router::new()
        .route("/", get(|| async { Redirect::permanent("/admin") }))
        .route("/admin", get(dashboard))
        .route("/admin/devices", get(device_list))
        .route(
            "/admin/devices/new",
            get(new_device_form).post(create_device),
        )
        .route("/admin/devices/{device_id}", get(device_detail))
        .route("/admin/devices/{device_id}/revoke", post(revoke_device))
        .route("/admin/vaults", get(vault_list))
        .route("/admin/vaults/{vault_id}", get(vault_detail))
        .route("/admin/vaults/{vault_id}/rollback", post(rollback_vault))
        .route("/admin/vaults/{vault_id}/purge", post(purge_vault))
        .route(
            "/admin/vaults/{vault_id}/export/{root_hash}",
            get(export_vault),
        )
        .route("/admin/enrollment/{code}", get(claim_enrollment))
        .layer(cors)
        .with_state(state)
}

// --- Dashboard ---

async fn dashboard(State(state): State<SharedState>) -> Html<String> {
    let device_list = devices::list_devices(&state.layout).unwrap_or_default();
    let online = device_list
        .iter()
        .filter(|d| is_recent(d.last_seen))
        .count();
    let revoked = device_list
        .iter()
        .filter(|d| devices::is_revoked(&state.layout, &d.device_id))
        .count();

    // Vault inventory: list each vault + size of its current root (if any).
    let vaults_dir = state.layout.base.join("vaults");
    let mut vaults: Vec<(String, Option<u64>, Option<u64>)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&vaults_dir) {
        for e in entries.filter_map(|e| e.ok()) {
            if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let name = e.file_name().to_string_lossy().to_string();
                let (size, mtime) = current_root_stats(&state, &name);
                vaults.push((name, size, mtime));
            }
        }
    }
    vaults.sort_by(|a, b| a.0.cmp(&b.0));

    // Storage breakdown. content/ contains blobs + manifests/ + chunks/ —
    // subtract the nested buckets so "small-file blobs" isolates the direct-
    // content bytes cleanly.
    let (idx_bytes, idx_files) = dir_stats(&state.layout.base.join("index"));
    let content_root = state.layout.base.join("content");
    let (manifest_bytes, manifest_files) = dir_stats(&content_root.join("manifests"));
    let (chunk_bytes, chunk_files) = dir_stats(&content_root.join("chunks"));
    let (content_all_bytes, content_all_files) = dir_stats(&content_root);
    let blob_bytes = content_all_bytes.saturating_sub(manifest_bytes + chunk_bytes);
    let blob_files = content_all_files.saturating_sub(manifest_files + chunk_files);
    let total_bytes = idx_bytes + content_all_bytes;

    let uptime = format_duration(state.started_at.elapsed());

    let device_rows: String = device_list
        .iter()
        .map(|d| {
            let dot = if devices::is_revoked(&state.layout, &d.device_id) {
                "⛔"
            } else if is_recent(d.last_seen) {
                "🟢"
            } else {
                "⚪"
            };
            let status = if devices::is_revoked(&state.layout, &d.device_id) {
                "Revoked"
            } else if is_recent(d.last_seen) {
                "Online"
            } else {
                "Offline"
            };
            format!(
                "<tr><td>{} {}</td><td>{}</td><td>{}</td><td>{}</td>\
                 <td><a href='/admin/devices/{}'>details</a></td></tr>",
                dot,
                d.name,
                status,
                format_time(d.last_seen),
                format_time(d.enrolled_at),
                d.device_id
            )
        })
        .collect();

    let vault_rows: String = vaults
        .iter()
        .map(|(name, size, mtime)| {
            format!(
                "<tr><td><a href='/admin/vaults/{name}'>{name}</a></td>\
                 <td>{}</td><td>{}</td></tr>",
                size.map(format_bytes).unwrap_or_else(|| "(empty)".into()),
                mtime.map(format_time).unwrap_or_else(|| "never".into()),
            )
        })
        .collect();

    Html(format!(
        r#"<!DOCTYPE html>
<html><head>
<title>ObsetyNC Admin</title>
<meta http-equiv="refresh" content="30">
{CSS}
</head>
<body>
<h1>ObsetyNC Server</h1>

<div class="kpi-grid">
  <div class="kpi"><div class="kpi-label">Uptime</div><div class="kpi-val">{uptime}</div></div>
  <div class="kpi"><div class="kpi-label">Devices</div>
    <div class="kpi-val">{} <span class="kpi-sub">({} online, {} revoked)</span></div></div>
  <div class="kpi"><div class="kpi-label">Vaults</div><div class="kpi-val">{}</div></div>
  <div class="kpi"><div class="kpi-label">Total storage</div><div class="kpi-val">{}</div></div>
</div>

<h2>Storage breakdown</h2>
<table>
<tr><th>Category</th><th>Files</th><th>Bytes</th></tr>
<tr><td>Index (Merkle tree)</td><td>{}</td><td>{}</td></tr>
<tr><td>Small-file blobs</td><td>{}</td><td>{}</td></tr>
<tr><td>Large-file manifests</td><td>{}</td><td>{}</td></tr>
<tr><td>Large-file sub-chunks</td><td>{}</td><td>{}</td></tr>
</table>

<h2>Vaults</h2>
<table>
<tr><th>Vault</th><th>Current root</th><th>Last push</th></tr>
{vault_rows}
</table>

<h2>Devices</h2>
<table>
<tr><th>Name</th><th>Status</th><th>Last Seen</th><th>Enrolled</th><th></th></tr>
{device_rows}
</table>
<p><a href="/admin/devices/new" class="btn">+ Add Device</a></p>
<p class="footer">Dashboard auto-refreshes every 30 s.</p>
</body></html>"#,
        device_list.len(),
        online,
        revoked,
        vaults.len(),
        format_bytes(total_bytes),
        idx_files,
        format_bytes(idx_bytes),
        blob_files,
        format_bytes(blob_bytes),
        manifest_files,
        format_bytes(manifest_bytes),
        chunk_files,
        format_bytes(chunk_bytes),
    ))
}

// --- Device List ---

async fn device_list(State(state): State<SharedState>) -> Html<String> {
    let devices = devices::list_devices(&state.layout).unwrap_or_default();
    let rows: String = devices
        .iter()
        .map(|d| {
            let revoked = devices::is_revoked(&state.layout, &d.device_id);
            let status = if revoked {
                "Revoked"
            } else if is_recent(d.last_seen) {
                "Online"
            } else {
                "Offline"
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
                 <td><a href='/admin/devices/{}'>details</a></td></tr>",
                d.name,
                status,
                format_time(d.last_seen),
                format_time(d.enrolled_at),
                if d.vaults.is_empty() {
                    "—".into()
                } else {
                    d.vaults.join(", ")
                },
                d.device_id
            )
        })
        .collect();

    Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>Devices - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / Devices</h1>
<table>
<tr><th>Name</th><th>Status</th><th>Last Seen</th><th>Enrolled</th><th>Vaults</th><th></th></tr>
{rows}
</table>
<p><a href="/admin/devices/new" class="btn">+ Add Device</a></p>
</body></html>"#
    ))
}

// --- New Device ---

#[derive(serde::Deserialize)]
struct NewDeviceForm {
    device_name: String,
}

async fn new_device_form() -> Html<String> {
    Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>Add Device - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / <a href="/admin/devices">Devices</a> / Add</h1>
<form method="POST">
    <label>Device Name:</label><br>
    <input type="text" name="device_name" placeholder="e.g. iPhone, Desktop Home" required><br><br>
    <button type="submit" class="btn">Generate Certificate</button>
</form>
</body></html>"#
    ))
}

async fn create_device(
    State(state): State<SharedState>,
    Form(form): Form<NewDeviceForm>,
) -> Result<Html<String>, ServerErrorHtml> {
    let info = enrollment::create_enrollment(&state.layout, &form.device_name)
        .map_err(|e| ServerErrorHtml(format!("enrollment failed: {}", e)))?;

    tracing::info!(
        device_name = %info.device_name,
        device = %&info.device_id[..info.device_id.len().min(12)],
        code = %info.code,
        "enrollment: code issued"
    );

    Ok(Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>Device Created - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / Device Created</h1>
<div class="success">
    <p>Certificate generated for <strong>{}</strong></p>
    <p>Enrollment code: <code class="code">{}</code></p>
    <p>Expires in 10 minutes.</p>
    <p>Device ID: <code>{}</code></p>
</div>
<h3>To enroll:</h3>
<ol>
    <li>Open Obsidian → Settings → ObsetyNC</li>
    <li>Enter the server URL and enrollment code</li>
    <li>Or visit: <code>/admin/enrollment/{}</code></li>
</ol>
<p><a href="/admin/devices">Back to devices</a></p>
</body></html>"#,
        info.device_name, info.code, info.device_id, info.code
    )))
}

// --- Device Detail ---

async fn device_detail(
    State(state): State<SharedState>,
    Path(device_id): Path<String>,
) -> Result<Html<String>, ServerErrorHtml> {
    let device = devices::get_device(&state.layout, &device_id)
        .ok_or_else(|| ServerErrorHtml("device not found".into()))?;

    let revoked = devices::is_revoked(&state.layout, &device_id);
    let status = if revoked {
        "Revoked"
    } else if is_recent(device.last_seen) {
        "Online"
    } else {
        "Offline"
    };

    let revoke_btn = if revoked {
        "<p><em>This device has been revoked.</em></p>".to_string()
    } else {
        format!(
            r#"<form method="POST" action="/admin/devices/{}/revoke" onsubmit="return confirm('Revoke this device?')">
            <button type="submit" class="btn btn-danger">Revoke Device</button>
            </form>"#,
            device_id
        )
    };

    Ok(Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>{} - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / <a href="/admin/devices">Devices</a> / {}</h1>
<table>
<tr><td>Status</td><td>{status}</td></tr>
<tr><td>Device ID</td><td><code>{}</code></td></tr>
<tr><td>Enrolled</td><td>{}</td></tr>
<tr><td>Last Seen</td><td>{}</td></tr>
</table>
{revoke_btn}
<p><a href="/admin/devices">Back to devices</a></p>
</body></html>"#,
        device.name,
        device.name,
        device_id,
        format_time(device.enrolled_at),
        format_time(device.last_seen),
    )))
}

// --- Revoke ---

async fn revoke_device(
    State(state): State<SharedState>,
    Path(device_id): Path<String>,
) -> Result<Redirect, ServerErrorHtml> {
    devices::revoke_device(&state.layout, &device_id)
        .map_err(|e| ServerErrorHtml(format!("revoke failed: {}", e)))?;
    tracing::info!(
        device = %&device_id[..device_id.len().min(12)],
        "devices: revoked"
    );
    Ok(Redirect::to(&format!("/admin/devices/{}", device_id)))
}

// --- Vault List ---

async fn vault_list(State(state): State<SharedState>) -> Html<String> {
    let vaults_dir = state.layout.base.join("vaults");
    let mut vaults = Vec::new();

    if let Ok(entries) = std::fs::read_dir(&vaults_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let name = entry.file_name().to_string_lossy().to_string();
                let has_root = state.vaults.get_current_root(&name).is_some();
                vaults.push((name, has_root));
            }
        }
    }
    vaults.sort();

    let rows: String = vaults
        .iter()
        .map(|(name, has_root)| {
            let status = if *has_root { "Active" } else { "Empty" };
            format!("<tr><td><a href='/admin/vaults/{name}'>{name}</a></td><td>{status}</td></tr>")
        })
        .collect();

    Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>Vaults - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / Vaults</h1>
<table>
<tr><th>Vault</th><th>Status</th></tr>
{rows}
</table>
<p><a href="/admin">Back</a></p>
</body></html>"#
    ))
}

// --- Vault Detail ---

async fn vault_detail(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
) -> Result<Html<String>, ServerErrorHtml> {
    let current = state.vaults.get_current_root(&vault_id);
    let current_hex = current
        .map(|h| hash_to_hex(&h))
        .unwrap_or_else(|| "none".into());

    // List all roots in history.
    let roots_dir = state.layout.vault_roots_dir(&vault_id);
    let mut roots = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&roots_dir) {
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(hash) = name.strip_suffix(".bin") {
                roots.push(hash.to_string());
            }
        }
    }
    roots.sort();
    roots.reverse();

    let root_rows: String = roots
        .iter()
        .map(|hash| {
            let is_current = hash == &current_hex;
            let marker = if is_current { " (current)" } else { "" };
            let rollback = if !is_current {
                format!(
                    r#"<form method="POST" action="/admin/vaults/{}/rollback" style="display:inline">
                    <input type="hidden" name="root_hash" value="{}">
                    <button type="submit" class="btn-small">rollback</button>
                    </form>"#,
                    vault_id, hash
                )
            } else {
                String::new()
            };
            format!(
                r#"<tr><td><code>{:.16}...</code>{marker}</td><td>{rollback} <a class="btn-small" href="/admin/vaults/{}/export/{}">export</a></td></tr>"#,
                hash, vault_id, hash
            )
        })
        .collect();

    Ok(Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>{vault_id} - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / <a href="/admin/vaults">Vaults</a> / {vault_id}</h1>
<p>Current root: <code>{:.16}...</code></p>
<h2>Root History</h2>
<table>
<tr><th>Root Hash</th><th></th></tr>
{root_rows}
</table>
<h2>Purge ignored paths</h2>
<p>Remove build output / junk (target/, node_modules/, .git/, …) from the current
root in one shot, so devices that ignore those paths can converge. This is
<strong>reversible</strong> — the pre-purge root stays in history above; roll back
to undo. Local files on each device are kept; they are only untracked. One
pattern per line, gitignore-style (<code>target/</code>, <code>*.tmp</code>).</p>
<form method="POST" action="/admin/vaults/{vault_id}/purge"
      onsubmit="return confirm('Purge matching paths from the current root? Reversible via rollback.')">
<textarea name="patterns" rows="5" cols="48" placeholder="target/&#10;node_modules/&#10;.git/&#10;*.tmp"></textarea><br>
<button type="submit" class="btn-small">purge</button>
</form>
<p><a href="/admin/vaults">Back to vaults</a></p>
</body></html>"#,
        current_hex,
    )))
}

// --- Rollback ---

#[derive(serde::Deserialize)]
struct RollbackForm {
    root_hash: String,
}

async fn rollback_vault(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    Form(form): Form<RollbackForm>,
) -> Result<Redirect, ServerErrorHtml> {
    let hash = sync_core::hash::hex_to_hash(&form.root_hash)
        .map_err(|_| ServerErrorHtml("invalid hash".into()))?;

    // Verify the root exists.
    state
        .vaults
        .get_root(&vault_id, &hash)
        .ok_or_else(|| ServerErrorHtml("root not found in history".into()))?;

    // Same per-vault write lock as put_root — a rollback racing an in-flight
    // push is the same lost-update bug in admin clothing.
    let vault_lock = state.vault_lock(&vault_id);
    let _vault_guard = vault_lock.lock().await;

    state
        .vaults
        .set_current_root(&vault_id, &hash)
        .map_err(|e| ServerErrorHtml(format!("rollback failed: {}", e)))?;
    state.notify_root_changed(&vault_id, &hash_to_hex(&hash));

    Ok(Redirect::to(&format!("/admin/vaults/{}", vault_id)))
}

// --- Purge ignored paths (Slice 2b) ---

#[derive(serde::Deserialize)]
struct PurgeForm {
    patterns: String,
}

/// POST /admin/vaults/{vault_id}/purge — rebuild the current root without the
/// paths matching the operator's gitignore-style patterns, and make it current.
/// Guard-exempt and COW-reversible (the pre-purge root stays in history), by the
/// same rationale as rollback: it is a deliberate operator action, not a client
/// push. Clients that ignore those paths untrack them on the next pull WITHOUT
/// deleting local files; a non-ignoring client would delete its copies, so the
/// fleet must share the ignore patterns first.
async fn purge_vault(
    State(state): State<SharedState>,
    Path(vault_id): Path<String>,
    Form(form): Form<PurgeForm>,
) -> Result<Redirect, ServerErrorHtml> {
    let patterns: Vec<String> = form
        .patterns
        .split(['\n', ','])
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if patterns.is_empty() {
        return Err(ServerErrorHtml("no purge patterns given".into()));
    }

    // Serialize with put_root/rollback: a purge racing an in-flight push is the
    // same lost-update hazard. Held across the whole read-modify-write.
    let vault_lock = state.vault_lock(&vault_id);
    let _vault_guard = vault_lock.lock().await;

    let current_hash = state
        .vaults
        .get_current_root(&vault_id)
        .ok_or_else(|| ServerErrorHtml("vault has no current root".into()))?;
    let current_bytes = state
        .vaults
        .get_root(&vault_id, &current_hash)
        .ok_or_else(|| ServerErrorHtml("current root data missing".into()))?;
    let current_root = sync_core::chunk::RootNode::deserialize(&current_bytes)
        .map_err(|e| ServerErrorHtml(format!("corrupt current root: {}", e)))?;

    let (mut new_root, removed, kept) =
        crate::bridge::run_purge(state.layout.base.join("index"), current_root, patterns)
            .await
            .map_err(|e| ServerErrorHtml(format!("purge failed: {}", e)))?;

    if removed == 0 {
        // Nothing matched — don't mint a redundant identical root.
        return Ok(Redirect::to(&format!("/admin/vaults/{}", vault_id)));
    }

    // Link the new root to the one it descends from so history stays a chain.
    // (parent_hash is metadata, not part of the content hash.)
    new_root.parent_hash = Some(current_hash);
    let new_bytes = new_root.serialize();
    let new_hash = new_root.hash();

    state
        .vaults
        .store_root(&vault_id, &new_hash, &new_bytes)
        .map_err(|e| ServerErrorHtml(format!("store root failed: {}", e)))?;
    let idx_path = state.layout.index_path(&new_hash);
    crate::storage::write_blob(&idx_path, &new_bytes)
        .map_err(|e| ServerErrorHtml(format!("write index failed: {}", e)))?;

    state
        .vaults
        .set_current_root(&vault_id, &new_hash)
        .map_err(|e| ServerErrorHtml(format!("set current failed: {}", e)))?;
    state.notify_root_changed(&vault_id, &hash_to_hex(&new_hash));

    tracing::warn!(
        vault = %vault_id,
        removed,
        kept,
        root = %&hash_to_hex(&new_hash)[..16],
        parent = %&hash_to_hex(&current_hash)[..16],
        "admin purge: removed ignored paths from current root (reversible via rollback)"
    );

    Ok(Redirect::to(&format!("/admin/vaults/{}", vault_id)))
}

// --- Vault export ---

/// GET /admin/vaults/{vault_id}/export/{root_hash} — download the vault as
/// it looked at any root in history, as a tar archive. Small files come
/// straight from the blob store; chunked large files are reassembled from
/// their FastCDC manifests. The tar is assembled into an anonymous tempfile
/// (blocking task) and streamed out, so memory stays bounded no matter how
/// big the vault is.
async fn export_vault(
    State(state): State<SharedState>,
    Path((vault_id, root_hash)): Path<(String, String)>,
) -> Result<axum::response::Response, ServerErrorHtml> {
    let hash = sync_core::hash::hex_to_hash(&root_hash)
        .map_err(|_| ServerErrorHtml("invalid root hash".into()))?;
    let root_bytes = state
        .vaults
        .get_root(&vault_id, &hash)
        .ok_or_else(|| ServerErrorHtml("root not found in history".into()))?;
    let root = sync_core::chunk::RootNode::deserialize(&root_bytes)
        .map_err(|e| ServerErrorHtml(format!("corrupt root: {}", e)))?;

    let entries = crate::bridge::run_list_entries(state.layout.base.join("index"), root)
        .await
        .map_err(|e| ServerErrorHtml(format!("failed to list entries: {}", e)))?;

    let layout = state.layout.clone();
    let file_count = entries.len();
    let tar_file = tokio::task::spawn_blocking(move || -> Result<std::fs::File, String> {
        use std::io::{Read, Seek, Write};

        // Anonymous tempfile: the OS reclaims it when the handle drops, even
        // if the download is abandoned mid-stream.
        let tmp = tempfile::tempfile().map_err(|e| e.to_string())?;
        let mut builder = tar::Builder::new(&tmp);

        for entry in &entries {
            let blob_path = layout.content_blob_path(&entry.hash);
            let mut data: Vec<u8> = Vec::new();
            if blob_path.exists() {
                std::fs::File::open(&blob_path)
                    .and_then(|mut f| f.read_to_end(&mut data))
                    .map_err(|e| format!("{}: {}", entry.path, e))?;
            } else {
                // Large file — reassemble from its manifest's chunk list.
                let manifest_path = layout.content_manifest_path(&entry.hash);
                let manifest_json = std::fs::read(&manifest_path)
                    .map_err(|e| format!("{}: manifest missing: {}", entry.path, e))?;
                let manifest: sync_core::content_store::FileManifest =
                    serde_json::from_slice(&manifest_json)
                        .map_err(|e| format!("{}: corrupt manifest: {}", entry.path, e))?;
                data.reserve(manifest.total_size as usize);
                for chunk in &manifest.chunks {
                    let chunk_path = layout.content_chunk_path(&chunk.hash);
                    std::fs::File::open(&chunk_path)
                        .and_then(|mut f| f.read_to_end(&mut data))
                        .map_err(|e| format!("{}: chunk missing: {}", entry.path, e))?;
                }
            }

            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(entry.mtime_ms / 1000);
            header.set_cksum();
            builder
                .append_data(&mut header, &entry.path, data.as_slice())
                .map_err(|e| format!("{}: tar append: {}", entry.path, e))?;
        }

        let mut inner = builder.into_inner().map_err(|e| e.to_string())?;
        inner.flush().map_err(|e| e.to_string())?;
        let mut file = tmp;
        file.seek(std::io::SeekFrom::Start(0))
            .map_err(|e| e.to_string())?;
        Ok(file)
    })
    .await
    .map_err(|e| ServerErrorHtml(format!("export task failed: {}", e)))?
    .map_err(ServerErrorHtml)?;

    tracing::info!(
        vault = %vault_id,
        root = %&root_hash[..root_hash.len().min(16)],
        files = file_count,
        "admin export served"
    );

    let stream = tokio_util::io::ReaderStream::new(tokio::fs::File::from_std(tar_file));
    let filename = format!("{}-{}.tar", vault_id, &root_hash[..root_hash.len().min(8)]);
    Ok((
        StatusCode::OK,
        [
            (
                axum::http::header::CONTENT_TYPE,
                "application/x-tar".to_string(),
            ),
            (
                axum::http::header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{}\"", filename),
            ),
        ],
        axum::body::Body::from_stream(stream),
    )
        .into_response())
}

// --- Enrollment Claim ---

async fn claim_enrollment(
    State(state): State<SharedState>,
    Path(code): Path<String>,
) -> impl IntoResponse {
    // Include the server's X25519 public key so the client can pin it for
    // all subsequent encrypted requests. Fetched fresh on every claim so
    // a key rotation propagates to newly enrolled devices.
    let box_pub = match crate::box_key::load_box_pub_base64(&state.layout) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                serde_json::json!({ "error": format!("server box key unavailable: {}", e) })
                    .to_string(),
            )
                .into_response();
        }
    };

    match enrollment::claim_enrollment(&state.layout, &code) {
        Ok(info) => {
            tracing::info!(
                device_name = %info.device_name,
                device = %&info.device_id[..info.device_id.len().min(12)],
                code = %code,
                "enrollment: claimed (bundle issued)"
            );
            let bundle = serde_json::json!({
                "device_name":     info.device_name,
                "device_id":       info.device_id,
                "bearer_token":    info.bearer_token,
                "server_box_pub":  box_pub,
            });
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                bundle.to_string(),
            )
                .into_response()
        }
        Err(e) => {
            tracing::warn!(code = %code, reason = %e, "enrollment: claim failed");
            (
                StatusCode::BAD_REQUEST,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                serde_json::json!({ "error": e }).to_string(),
            )
                .into_response()
        }
    }
}

// --- Error type for HTML pages ---

struct ServerErrorHtml(String);

impl IntoResponse for ServerErrorHtml {
    fn into_response(self) -> axum::response::Response {
        // Human-facing HTML, machine-truthful status: these pages used to
        // ship with 200 OK, which made scripted admin calls (export, curl'd
        // rollbacks) impossible to error-check.
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Html(format!(
                r#"<!DOCTYPE html>
<html><head><title>Error - ObsetyNC</title>{CSS}</head>
<body>
<h1>Error</h1>
<p class="error">{}</p>
<p><a href="/admin">Back to dashboard</a></p>
</body></html>"#,
                self.0
            )),
        )
            .into_response()
    }
}

// --- Helpers ---

fn is_recent(timestamp_ms: u64) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    now - timestamp_ms < 5 * 60 * 1000 // 5 minutes
}

fn format_time(timestamp_ms: u64) -> String {
    if timestamp_ms == 0 {
        return "never".into();
    }
    let secs = timestamp_ms / 1000;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let ago = now.saturating_sub(secs);
    if ago < 60 {
        "just now".into()
    } else if ago < 3600 {
        format!("{} min ago", ago / 60)
    } else if ago < 86400 {
        format!("{} hours ago", ago / 3600)
    } else {
        format!("{} days ago", ago / 86400)
    }
}

fn format_bytes(n: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    if n >= GB {
        format!("{:.2} GB", n as f64 / GB as f64)
    } else if n >= MB {
        format!("{:.1} MB", n as f64 / MB as f64)
    } else if n >= KB {
        format!("{} KB", n / KB)
    } else {
        format!("{} B", n)
    }
}

fn format_duration(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    let s = secs % 60;
    if days > 0 {
        format!("{}d {}h", days, hours)
    } else if hours > 0 {
        format!("{}h {}m", hours, mins)
    } else if mins > 0 {
        format!("{}m {}s", mins, s)
    } else {
        format!("{}s", s)
    }
}

/// Walk a directory tree and sum `(total_bytes, file_count)` of all regular
/// files. Used by the admin dashboard to show storage breakdown without
/// needing an external tool like `du`.
fn dir_stats(path: &std::path::Path) -> (u64, u64) {
    let mut bytes: u64 = 0;
    let mut count: u64 = 0;
    let entries = match std::fs::read_dir(path) {
        Ok(e) => e,
        Err(_) => return (0, 0),
    };
    for entry in entries.filter_map(|e| e.ok()) {
        match entry.file_type() {
            Ok(ft) if ft.is_file() => {
                if let Ok(m) = entry.metadata() {
                    bytes += m.len();
                    count += 1;
                }
            }
            Ok(ft) if ft.is_dir() => {
                let (b, c) = dir_stats(&entry.path());
                bytes += b;
                count += c;
            }
            _ => {}
        }
    }
    (bytes, count)
}

/// Size + last-modified-ms (epoch) of the current root blob for a vault.
/// Returns (None, None) if the vault has no current root.
fn current_root_stats(
    state: &crate::state::SharedState,
    vault_id: &str,
) -> (Option<u64>, Option<u64>) {
    let hash = match state.vaults.get_current_root(vault_id) {
        Some(h) => h,
        None => return (None, None),
    };
    let path = state.layout.vault_root_path(vault_id, &hash);
    match std::fs::metadata(&path) {
        Ok(m) => {
            let mtime_ms = m
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64);
            (Some(m.len()), mtime_ms)
        }
        Err(_) => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn format_bytes_units() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1 KB");
        assert_eq!(format_bytes(2048), "2 KB");
        // 1.5 MB
        assert_eq!(format_bytes(1024 * 1024 + 512 * 1024), "1.5 MB");
        // 2 GB exact
        assert_eq!(format_bytes(2u64 * 1024 * 1024 * 1024), "2.00 GB");
    }

    #[test]
    fn format_bytes_boundary_kb_lower() {
        // 1023 B stays in bytes; 1024 B promotes to KB.
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1 KB");
    }

    #[test]
    fn format_duration_buckets() {
        assert_eq!(format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(format_duration(Duration::from_secs(75)), "1m 15s");
        assert_eq!(
            format_duration(Duration::from_secs(3 * 3600 + 14 * 60)),
            "3h 14m"
        );
        assert_eq!(
            format_duration(Duration::from_secs(2 * 86400 + 5 * 3600)),
            "2d 5h"
        );
    }

    #[test]
    fn format_time_zero_is_never() {
        assert_eq!(format_time(0), "never");
    }

    #[test]
    fn format_time_recent_is_just_now() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert_eq!(format_time(now_ms), "just now");
    }

    #[test]
    fn format_time_minutes_ago() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // 5 minutes ago.
        let five_min = now_ms - 5 * 60 * 1000;
        let s = format_time(five_min);
        assert!(s.ends_with(" min ago"), "got: {}", s);
    }

    #[test]
    fn is_recent_threshold_is_five_minutes() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(is_recent(now_ms));
        assert!(is_recent(now_ms - 4 * 60 * 1000));
        // Older than 5 minutes — must be considered offline.
        assert!(!is_recent(now_ms - 10 * 60 * 1000));
    }

    #[test]
    fn dir_stats_returns_zero_for_missing_dir() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("absent");
        assert_eq!(dir_stats(&missing), (0, 0));
    }

    #[test]
    fn dir_stats_recurses_and_sums() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("a.bin"), b"hello").unwrap(); // 5
        std::fs::create_dir_all(root.join("sub/sub2")).unwrap();
        std::fs::write(root.join("sub/b.bin"), b"world!").unwrap(); // 6
        std::fs::write(root.join("sub/sub2/c.bin"), b"x").unwrap(); // 1

        let (bytes, count) = dir_stats(root);
        assert_eq!(count, 3);
        assert_eq!(bytes, 5 + 6 + 1);
    }

    #[test]
    fn dir_stats_empty_dir_zero() {
        let dir = tempdir().unwrap();
        assert_eq!(dir_stats(dir.path()), (0, 0));
    }
}

const CSS: &str = r#"<style>
body { font-family: -apple-system, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #333; }
h1 { border-bottom: 2px solid #eee; padding-bottom: 10px; }
h1 a { color: #333; text-decoration: none; }
h1 a:hover { color: #666; }
table { border-collapse: collapse; width: 100%; margin: 20px 0; }
th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: left; }
th { background: #f5f5f5; }
code { background: #f0f0f0; padding: 2px 6px; border-radius: 3px; font-size: 0.9em; }
.code { font-size: 1.4em; letter-spacing: 2px; background: #e8f5e9; padding: 8px 16px; }
.btn { display: inline-block; padding: 8px 16px; background: #2196F3; color: white; text-decoration: none; border-radius: 4px; border: none; cursor: pointer; font-size: 1em; }
.btn:hover { background: #1976D2; }
.btn-danger { background: #f44336; }
.btn-danger:hover { background: #d32f2f; }
.btn-small { padding: 4px 8px; font-size: 0.85em; background: #ff9800; color: white; border: none; border-radius: 3px; cursor: pointer; }
.success { background: #e8f5e9; border: 1px solid #c8e6c9; padding: 16px; border-radius: 4px; }
.error { color: #d32f2f; }
.stats { display: flex; gap: 30px; font-size: 1.1em; margin: 20px 0; }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 20px 0; }
.kpi { border: 1px solid #e0e0e0; border-radius: 6px; padding: 12px 16px; background: #fafafa; }
.kpi-label { color: #666; font-size: 0.85em; text-transform: uppercase; letter-spacing: 0.5px; }
.kpi-val   { font-size: 1.3em; font-weight: 600; margin-top: 4px; color: #1a73e8; }
.kpi-sub   { font-size: 0.75em; color: #888; font-weight: 400; }
.footer    { color: #888; font-size: 0.85em; margin-top: 30px; }
</style>"#;
