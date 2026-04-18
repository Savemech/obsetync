use axum::{
    Router,
    extract::{Form, Path, State},
    http::{Method, StatusCode},
    response::{Html, IntoResponse, Redirect},
    routing::{get, post},
};
use tower_http::cors::CorsLayer;
use crate::devices;
use crate::enrollment;
use crate::state::SharedState;
use sync_core::hash::hash_to_hex;

pub fn admin_router(state: SharedState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::POST]);

    Router::new()
        .route("/", get(|| async { Redirect::permanent("/admin") }))
        .route("/admin", get(dashboard))
        .route("/admin/devices", get(device_list))
        .route("/admin/devices/new", get(new_device_form).post(create_device))
        .route("/admin/devices/{fingerprint}", get(device_detail))
        .route("/admin/devices/{fingerprint}/revoke", post(revoke_device))
        .route("/admin/vaults", get(vault_list))
        .route("/admin/vaults/{vault_id}", get(vault_detail))
        .route("/admin/vaults/{vault_id}/rollback", post(rollback_vault))
        .route("/admin/enrollment/{code}", get(claim_enrollment))
        .layer(cors)
        .with_state(state)
}

// --- Dashboard ---

async fn dashboard(State(state): State<SharedState>) -> Html<String> {
    let device_list = devices::list_devices(&state.layout).unwrap_or_default();
    let vault_count = std::fs::read_dir(state.layout.base.join("vaults"))
        .map(|d| d.filter_map(|e| e.ok()).count())
        .unwrap_or(0);

    let device_rows: String = device_list
        .iter()
        .map(|d| {
            let status = if is_recent(d.last_seen) { "Online" } else { "Offline" };
            let dot = if is_recent(d.last_seen) { "🟢" } else { "⚪" };
            format!(
                "<tr><td>{} {}</td><td>{}</td><td>{}</td><td><a href='/admin/devices/{}'>details</a></td></tr>",
                dot, d.name, status, format_time(d.last_seen), d.fingerprint
            )
        })
        .collect();

    Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>ObsetyNC Admin</title>{CSS}</head>
<body>
<h1>ObsetyNC Server</h1>
<div class="stats">
    <span>Vaults: {vault_count}</span>
    <span>Devices: {}</span>
</div>
<h2>Devices</h2>
<table>
<tr><th>Name</th><th>Status</th><th>Last Seen</th><th></th></tr>
{device_rows}
</table>
<p><a href="/admin/devices/new" class="btn">+ Add Device</a></p>
<p><a href="/admin/vaults">View Vaults</a></p>
</body></html>"#,
        device_list.len(),
    ))
}

// --- Device List ---

async fn device_list(State(state): State<SharedState>) -> Html<String> {
    let devices = devices::list_devices(&state.layout).unwrap_or_default();
    let rows: String = devices
        .iter()
        .map(|d| {
            let revoked = devices::is_revoked(&state.layout, &d.fingerprint);
            let status = if revoked {
                "Revoked"
            } else if is_recent(d.last_seen) {
                "Online"
            } else {
                "Offline"
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td><a href='/admin/devices/{}'>details</a></td></tr>",
                d.name, status, format_time(d.last_seen), d.fingerprint
            )
        })
        .collect();

    Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>Devices - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / Devices</h1>
<table>
<tr><th>Name</th><th>Status</th><th>Last Seen</th><th></th></tr>
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

    Ok(Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>Device Created - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / Device Created</h1>
<div class="success">
    <p>Certificate generated for <strong>{}</strong></p>
    <p>Enrollment code: <code class="code">{}</code></p>
    <p>Expires in 10 minutes.</p>
    <p>Fingerprint: <code>{}</code></p>
</div>
<h3>To enroll:</h3>
<ol>
    <li>Open Obsidian → Settings → ObsetyNC</li>
    <li>Enter the server URL and enrollment code</li>
    <li>Or visit: <code>/admin/enrollment/{}</code></li>
</ol>
<p><a href="/admin/devices">Back to devices</a></p>
</body></html>"#,
        info.device_name, info.code, info.fingerprint, info.code
    )))
}

// --- Device Detail ---

async fn device_detail(
    State(state): State<SharedState>,
    Path(fingerprint): Path<String>,
) -> Result<Html<String>, ServerErrorHtml> {
    let device = devices::get_device(&state.layout, &fingerprint)
        .ok_or_else(|| ServerErrorHtml("device not found".into()))?;

    let revoked = devices::is_revoked(&state.layout, &fingerprint);
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
            fingerprint
        )
    };

    Ok(Html(format!(
        r#"<!DOCTYPE html>
<html><head><title>{} - ObsetyNC</title>{CSS}</head>
<body>
<h1><a href="/admin">ObsetyNC</a> / <a href="/admin/devices">Devices</a> / {}</h1>
<table>
<tr><td>Status</td><td>{status}</td></tr>
<tr><td>Fingerprint</td><td><code>{}</code></td></tr>
<tr><td>Enrolled</td><td>{}</td></tr>
<tr><td>Last Seen</td><td>{}</td></tr>
</table>
{revoke_btn}
<p><a href="/admin/devices">Back to devices</a></p>
</body></html>"#,
        device.name, device.name, fingerprint,
        format_time(device.enrolled_at),
        format_time(device.last_seen),
    )))
}

// --- Revoke ---

async fn revoke_device(
    State(state): State<SharedState>,
    Path(fingerprint): Path<String>,
) -> Result<Redirect, ServerErrorHtml> {
    devices::revoke_device(&state.layout, &fingerprint)
        .map_err(|e| ServerErrorHtml(format!("revoke failed: {}", e)))?;
    Ok(Redirect::to(&format!("/admin/devices/{}", fingerprint)))
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
            format!(
                "<tr><td><a href='/admin/vaults/{name}'>{name}</a></td><td>{status}</td></tr>"
            )
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
    let current_hex = current.map(|h| hash_to_hex(&h)).unwrap_or_else(|| "none".into());

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
                "<tr><td><code>{:.16}...</code>{marker}</td><td>{rollback}</td></tr>",
                hash
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

    state
        .vaults
        .set_current_root(&vault_id, &hash)
        .map_err(|e| ServerErrorHtml(format!("rollback failed: {}", e)))?;

    Ok(Redirect::to(&format!("/admin/vaults/{}", vault_id)))
}

// --- Enrollment Claim ---

async fn claim_enrollment(
    State(state): State<SharedState>,
    Path(code): Path<String>,
) -> impl IntoResponse {
    match enrollment::claim_enrollment(&state.layout, &code) {
        Ok(info) => {
            let bundle = serde_json::json!({
                "device_name": info.device_name,
                "fingerprint": info.fingerprint,
                "cert_pem": info.cert_pem,
                "key_pem": info.key_pem,
                "bearer_token": info.bearer_token,
            });
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                bundle.to_string(),
            ).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            serde_json::json!({ "error": e }).to_string(),
        ).into_response(),
    }
}

// --- Error type for HTML pages ---

struct ServerErrorHtml(String);

impl IntoResponse for ServerErrorHtml {
    fn into_response(self) -> axum::response::Response {
        Html(format!(
            r#"<!DOCTYPE html>
<html><head><title>Error - ObsetyNC</title>{CSS}</head>
<body>
<h1>Error</h1>
<p class="error">{}</p>
<p><a href="/admin">Back to dashboard</a></p>
</body></html>"#,
            self.0
        ))
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

const CSS: &str = r#"<style>
body { font-family: -apple-system, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; color: #333; }
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
</style>"#;
