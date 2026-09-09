// Included inside api.rs::integration_tests. These use the real protected
// router, sealed transport, root validation and temporary server persistence.
// Commit sequence numbers below are deliberately distinct from the transport
// anti-replay sequence automatically advanced by send_semantic.

#[tokio::test]
async fn root_outcome_endpoints_reject_non_post_semantic_methods_without_mutation() {
    let env = setup();
    let vault = "outcome-method-boundary";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let request = outcome_commit_body(&env, &root, 1, "");
    let cancel = outcome_cancel_body(&env, vault, &request);
    for (endpoint, body) in [
        ("root-commit", request),
        ("root-outcome", serde_json::json!({"protocol_version":1})),
        ("root-cancel", cancel),
    ] {
        for method in ["GET", "PUT", "DELETE"] {
            assert_eq!(
                send_semantic(
                    &env,
                    method,
                    &format!("/api/v1/{endpoint}/{vault}"),
                    &serde_json::to_vec(&body).unwrap()
                )
                .await
                .0,
                StatusCode::METHOD_NOT_ALLOWED.as_u16(),
                "{method} {endpoint}"
            );
            assert!(!env.state.layout.vault_current_path(vault).exists());
        }
    }
    let stream = outcome_success(
        &env,
        "root-outcome",
        vault,
        &serde_json::json!({"protocol_version":1}),
    )
    .await;
    assert_eq!(stream["last_sequence"], 0);
    assert!(stream["current_root_hash"].is_null());
}

#[tokio::test]
async fn root_cancel_is_scoped_to_authenticated_device_and_vault_across_transport_sessions() {
    let mut env = setup();
    let vault = "cancel-scope";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let request = outcome_commit_body(&env, &root, 1, "");
    let cancel = outcome_cancel_body(&env, vault, &request);
    let foreign_vault = outcome_success(&env, "root-cancel", "cancel-other-vault", &cancel).await;
    assert_eq!(foreign_vault["status"], "cancelled");
    let other_bearer = "a".repeat(64);
    env.state
        .devices
        .register(&"ab".repeat(32), "other-cancelling-device", &other_bearer)
        .unwrap();
    let original_bearer = std::mem::replace(&mut env.bearer, other_bearer);
    let other_cancelled = outcome_success(&env, "root-cancel", vault, &cancel).await;
    assert_eq!(other_cancelled["status"], "cancelled");
    let other_bearer = std::mem::replace(&mut env.bearer, original_bearer);
    let query = outcome_query_body(&other_cancelled);
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &query).await["status"],
        "unknown"
    );
    let accepted = outcome_success(&env, "root-commit", vault, &request).await;
    assert_eq!(accepted["status"], "accepted");
    let another_session = StaticSecret::from([97; 32]);
    let (status, bytes) = send_semantic_as(
        &env,
        &another_session,
        "POST",
        &format!("/api/v1/root-cancel/{vault}"),
        &serde_json::to_vec(&cancel).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK.as_u16());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        accepted
    );
    env.bearer = other_bearer;
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &query).await,
        other_cancelled
    );
    assert_eq!(
        env.state.vaults.try_get_current_root(vault).unwrap(),
        Some(root.hash())
    );
}

#[tokio::test]
async fn root_cancel_first_is_durable_terminal_and_late_commit_has_no_effect() {
    let env = setup();
    let vault = "cancel-first";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let request = outcome_commit_body(&env, &root, 1, "");
    let cancel = outcome_cancel_body(&env, vault, &request);
    let mut notifications = env.state.subscribe_roots(vault);
    let terminal = outcome_success(&env, "root-cancel", vault, &cancel).await;
    assert_eq!(terminal["status"], "cancelled");
    assert_eq!(terminal["result"], serde_json::json!({"cancelled":true}));
    assert_eq!(terminal["sequence"], 1);
    assert_eq!(terminal["request_hash"], cancel["request_hash"]);
    assert_eq!(env.state.vaults.try_get_current_root(vault).unwrap(), None);
    assert!(env.state.layout.vault_current_path(vault).is_file());
    assert!(env.state.vaults.get_root(vault, &root.hash()).is_none());
    for (endpoint, body) in [
        ("root-cancel", cancel),
        ("root-commit", request.clone()),
        ("root-outcome", outcome_query_body(&terminal)),
    ] {
        assert_eq!(
            outcome_success(&env, endpoint, vault, &body).await,
            terminal
        );
    }
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let stream = outcome_success(
        &env,
        "root-outcome",
        vault,
        &serde_json::json!({"protocol_version":1}),
    )
    .await;
    assert_eq!(stream["last_sequence"], 1);
    assert!(stream["current_root_hash"].is_null());
    assert_eq!(
        send_semantic(&env, "GET", &format!("/api/v1/root/{vault}"), &[])
            .await
            .0,
        StatusCode::NOT_FOUND.as_u16()
    );
    let next = outcome_commit_body(&env, &root, 2, "");
    let accepted = outcome_success(&env, "root-commit", vault, &next).await;
    outcome_assert_accepted(&accepted, &next, &hash_to_hex(&root.hash()));
    assert!(notifications.try_recv().is_ok());
    assert_eq!(
        outcome_post(&env, "root-commit", vault, &request).await.0,
        StatusCode::CONFLICT.as_u16()
    );
    assert_eq!(
        env.state.vaults.try_get_current_root(vault).unwrap(),
        Some(root.hash())
    );
}

#[tokio::test]
async fn root_cancel_after_acceptance_returns_acceptance_and_new_cancel_preserves_root_and_bypass()
{
    let env = setup();
    let vault = "cancel-after-root";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let candidate = outcome_root_fixture(&env, vault, 2).await;
    let request = outcome_commit_body(&env, &root, 1, "");
    let accepted = outcome_success(&env, "root-commit", vault, &request).await;
    let mut notifications = env.state.subscribe_roots(vault);
    let mut first_cancel = outcome_cancel_body(&env, vault, &request);
    first_cancel["server_incarnation"] =
        serde_json::json!(if env.state.root_incarnation == "f".repeat(64) {
            "e".repeat(64)
        } else {
            "f".repeat(64)
        });
    assert_eq!(
        outcome_success(&env, "root-cancel", vault, &first_cancel).await,
        accepted
    );
    let next = outcome_commit_body(&env, &candidate, 2, &hash_to_hex(&root.hash()));
    let cancel = outcome_cancel_body(&env, vault, &next);
    let device = env.state.devices.list().pop().unwrap();
    devices::grant_deletion_bypass(&env.state.layout, &device.device_id, 60).unwrap();
    let terminal = outcome_success(&env, "root-cancel", vault, &cancel).await;
    assert_eq!(terminal["status"], "cancelled");
    assert_eq!(
        outcome_success(&env, "root-commit", vault, &next).await,
        terminal
    );
    assert_eq!(
        env.state.vaults.try_get_current_root(vault).unwrap(),
        Some(root.hash())
    );
    assert!(env
        .state
        .vaults
        .get_root(vault, &candidate.hash())
        .is_none());
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(devices::deletion_bypass_remaining_ms(&env.state.layout, &device.device_id).is_some());
    assert!(devices::consume_deletion_bypass(
        &env.state.layout,
        &device.device_id
    ));
}

#[tokio::test]
async fn root_cancel_concurrent_commit_and_cancel_observe_one_terminal_winner() {
    for cancel_first in [false, true] {
        let env = setup();
        let vault = "cancel-concurrent";
        let root = outcome_root_fixture(&env, vault, 1).await;
        let request = outcome_commit_body(&env, &root, 1, "");
        let cancel = outcome_cancel_body(&env, vault, &request);
        let mut notifications = env.state.subscribe_roots(vault);
        let (a, b) = if cancel_first {
            tokio::join!(
                outcome_post(&env, "root-cancel", vault, &cancel),
                outcome_post(&env, "root-commit", vault, &request)
            )
        } else {
            tokio::join!(
                outcome_post(&env, "root-commit", vault, &request),
                outcome_post(&env, "root-cancel", vault, &cancel)
            )
        };
        assert_eq!(
            (a.0, b.0),
            (StatusCode::OK.as_u16(), StatusCode::OK.as_u16())
        );
        let a: serde_json::Value = serde_json::from_slice(&a.1).unwrap();
        let b: serde_json::Value = serde_json::from_slice(&b.1).unwrap();
        assert_eq!(
            a, b,
            "Commit and cancellation must observe the same terminal cut"
        );
        match a["status"].as_str().unwrap() {
            "accepted" => {
                assert_eq!(
                    env.state.vaults.try_get_current_root(vault).unwrap(),
                    Some(root.hash())
                );
                assert!(env.state.vaults.get_root(vault, &root.hash()).is_some());
                assert!(notifications.try_recv().is_ok());
            }
            "cancelled" => {
                assert_eq!(env.state.vaults.try_get_current_root(vault).unwrap(), None);
                assert!(env.state.vaults.get_root(vault, &root.hash()).is_none());
            }
            other => panic!("unexpected terminal status {other}"),
        }
        assert!(matches!(
            notifications.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(
            outcome_success(&env, "root-outcome", vault, &outcome_query_body(&a)).await,
            a
        );
        assert_eq!(
            outcome_success(&env, "root-cancel", vault, &cancel).await,
            a
        );
        assert_eq!(
            outcome_success(&env, "root-commit", vault, &request).await,
            a
        );
    }
}

#[tokio::test]
async fn root_cancel_restart_replays_known_outcome_and_current_epoch_cancels_original_old_digest() {
    let mut env = setup();
    let vault = "cancel-restart";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let first = outcome_commit_body(&env, &root, 1, "");
    let cancel = outcome_cancel_body(&env, vault, &first);
    let next = outcome_commit_body(&env, &root, 2, "");
    let mut next_cancel = outcome_cancel_body(&env, vault, &next);
    let terminal = outcome_success(&env, "root-cancel", vault, &cancel).await;
    let old_incarnation = env.state.root_incarnation.clone();
    env.state = Arc::new(AppState::new(ServerConfig::new(
        env._tmp.path().to_path_buf(),
    )));
    env.sequence.store(100_000, Ordering::Relaxed);
    assert_ne!(env.state.root_incarnation, old_incarnation);
    for (endpoint, body) in [
        ("root-cancel", cancel),
        ("root-commit", first),
        ("root-outcome", outcome_query_body(&terminal)),
    ] {
        let replay = outcome_success(&env, endpoint, vault, &body).await;
        assert_eq!(replay["server_incarnation"], env.state.root_incarnation);
        for field in [
            "status",
            "sequence",
            "mutation_id",
            "request_hash",
            "result",
        ] {
            assert_eq!(replay[field], terminal[field]);
        }
    }
    assert_eq!(
        outcome_post(&env, "root-cancel", vault, &next_cancel)
            .await
            .0,
        StatusCode::CONFLICT.as_u16()
    );
    let original_digest = next_cancel["request_hash"].clone();
    next_cancel["server_incarnation"] = serde_json::json!(env.state.root_incarnation);
    let cancelled = outcome_success(&env, "root-cancel", vault, &next_cancel).await;
    assert_eq!(cancelled["request_hash"], original_digest);
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(
        outcome_success(&env, "root-commit", vault, &next).await,
        cancelled
    );
    assert_eq!(env.state.vaults.try_get_current_root(vault).unwrap(), None);
}

#[tokio::test]
async fn root_cancel_strict_schema_caps_and_sequence_identity_refusals_leave_stream_unchanged() {
    let env = setup();
    let vault = "cancel-strict";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let request = outcome_commit_body(&env, &root, 1, "");
    let valid = outcome_cancel_body(&env, vault, &request);
    for method in ["GET", "PUT", "DELETE"] {
        assert_eq!(
            send_semantic(
                &env,
                method,
                &format!("/api/v1/root-cancel/{vault}"),
                &serde_json::to_vec(&valid).unwrap()
            )
            .await
            .0,
            StatusCode::METHOD_NOT_ALLOWED.as_u16()
        );
    }
    for (field, value) in [
        ("protocol_version", serde_json::json!(2)),
        ("sequence", serde_json::json!(0)),
        (
            "sequence",
            serde_json::json!(root_outcome::MAX_SEQUENCE + 1),
        ),
        ("sequence", serde_json::json!(1.5)),
        ("request_hash", serde_json::Value::Null),
        ("request_hash", serde_json::json!("F".repeat(64))),
        ("mutation_id", serde_json::json!("a".repeat(31))),
        ("server_incarnation", serde_json::json!("a".repeat(63))),
        ("unknown", serde_json::json!(true)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert_eq!(
            outcome_post(&env, "root-cancel", vault, &invalid).await.0,
            StatusCode::BAD_REQUEST.as_u16(),
            "invalid {field}"
        );
    }
    let duplicate = serde_json::to_string(&valid)
        .unwrap()
        .replacen('{', "{\"sequence\":1,", 1);
    let path = format!("/api/v1/root-cancel/{vault}");
    assert_eq!(
        send_semantic(&env, "POST", &path, duplicate.as_bytes())
            .await
            .0,
        StatusCode::BAD_REQUEST.as_u16()
    );
    assert!(!env.state.layout.vault_current_path(vault).exists());
    let mut padded = serde_json::to_vec(&valid).unwrap();
    padded.resize(root_outcome::MAX_CANCEL_REQUEST_BYTES, b' ');
    let (status, bytes) = send_semantic(&env, "POST", &path, &padded).await;
    assert_eq!(status, StatusCode::OK.as_u16());
    let terminal: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(terminal["status"], "cancelled");
    let before = std::fs::read(env.state.layout.vault_current_path(vault)).unwrap();
    padded.push(b' ');
    assert_eq!(
        send_semantic(&env, "POST", &path, &padded).await.0,
        StatusCode::PAYLOAD_TOO_LARGE.as_u16()
    );
    for (field, value) in [
        ("mutation_id", serde_json::json!("e".repeat(32))),
        ("request_hash", serde_json::json!("e".repeat(64))),
        ("sequence", serde_json::json!(3)),
    ] {
        let mut conflicting = valid.clone();
        conflicting[field] = value;
        assert_eq!(
            outcome_post(&env, "root-cancel", vault, &conflicting)
                .await
                .0,
            StatusCode::CONFLICT.as_u16()
        );
    }
    assert_eq!(
        std::fs::read(env.state.layout.vault_current_path(vault)).unwrap(),
        before
    );
    let next = outcome_commit_body(&env, &root, 2, "");
    outcome_success(
        &env,
        "root-cancel",
        vault,
        &outcome_cancel_body(&env, vault, &next),
    )
    .await;
    assert_eq!(
        outcome_post(&env, "root-cancel", vault, &valid).await.0,
        StatusCode::CONFLICT.as_u16()
    );
    assert_eq!(env.state.vaults.try_get_current_root(vault).unwrap(), None);
}

#[tokio::test]
async fn root_cancel_corrupt_head_and_retention_overflow_fail_closed_without_bypass_or_notification(
) {
    for corrupt in [false, true] {
        let env = setup();
        let vault = "cancel-storage-refusal";
        let root = outcome_root_fixture(&env, vault, 1).await;
        let request = outcome_commit_body(&env, &root, 1, "");
        let cancel = outcome_cancel_body(&env, vault, &request);
        env.state.layout.ensure_vault(vault).unwrap();
        let before = if corrupt {
            b"corrupt authoritative head".to_vec()
        } else {
            let head = crate::root_head::RootHead {
                root_hash: None,
                receipts: (0..crate::root_head::MAX_ROOT_RECEIPT_DEVICES)
                    .map(|index| {
                        (
                            format!("retained-{index}"),
                            RootReceipt {
                                sequence: 1,
                                mutation_id: format!("{index:032x}"),
                                request_hash: format!("{index:064x}"),
                                result: serde_json::json!({"cancelled":true}),
                            },
                        )
                    })
                    .collect(),
            };
            crate::root_head::encode(vault, &head).unwrap()
        };
        let path = env.state.layout.vault_current_path(vault);
        std::fs::write(&path, &before).unwrap();
        let device = env.state.devices.list().pop().unwrap();
        devices::grant_deletion_bypass(&env.state.layout, &device.device_id, 60).unwrap();
        let mut notifications = env.state.subscribe_roots(vault);
        let (status, _) = outcome_post(&env, "root-cancel", vault, &cancel).await;
        if corrupt {
            assert!(status >= 500);
        } else {
            assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE.as_u16());
        }
        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert!(devices::consume_deletion_bypass(
            &env.state.layout,
            &device.device_id
        ));
        assert!(matches!(
            notifications.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }
}

async fn outcome_root_fixture(
    env: &Env,
    vault: &str,
    revision: u8,
) -> sync_core::versioned_root::VersionedRoot {
    let content = vec![revision; 32];
    let content_hash = hash_bytes(&content);
    let content_path = format!("/api/v1/content/{}", hash_to_hex(&content_hash));
    assert_eq!(
        send_semantic(env, "PUT", &content_path, &content).await.0,
        StatusCode::NO_CONTENT.as_u16(),
    );
    let entry = sync_core::chunk::FileEntry::new(
        "synthetic-note.md".into(),
        content_hash,
        1_900_000_000_000 + u64::from(revision),
        content.len() as u64,
    );
    sync_core::versioned_root::build_root(
        &env.state.storage_writer,
        vec![entry],
        sync_core::versioned_root::TREE_V1,
        vault,
        "untrusted-client-history-author",
    )
    .await
    .unwrap()
}

fn outcome_base64(bytes: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, bytes)
}

fn outcome_commit_body(
    env: &Env,
    root: &sync_core::versioned_root::VersionedRoot,
    sequence: u64,
    parent_root: &str,
) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": 1,
        "server_incarnation": env.state.root_incarnation,
        "sequence": sequence,
        "mutation_id": format!("{sequence:032x}"),
        "parent_root": parent_root,
        "root": outcome_base64(&root.serialize().unwrap()),
    })
}

async fn outcome_post(
    env: &Env,
    endpoint: &str,
    vault: &str,
    body: &serde_json::Value,
) -> (u16, Vec<u8>) {
    send_semantic(
        env,
        "POST",
        &format!("/api/v1/{endpoint}/{vault}"),
        &serde_json::to_vec(body).unwrap(),
    )
    .await
}

async fn outcome_success(
    env: &Env,
    endpoint: &str,
    vault: &str,
    body: &serde_json::Value,
) -> serde_json::Value {
    let (status, bytes) = outcome_post(env, endpoint, vault, body).await;
    assert_eq!(
        status,
        StatusCode::OK.as_u16(),
        "{endpoint} failed: {}",
        String::from_utf8_lossy(&bytes),
    );
    serde_json::from_slice(&bytes).unwrap()
}

fn outcome_query_body(receipt: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "protocol_version": 1,
        "sequence": receipt["sequence"],
        "mutation_id": receipt["mutation_id"],
        "request_hash": receipt["request_hash"],
    })
}

fn outcome_cancel_body(env: &Env, vault: &str, request: &serde_json::Value) -> serde_json::Value {
    let device = env.state.devices.list().pop().unwrap();
    let (identity, _) = root_outcome::decode_commit(
        &serde_json::to_vec(request).unwrap(),
        vault,
        &device.device_id,
    )
    .unwrap();
    serde_json::json!({"protocol_version":1,"server_incarnation":env.state.root_incarnation,
        "sequence":identity.sequence,"mutation_id":identity.mutation_id,"request_hash":identity.request_hash})
}

fn outcome_assert_hex(value: &serde_json::Value, bytes: usize) {
    let text = value.as_str().expect("expected hex string");
    assert_eq!(text.len(), bytes * 2);
    assert!(text
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
}

fn outcome_assert_accepted(
    receipt: &serde_json::Value,
    request: &serde_json::Value,
    root_hash: &str,
) {
    assert_eq!(receipt["protocol_version"], 1);
    assert_eq!(receipt["status"], "accepted");
    assert_eq!(receipt["sequence"], request["sequence"]);
    assert_eq!(receipt["mutation_id"], request["mutation_id"]);
    outcome_assert_hex(&receipt["server_incarnation"], 32);
    outcome_assert_hex(&receipt["request_hash"], 32);
    assert_eq!(receipt["result"]["accepted"], true);
    assert_eq!(receipt["result"]["root_hash"], root_hash);
}

#[tokio::test]
async fn root_outcome_routes_require_sealed_authenticated_requests() {
    let mut env = setup();
    let vault = "outcome-protected";
    for endpoint in ["root-commit", "root-outcome", "root-cancel"] {
        let path = format!("/api/v1/{endpoint}/{vault}");
        let (status, body) =
            dispatch_wire(&env, "POST", &path, br#"{"protocol_version":1}"#.to_vec()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, vec![0; 256], "Unsealed request receives only a decoy");
    }
    assert!(env.state.vaults.get_current_root(vault).is_none());

    let root = outcome_root_fixture(&env, vault, 1).await;
    let request = outcome_commit_body(&env, &root, 1, "");
    let cancellation = outcome_cancel_body(&env, vault, &request);
    let original_bearer = std::mem::replace(&mut env.bearer, "f".repeat(64));
    for (endpoint, body) in [
        ("root-commit", request.clone()),
        ("root-outcome", serde_json::json!({ "protocol_version": 1 })),
        ("root-cancel", cancellation.clone()),
    ] {
        assert_eq!(
            outcome_post(&env, endpoint, vault, &body).await.0,
            StatusCode::UNAUTHORIZED.as_u16(),
        );
    }
    env.bearer = original_bearer;
    let stream = outcome_success(
        &env,
        "root-outcome",
        vault,
        &serde_json::json!({ "protocol_version": 1 }),
    )
    .await;
    assert_eq!(stream["status"], "stream");
    assert_eq!(stream["last_sequence"], 0);
    assert_eq!(stream["server_incarnation"], env.state.root_incarnation);

    let device = env.state.devices.list().pop().unwrap();
    env.state.devices.revoke(&device.device_id).unwrap();
    for (endpoint, body) in [
        ("root-commit", request),
        ("root-outcome", serde_json::json!({ "protocol_version": 1 })),
        ("root-cancel", cancellation),
    ] {
        assert_eq!(
            outcome_post(&env, endpoint, vault, &body).await.0,
            StatusCode::FORBIDDEN.as_u16(),
        );
    }
    assert!(env.state.vaults.get_current_root(vault).is_none());
}

#[tokio::test]
async fn root_outcome_exact_replay_is_stable_and_same_semantic_root_changed_bytes_conflict() {
    let env = setup();
    let vault = "outcome-replay";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let request = outcome_commit_body(&env, &root, 1, "");
    let mut notifications = env.state.subscribe_roots(vault);
    let accepted = outcome_success(&env, "root-commit", vault, &request).await;
    outcome_assert_accepted(&accepted, &request, &hash_to_hex(&root.hash()));
    assert!(notifications.try_recv().is_ok());

    assert_eq!(
        outcome_success(&env, "root-commit", vault, &request).await,
        accepted,
        "Application retry uses a fresh transport envelope but returns the same receipt",
    );
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &outcome_query_body(&accepted)).await,
        accepted,
    );
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));

    let mut metadata_changed = root.clone();
    metadata_changed.set_history_metadata(42, Some([7; 32]), "different-untrusted-author");
    assert_eq!(metadata_changed.hash(), root.hash());
    assert_ne!(
        metadata_changed.serialize().unwrap(),
        root.serialize().unwrap()
    );
    let mut changed = request.clone();
    changed["root"] = serde_json::json!(outcome_base64(&metadata_changed.serialize().unwrap()));
    assert_eq!(
        outcome_post(&env, "root-commit", vault, &changed).await.0,
        StatusCode::CONFLICT.as_u16(),
        "Receipt identity must bind exact serialized root bytes, not only semantic root hash",
    );
    for (field, value) in [
        ("parent_root", "a".repeat(64)),
        ("mutation_id", "b".repeat(32)),
    ] {
        let mut changed = request.clone();
        changed[field] = serde_json::json!(value);
        assert_eq!(
            outcome_post(&env, "root-commit", vault, &changed).await.0,
            StatusCode::CONFLICT.as_u16(),
            "Changing {field} cannot alias an accepted sequence",
        );
    }
    assert_eq!(env.state.vaults.get_current_root(vault), Some(root.hash()));
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &outcome_query_body(&accepted)).await,
        accepted,
    );
}

#[tokio::test]
async fn root_outcome_sequence_progression_expiration_and_query_mismatch_are_distinct() {
    let env = setup();
    let vault = "outcome-sequences";
    let root_a = outcome_root_fixture(&env, vault, 1).await;
    let root_b = outcome_root_fixture(&env, vault, 2).await;
    let first_request = outcome_commit_body(&env, &root_a, 1, "");
    let first = outcome_success(&env, "root-commit", vault, &first_request).await;
    let skipped = outcome_commit_body(&env, &root_b, 3, &hash_to_hex(&root_a.hash()));
    assert_eq!(
        outcome_post(&env, "root-commit", vault, &skipped).await.0,
        StatusCode::CONFLICT.as_u16(),
        "A sequence gap cannot consume the next stream position",
    );
    let second_request = outcome_commit_body(&env, &root_b, 2, &hash_to_hex(&root_a.hash()));
    let second = outcome_success(&env, "root-commit", vault, &second_request).await;
    outcome_assert_accepted(&second, &second_request, &hash_to_hex(&root_b.hash()));
    assert_ne!(first["request_hash"], second["request_hash"]);
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &outcome_query_body(&first)).await["status"],
        "expired",
    );
    assert_eq!(
        outcome_post(&env, "root-commit", vault, &first_request)
            .await
            .0,
        StatusCode::CONFLICT.as_u16(),
        "An expired commit is not republished",
    );
    let mut future = outcome_query_body(&second);
    future["sequence"] = serde_json::json!(3);
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &future).await["status"],
        "unknown"
    );
    for (field, value) in [
        ("mutation_id", "d".repeat(32)),
        ("request_hash", "e".repeat(64)),
    ] {
        let mut mismatch = outcome_query_body(&second);
        mismatch[field] = serde_json::json!(value);
        assert_eq!(
            outcome_post(&env, "root-outcome", vault, &mismatch).await.0,
            StatusCode::CONFLICT.as_u16(),
            "Same-sequence query with different {field} is an identity conflict",
        );
    }
    let stream = outcome_success(
        &env,
        "root-outcome",
        vault,
        &serde_json::json!({ "protocol_version": 1 }),
    )
    .await;
    assert_eq!(stream["status"], "stream");
    assert_eq!(stream["last_sequence"], 2);
    assert_eq!(stream["current_root_hash"], hash_to_hex(&root_b.hash()));
    assert_eq!(
        outcome_success(&env, "root-commit", vault, &second_request).await,
        second,
    );
}

#[tokio::test]
async fn root_outcome_accepted_receipt_survives_legacy_and_admin_current_root_changes() {
    for administrative in [false, true] {
        let env = setup();
        let vault = if administrative {
            "outcome-admin"
        } else {
            "outcome-legacy"
        };
        let root_a = outcome_root_fixture(&env, vault, 1).await;
        let mut root_b = outcome_root_fixture(&env, vault, 2).await;
        let request = outcome_commit_body(&env, &root_a, 1, "");
        let accepted = outcome_success(&env, "root-commit", vault, &request).await;
        if administrative {
            // Same persisted current-root mutation as administrative tree
            // activation. A v1 retry must replay its receipt before today's
            // v2/session/parent validation, not attempt to publish old state.
            root_b = bridge::run_project_root_version(
                env.state.storage_writer.clone(),
                root_b,
                sync_core::versioned_root::TREE_V2,
                "admin-outcome-fixture".into(),
            )
            .await
            .unwrap();
            root_b.set_history_metadata(
                1_900_000_000_100,
                Some(root_a.hash()),
                "admin-outcome-fixture",
            );
            env.state
                .vaults
                .store_root(vault, &root_b.hash(), &root_b.serialize().unwrap())
                .unwrap();
            env.state
                .vaults
                .set_current_root(vault, &root_b.hash())
                .unwrap();
        } else {
            let mut body = hash_to_hex(&root_a.hash()).into_bytes();
            body.extend_from_slice(&root_b.serialize().unwrap());
            assert_eq!(
                send_semantic(&env, "PUT", &format!("/api/v1/root/{vault}"), &body)
                    .await
                    .0,
                StatusCode::OK.as_u16(),
            );
        }
        assert_ne!(root_a.hash(), root_b.hash());
        assert_eq!(
            outcome_success(&env, "root-outcome", vault, &outcome_query_body(&accepted)).await,
            accepted,
            "Receipt is immutable historical evidence, not a fresh read of current root",
        );
        assert_eq!(
            outcome_success(&env, "root-commit", vault, &request).await,
            accepted,
            "Exact replay must not re-run old root validation or change current root",
        );
        assert_eq!(
            env.state.vaults.get_current_root(vault),
            Some(root_b.hash())
        );
        let stream = outcome_success(
            &env,
            "root-outcome",
            vault,
            &serde_json::json!({ "protocol_version": 1 }),
        )
        .await;
        assert_eq!(stream["last_sequence"], 1);
        assert_eq!(stream["current_root_hash"], hash_to_hex(&root_b.hash()));
    }
}

#[tokio::test]
async fn root_outcome_scope_is_authenticated_device_and_vault_not_transport_session() {
    let mut env = setup();
    let vault = "outcome-device";
    let root_a = outcome_root_fixture(&env, vault, 1).await;
    let root_b = outcome_root_fixture(&env, vault, 2).await;
    let request_a = outcome_commit_body(&env, &root_a, 1, "");
    let first = outcome_success(&env, "root-commit", vault, &request_a).await;
    let query = outcome_query_body(&first);
    let another_session = StaticSecret::from([91; 32]);
    let (status, bytes) = send_semantic_as(
        &env,
        &another_session,
        "POST",
        &format!("/api/v1/root-outcome/{vault}"),
        &serde_json::to_vec(&query).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK.as_u16());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        first
    );

    let foreign_vault = outcome_success(&env, "root-outcome", "outcome-other-vault", &query).await;
    assert_eq!(foreign_vault["status"], "unknown");
    assert!(foreign_vault.get("result").is_none());
    assert_eq!(
        outcome_post(&env, "root-commit", "outcome-other-vault", &request_a)
            .await
            .0,
        StatusCode::BAD_REQUEST.as_u16(),
        "A serialized root for another vault cannot be accepted on this path",
    );
    let other_bearer = "a".repeat(64);
    env.state
        .devices
        .register(&"ab".repeat(32), "other-enrolled-device", &other_bearer)
        .unwrap();
    let original_bearer = std::mem::replace(&mut env.bearer, other_bearer);
    let absent = outcome_success(&env, "root-outcome", vault, &query).await;
    assert_eq!(absent["status"], "unknown");
    assert!(
        absent.get("result").is_none(),
        "Foreign stream cannot disclose another device's receipt"
    );
    let stream = outcome_success(
        &env,
        "root-outcome",
        vault,
        &serde_json::json!({ "protocol_version": 1 }),
    )
    .await;
    assert_eq!(stream["last_sequence"], 0);
    assert_eq!(stream["current_root_hash"], hash_to_hex(&root_a.hash()));
    let request_b = outcome_commit_body(&env, &root_b, 1, &hash_to_hex(&root_a.hash()));
    let foreign = outcome_success(&env, "root-commit", vault, &request_b).await;
    outcome_assert_accepted(&foreign, &request_b, &hash_to_hex(&root_b.hash()));
    env.bearer = original_bearer;
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &query).await,
        first
    );
    assert_eq!(
        env.state.vaults.get_current_root(vault),
        Some(root_b.hash())
    );
}

#[tokio::test]
async fn root_outcome_restart_replays_prior_incarnation_but_rejects_absent_stale_commits() {
    let mut env = setup();
    let vault = "outcome-restart";
    let root_a = outcome_root_fixture(&env, vault, 1).await;
    let root_b = outcome_root_fixture(&env, vault, 2).await;
    let request_a = outcome_commit_body(&env, &root_a, 1, "");
    let accepted = outcome_success(&env, "root-commit", vault, &request_a).await;
    let stale_next = outcome_commit_body(&env, &root_b, 2, &hash_to_hex(&root_a.hash()));
    let old_incarnation = env.state.root_incarnation.clone();

    // Real cold server state on the same isolated disk fixture and pinned box
    // key; transport reservations burn their old range independently.
    env.state = Arc::new(AppState::new(ServerConfig::new(
        env._tmp.path().to_path_buf(),
    )));
    env.sequence.store(100_000, Ordering::Relaxed);
    assert_ne!(env.state.root_incarnation, old_incarnation);
    let queried =
        outcome_success(&env, "root-outcome", vault, &outcome_query_body(&accepted)).await;
    let replay = outcome_success(&env, "root-commit", vault, &request_a).await;
    for returned in [&queried, &replay] {
        assert_eq!(returned["server_incarnation"], env.state.root_incarnation);
        for field in [
            "protocol_version",
            "status",
            "sequence",
            "mutation_id",
            "request_hash",
            "result",
        ] {
            assert_eq!(
                returned[field], accepted[field],
                "Restart changed receipt field {field}"
            );
        }
    }
    assert_eq!(
        outcome_post(&env, "root-commit", vault, &stale_next)
            .await
            .0,
        StatusCode::CONFLICT.as_u16(),
        "Old incarnation may query/replay known evidence but cannot create an absent acceptance",
    );
    assert_eq!(
        env.state.vaults.get_current_root(vault),
        Some(root_a.hash())
    );
    let mut fresh = stale_next;
    fresh["server_incarnation"] = serde_json::json!(env.state.root_incarnation);
    let second = outcome_success(&env, "root-commit", vault, &fresh).await;
    outcome_assert_accepted(&second, &fresh, &hash_to_hex(&root_b.hash()));
    assert_eq!(
        env.state.vaults.get_current_root(vault),
        Some(root_b.hash())
    );
}

#[tokio::test]
async fn root_outcome_malformed_fields_are_rejected_without_consuming_sequence() {
    let env = setup();
    let vault = "outcome-malformed";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let valid = outcome_commit_body(&env, &root, 1, "");
    let invalid_fields = [
        ("protocol_version", serde_json::json!(2)),
        ("sequence", serde_json::json!(0)),
        ("sequence", serde_json::json!(-1)),
        ("sequence", serde_json::json!(1.5)),
        ("sequence", serde_json::json!(9_007_199_254_740_992u64)),
        ("sequence", serde_json::json!("1")),
        ("mutation_id", serde_json::json!("a".repeat(31))),
        ("mutation_id", serde_json::json!("A".repeat(32))),
        ("mutation_id", serde_json::json!("g".repeat(32))),
        ("parent_root", serde_json::json!("a".repeat(63))),
        ("parent_root", serde_json::json!("A".repeat(64))),
        ("parent_root", serde_json::json!(null)),
        ("server_incarnation", serde_json::json!("a".repeat(63))),
        ("server_incarnation", serde_json::json!("A".repeat(64))),
        ("unexpected", serde_json::json!(true)),
    ];
    for (field, value) in invalid_fields {
        let mut request = valid.clone();
        request[field] = value;
        assert_eq!(
            outcome_post(&env, "root-commit", vault, &request).await.0,
            StatusCode::BAD_REQUEST.as_u16(),
            "Malformed field {field} must fail before publication",
        );
    }
    for encoding in ["", "a", "!!!!", "AA==\n", "AB==", "AA", "-_=="] {
        let mut request = valid.clone();
        request["root"] = serde_json::json!(encoding);
        assert_eq!(
            outcome_post(&env, "root-commit", vault, &request).await.0,
            StatusCode::BAD_REQUEST.as_u16(),
            "Malformed or noncanonical root encoding was accepted",
        );
    }
    for missing in [
        "protocol_version",
        "server_incarnation",
        "sequence",
        "mutation_id",
        "parent_root",
        "root",
    ] {
        let mut request = valid.clone();
        request.as_object_mut().unwrap().remove(missing);
        assert_eq!(
            outcome_post(&env, "root-commit", vault, &request).await.0,
            StatusCode::BAD_REQUEST.as_u16()
        );
    }
    assert!(env.state.vaults.get_current_root(vault).is_none());
    let accepted = outcome_success(&env, "root-commit", vault, &valid).await;
    outcome_assert_accepted(&accepted, &valid, &hash_to_hex(&root.hash()));
}

#[tokio::test]
async fn root_outcome_query_requires_complete_identity_group_and_strict_schema() {
    let env = setup();
    let vault = "outcome-query-validation";
    let fields = [
        ("sequence", serde_json::json!(1)),
        ("mutation_id", serde_json::json!("a".repeat(32))),
        ("request_hash", serde_json::json!("b".repeat(64))),
    ];
    for mask in 1..7 {
        let mut query = serde_json::json!({ "protocol_version": 1 });
        for (i, (field, value)) in fields.iter().enumerate() {
            if mask & (1 << i) != 0 {
                query[*field] = value.clone();
            }
        }
        assert_eq!(
            outcome_post(&env, "root-outcome", vault, &query).await.0,
            StatusCode::BAD_REQUEST.as_u16(),
            "Partial identity group {mask} must not silently become stream/unknown",
        );
    }
    let complete = serde_json::json!({ "protocol_version": 1, "sequence": 1,
        "mutation_id": "a".repeat(32), "request_hash": "b".repeat(64) });
    for (field, value) in [
        ("protocol_version", serde_json::json!(0)),
        ("sequence", serde_json::json!(0)),
        ("sequence", serde_json::json!(9_007_199_254_740_992u64)),
        ("mutation_id", serde_json::json!("A".repeat(32))),
        ("request_hash", serde_json::json!("B".repeat(64))),
        ("request_hash", serde_json::json!("b".repeat(63))),
        (
            "server_incarnation",
            serde_json::json!(env.state.root_incarnation),
        ),
    ] {
        let mut query = complete.clone();
        query[field] = value;
        assert_eq!(
            outcome_post(&env, "root-outcome", vault, &query).await.0,
            StatusCode::BAD_REQUEST.as_u16()
        );
    }
    let unknown = outcome_success(&env, "root-outcome", vault, &complete).await;
    assert_eq!(unknown["status"], "unknown");
    assert!(unknown.get("result").is_none());
}

#[tokio::test]
async fn root_outcome_plaintext_and_decoded_root_caps_are_enforced_at_exact_boundaries() {
    const COMMIT_LIMIT: usize = 704 * 1024;
    const ROOT_LIMIT: usize = 512 * 1024;
    const QUERY_LIMIT: usize = 4 * 1024;
    let env = setup();
    let vault = "outcome-caps";
    let root = outcome_root_fixture(&env, vault, 1).await;
    let valid = outcome_commit_body(&env, &root, 1, "");
    let path = format!("/api/v1/root-commit/{vault}");

    let mut oversized_request = serde_json::to_vec(&valid).unwrap();
    oversized_request.resize(COMMIT_LIMIT + 1, b' ');
    assert_eq!(
        send_semantic(&env, "POST", &path, &oversized_request)
            .await
            .0,
        StatusCode::PAYLOAD_TOO_LARGE.as_u16()
    );
    let at_limit = outcome_base64(&vec![0; ROOT_LIMIT]);
    let beyond_limit = outcome_base64(&vec![0; ROOT_LIMIT + 1]);
    assert_eq!(
        at_limit.len(),
        beyond_limit.len(),
        "Fixture must require checking decoded bytes, not just encoded length"
    );
    let mut oversized_root = valid.clone();
    oversized_root["root"] = serde_json::json!(beyond_limit);
    assert!(serde_json::to_vec(&oversized_root).unwrap().len() < COMMIT_LIMIT);
    assert_eq!(
        outcome_post(&env, "root-commit", vault, &oversized_root)
            .await
            .0,
        StatusCode::PAYLOAD_TOO_LARGE.as_u16()
    );
    oversized_root["root"] = serde_json::json!(at_limit);
    assert_eq!(
        outcome_post(&env, "root-commit", vault, &oversized_root).await.0,
        StatusCode::BAD_REQUEST.as_u16(),
        "Exactly the decoded cap reaches structural validation rather than being rejected as oversized",
    );
    assert!(env.state.vaults.get_current_root(vault).is_none());
    let mut boundary = serde_json::to_vec(&valid).unwrap();
    boundary.resize(COMMIT_LIMIT, b' ');
    let (status, body) = send_semantic(&env, "POST", &path, &boundary).await;
    assert_eq!(status, StatusCode::OK.as_u16());
    outcome_assert_accepted(
        &serde_json::from_slice(&body).unwrap(),
        &valid,
        &hash_to_hex(&root.hash()),
    );

    let query_path = format!("/api/v1/root-outcome/{vault}");
    let mut query = br#"{"protocol_version":1}"#.to_vec();
    query.resize(QUERY_LIMIT, b' ');
    assert_eq!(
        send_semantic(&env, "POST", &query_path, &query).await.0,
        StatusCode::OK.as_u16()
    );
    query.push(b' ');
    assert_eq!(
        send_semantic(&env, "POST", &query_path, &query).await.0,
        StatusCode::PAYLOAD_TOO_LARGE.as_u16()
    );
    assert_eq!(env.state.vaults.get_current_root(vault), Some(root.hash()));
}

#[tokio::test]
async fn root_outcome_concurrent_same_sequence_publishes_once_for_equal_or_conflicting_bodies() {
    for equal_bodies in [true, false] {
        let env = setup();
        let vault = if equal_bodies {
            "outcome-concurrent-equal"
        } else {
            "outcome-concurrent-conflict"
        };
        let root_a = outcome_root_fixture(&env, vault, 1).await;
        let root_b = outcome_root_fixture(&env, vault, 2).await;
        let request_a = outcome_commit_body(&env, &root_a, 1, "");
        let request_b = if equal_bodies {
            request_a.clone()
        } else {
            outcome_commit_body(&env, &root_b, 1, "")
        };
        let mut notifications = env.state.subscribe_roots(vault);
        let (a, b) = tokio::join!(
            outcome_post(&env, "root-commit", vault, &request_a),
            outcome_post(&env, "root-commit", vault, &request_b),
        );
        let accepted = if equal_bodies {
            assert_eq!(a.0, StatusCode::OK.as_u16());
            assert_eq!(b.0, StatusCode::OK.as_u16());
            let a: serde_json::Value = serde_json::from_slice(&a.1).unwrap();
            let b: serde_json::Value = serde_json::from_slice(&b.1).unwrap();
            assert_eq!(
                a, b,
                "Both concurrent equal requests observe one accepted outcome"
            );
            assert_eq!(
                env.state.vaults.get_current_root(vault),
                Some(root_a.hash())
            );
            a
        } else {
            let mut statuses = [a.0, b.0];
            statuses.sort();
            assert_eq!(
                statuses,
                [StatusCode::OK.as_u16(), StatusCode::CONFLICT.as_u16()]
            );
            let (winner, root) = if a.0 == StatusCode::OK.as_u16() {
                (a, &root_a)
            } else {
                (b, &root_b)
            };
            assert_eq!(env.state.vaults.get_current_root(vault), Some(root.hash()));
            serde_json::from_slice(&winner.1).unwrap()
        };
        assert!(notifications.try_recv().is_ok());
        assert!(
            matches!(
                notifications.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ),
            "Only the committed winner publishes a root notification"
        );
        assert_eq!(
            outcome_success(&env, "root-outcome", vault, &outcome_query_body(&accepted)).await,
            accepted
        );
        let stream = outcome_success(
            &env,
            "root-outcome",
            vault,
            &serde_json::json!({ "protocol_version": 1 }),
        )
        .await;
        assert_eq!(
            stream["last_sequence"], 1,
            "Concurrent retry never consumes sequence 2"
        );
    }
}

#[tokio::test]
async fn root_outcome_actual_merge_receipt_preserves_conflicts_and_counts_after_later_publication()
{
    let env = setup();
    let vault = "outcome-merged-receipt";
    let base = outcome_root_fixture(&env, vault, 1).await;
    let side_b = outcome_root_fixture(&env, vault, 2).await;
    let side_c = outcome_root_fixture(&env, vault, 3).await;
    let later = outcome_root_fixture(&env, vault, 4).await;
    let root_path = format!("/api/v1/root/{vault}");
    for (parent, root) in [
        ("0".repeat(64), &base),
        (hash_to_hex(&base.hash()), &side_b),
    ] {
        let mut bytes = parent.into_bytes();
        bytes.extend_from_slice(&root.serialize().unwrap());
        assert_eq!(
            send_semantic(&env, "PUT", &root_path, &bytes).await.0,
            StatusCode::OK.as_u16()
        );
    }
    let request = outcome_commit_body(&env, &side_c, 1, &hash_to_hex(&base.hash()));
    let accepted = outcome_success(&env, "root-commit", vault, &request).await;
    assert_eq!(accepted["status"], "accepted");
    assert_eq!(accepted["sequence"], 1);
    assert_eq!(accepted["result"]["merged"], true);
    assert_eq!(accepted["result"]["auto_resolved"], 0);
    assert_eq!(accepted["result"]["text_merged"], 0);
    assert_eq!(
        accepted["result"]["conflicts"],
        serde_json::json!([{
            "path": "synthetic-note.md",
            "base_hash": hash_to_hex(&hash_bytes(&[1; 32])),
            "side_a_hash": hash_to_hex(&hash_bytes(&[2; 32])),
            "side_b_hash": hash_to_hex(&hash_bytes(&[3; 32])),
        }])
    );
    let merged_hash = env.state.vaults.get_current_root(vault).unwrap();
    assert_eq!(accepted["result"]["root_hash"], hash_to_hex(&merged_hash));
    assert_ne!(
        merged_hash,
        side_c.hash(),
        "Acceptance is the actual merge result, not incoming candidate identity"
    );

    let mut bytes = hash_to_hex(&merged_hash).into_bytes();
    bytes.extend_from_slice(&later.serialize().unwrap());
    assert_eq!(
        send_semantic(&env, "PUT", &root_path, &bytes).await.0,
        StatusCode::OK.as_u16()
    );
    assert_eq!(env.state.vaults.get_current_root(vault), Some(later.hash()));
    assert_eq!(
        outcome_success(&env, "root-outcome", vault, &outcome_query_body(&accepted)).await,
        accepted
    );
    assert_eq!(
        outcome_success(&env, "root-commit", vault, &request).await,
        accepted,
        "Retry returns exact original merge result, including conflict list and resolution counts"
    );
    assert_eq!(
        env.state.vaults.get_current_root(vault),
        Some(later.hash()),
        "Retry must not remerge against later current state"
    );
}

#[tokio::test]
async fn root_outcome_corrupt_or_unknown_authoritative_head_never_becomes_empty_or_overwritten() {
    for unknown_schema in [false, true] {
        let env = setup();
        let vault = if unknown_schema {
            "outcome-unknown-head"
        } else {
            "outcome-corrupt-head"
        };
        let root = outcome_root_fixture(&env, vault, 1).await;
        let request = outcome_commit_body(&env, &root, 1, "");
        let accepted = outcome_success(&env, "root-commit", vault, &request).await;
        let path = env.state.layout.vault_current_path(vault);
        let original = std::fs::read(&path).unwrap();
        assert!(original.starts_with(b"OBSETYNC_ROOT_HEAD_V1\n"));
        let damaged = if unknown_schema {
            // Valid checksum but unknown schema. This is not merely a failed
            // checksum that happens to exercise the same HTTP error mapping.
            let payload = serde_json::to_vec(&serde_json::json!({ "schema": 999,
                "vault_id": vault, "root_hash": hash_to_hex(&root.hash()), "receipts": {} }))
            .unwrap();
            let mut checksum = blake3::Hasher::new();
            checksum.update(b"obsetync:root-head:v1\0");
            checksum.update(&payload);
            let mut frame =
                format!("OBSETYNC_ROOT_HEAD_V1\n{}\n", checksum.finalize().to_hex()).into_bytes();
            frame.extend_from_slice(&payload);
            frame
        } else {
            let mut broken = original.clone();
            let last = broken.len() - 1;
            broken[last] ^= 1;
            broken
        };
        std::fs::write(&path, &damaged).unwrap();
        let mut legacy_put = hash_to_hex(&root.hash()).into_bytes();
        legacy_put.extend_from_slice(&root.serialize().unwrap());
        let capabilities = serde_json::to_vec(&serde_json::json!({ "protocol_version": 1,
            "vault_id": vault, "capabilities": ["tree-v2", "root-outcome-v1"] }))
        .unwrap();
        let calls = [
            ("GET", format!("/api/v1/root/{vault}"), Vec::new()),
            ("PUT", format!("/api/v1/root/{vault}"), legacy_put),
            (
                "POST",
                format!("/api/v1/diff/{vault}"),
                hash_to_hex(&root.hash()).into_bytes(),
            ),
            ("POST", "/api/v1/capabilities".into(), capabilities),
            (
                "POST",
                format!("/api/v1/root-outcome/{vault}"),
                serde_json::to_vec(&outcome_query_body(&accepted)).unwrap(),
            ),
            (
                "POST",
                format!("/api/v1/root-commit/{vault}"),
                serde_json::to_vec(&request).unwrap(),
            ),
        ];
        for (method, endpoint, body) in calls {
            let (status, response) = send_semantic(&env, method, &endpoint, &body).await;
            assert!((500..600).contains(&status),
                "Unreadable authoritative head must fail closed, not empty/404/success: {method} {endpoint} => {status}: {}",
                String::from_utf8_lossy(&response));
            assert_eq!(
                std::fs::read(&path).unwrap(),
                damaged,
                "A failing {method} request must preserve authoritative source bytes"
            );
        }
        // Restore only this test's known original bytes to prove that failed
        // calls did not mutate an independent receipt or advance a stream.
        std::fs::write(&path, &original).unwrap();
        assert_eq!(
            outcome_success(&env, "root-outcome", vault, &outcome_query_body(&accepted)).await,
            accepted
        );
        assert_eq!(env.state.vaults.get_current_root(vault), Some(root.hash()));
    }
}

#[tokio::test]
async fn root_outcome_retention_refusal_preserves_deletion_bypass() {
    let env = setup();
    let vault = "outcome-retention-bypass";
    let content = b"shared synthetic content";
    let content_hash = hash_bytes(content);
    assert_eq!(
        send_semantic(
            &env,
            "PUT",
            &format!("/api/v1/content/{}", hash_to_hex(&content_hash)),
            content,
        )
        .await
        .0,
        StatusCode::NO_CONTENT.as_u16(),
    );
    let entries = (0..256)
        .map(|index| {
            sync_core::chunk::FileEntry::new(
                format!("synthetic/note-{index:03}.md"),
                content_hash,
                1_900_000_000_000,
                content.len() as u64,
            )
        })
        .collect();
    let current = sync_core::versioned_root::build_root(
        &env.state.storage_writer,
        entries,
        sync_core::versioned_root::TREE_V1,
        vault,
        "fixture-seed",
    )
    .await
    .unwrap();
    let candidate = sync_core::versioned_root::build_root(
        &env.state.storage_writer,
        Vec::new(),
        sync_core::versioned_root::TREE_V1,
        vault,
        "fixture-deletion",
    )
    .await
    .unwrap();
    let mut legacy_seed = "0".repeat(64).into_bytes();
    legacy_seed.extend_from_slice(&current.serialize().unwrap());
    assert_eq!(
        send_semantic(&env, "PUT", &format!("/api/v1/root/{vault}"), &legacy_seed,)
            .await
            .0,
        StatusCode::OK.as_u16(),
    );

    // The ordinary suite may run with the default warning mode. A separate
    // process with OBSETYNC_GUARD=enforce proves the production branch which
    // would consume this grant. Never mutate the process-global environment.
    let configuration = crate::guard::config();
    if std::env::var("OBSETYNC_GUARD").as_deref() == Ok("enforce") {
        assert_eq!(configuration.mode, crate::guard::GuardMode::Enforce);
    }
    let scan = crate::guard::scan_versioned(
        env.state.storage_writer.clone(),
        current.clone(),
        candidate.clone(),
    )
    .await
    .unwrap();
    assert_eq!(scan.current_total, 256);
    assert_eq!(scan.deletions, 256);
    assert_eq!(scan.content_changes, 0);
    assert_eq!(scan.triggered(configuration), Some("blast_radius"));

    let device = env.state.devices.list().pop().unwrap();
    for index in 0..128u64 {
        let retained_device = format!("retained-fixture-{index:03}");
        assert_ne!(retained_device, device.device_id);
        env.state
            .vaults
            .set_current_root_with_receipt(
                vault,
                &current.hash(),
                &retained_device,
                crate::root_head::RootReceipt {
                    sequence: 1,
                    mutation_id: format!("{index:032x}"),
                    request_hash: format!("{index:064x}"),
                    result: serde_json::json!({
                        "accepted": true,
                        "root_hash": hash_to_hex(&current.hash()),
                    }),
                },
            )
            .unwrap();
    }
    assert!(env
        .state
        .vaults
        .get_root_receipt(vault, &device.device_id)
        .unwrap()
        .is_none());
    let head_path = env.state.layout.vault_current_path(vault);
    let previous_head = std::fs::read(&head_path).unwrap();
    devices::grant_deletion_bypass(&env.state.layout, &device.device_id, 60).unwrap();
    assert!(devices::deletion_bypass_remaining_ms(&env.state.layout, &device.device_id).is_some());

    let request = outcome_commit_body(&env, &candidate, 1, &hash_to_hex(&current.hash()));
    let mut notifications = env.state.subscribe_roots(vault);
    let (status, body) = outcome_post(&env, "root-commit", vault, &request).await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
        "New device receipt exceeds retained-device capacity: {}",
        String::from_utf8_lossy(&body),
    );
    assert_eq!(std::fs::read(&head_path).unwrap(), previous_head);
    assert_eq!(
        env.state.vaults.get_current_root(vault),
        Some(current.hash())
    );
    assert!(env
        .state
        .vaults
        .get_root_receipt(vault, &device.device_id)
        .unwrap()
        .is_none());
    assert!(matches!(
        notifications.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    assert!(
        devices::consume_deletion_bypass(&env.state.layout, &device.device_id),
        "Deterministic retention refusal must happen before consuming one-time deletion approval",
    );
    assert!(!devices::consume_deletion_bypass(
        &env.state.layout,
        &device.device_id
    ));
}
