use std::env;

const UNKNOWN_COMMIT: &str = "unknown";

fn flag(name: &str) -> bool {
    match env::var(name) {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" || value.is_empty() => false,
        Ok(_) => panic!("{name} must be 0 or 1"),
        Err(env::VarError::NotPresent) => false,
        Err(error) => panic!("cannot read {name}: {error}"),
    }
}

fn full_sha(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn main() {
    for name in [
        "OBSETYNC_BUILD_GIT_COMMIT",
        "OBSETYNC_BUILD_SOURCE_STATE",
        "OBSETYNC_BUILD_EXPECTED_COMMIT",
        "OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT",
        "OBSETYNC_BUILD_REQUIRE_CLEAN",
        "OBSETYNC_BUILD_EXPECTED_VERSION",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }

    let version = env::var("CARGO_PKG_VERSION").expect("Cargo must provide CARGO_PKG_VERSION");
    if let Ok(expected) = env::var("OBSETYNC_BUILD_EXPECTED_VERSION") {
        if !expected.is_empty() && expected != version {
            panic!("expected semantic version mismatch ({expected} != {version})");
        }
    }

    let commit = env::var("OBSETYNC_BUILD_GIT_COMMIT").unwrap_or_else(|_| UNKNOWN_COMMIT.into());
    let source_state =
        env::var("OBSETYNC_BUILD_SOURCE_STATE").unwrap_or_else(|_| "local-unknown".into());
    match source_state.as_str() {
        "clean" | "dirty" if full_sha(&commit) => {}
        "local-unknown" if commit == UNKNOWN_COMMIT => {}
        _ => panic!("build commit/source-state pair is invalid"),
    }
    if flag("OBSETYNC_BUILD_REQUIRE_CLEAN") && source_state != "clean" {
        panic!("release build requires clean source state");
    }

    let expected_commit = env::var("OBSETYNC_BUILD_EXPECTED_COMMIT").unwrap_or_default();
    if !expected_commit.is_empty() && !full_sha(&expected_commit) {
        panic!("expected commit must be a full lowercase Git SHA-1");
    }
    if flag("OBSETYNC_BUILD_REQUIRE_EXPECTED_COMMIT") && expected_commit.is_empty() {
        panic!("release build requires an expected commit");
    }
    if !expected_commit.is_empty() && commit != expected_commit {
        panic!("expected commit mismatch ({expected_commit} != {commit})");
    }

    println!("cargo:rustc-env=OBSETYNC_SERVER_BUILD_GIT_COMMIT={commit}");
    println!("cargo:rustc-env=OBSETYNC_SERVER_BUILD_SOURCE_STATE={source_state}");
}
