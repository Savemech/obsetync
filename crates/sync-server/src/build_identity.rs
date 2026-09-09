use serde::Serialize;

pub const SCHEMA: &str = "obsetync-server-build-identity-v1";
pub const SEMVER: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("OBSETYNC_SERVER_BUILD_GIT_COMMIT");
pub const SOURCE_STATE: &str = env!("OBSETYNC_SERVER_BUILD_SOURCE_STATE");

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ProtocolIdentity {
    pub api: u8,
    pub secure_transport_wire: u8,
    pub tree: [u32; 2],
    pub ws_data: [u16; 2],
    pub root_outcome: u32,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ServerBuildIdentity {
    pub schema: &'static str,
    pub semver: &'static str,
    pub git_commit: &'static str,
    pub source_state: &'static str,
    pub protocol: ProtocolIdentity,
}

pub fn current() -> ServerBuildIdentity {
    ServerBuildIdentity {
        schema: SCHEMA,
        semver: SEMVER,
        git_commit: GIT_COMMIT,
        source_state: SOURCE_STATE,
        protocol: ProtocolIdentity {
            api: crate::api::API_VERSION,
            secure_transport_wire: crate::secure::WIRE_VERSION,
            tree: [
                sync_core::versioned_root::TREE_V1,
                sync_core::versioned_root::TREE_V2,
            ],
            ws_data: crate::ws_data::WIRE_VERSIONS,
            root_outcome: crate::root_outcome::PROTOCOL_VERSION,
        },
    }
}

pub fn canonical_json() -> String {
    serde_json::to_string(&current()).expect("static server build identity must serialize")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_sha(value: &str) -> bool {
        value.len() == 40
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    #[test]
    fn embedded_identity_is_complete_and_canonical() {
        let identity = current();
        assert_eq!(identity.schema, SCHEMA);
        assert_eq!(identity.semver, env!("CARGO_PKG_VERSION"));
        assert!(matches!(
            identity.source_state,
            "clean" | "dirty" | "local-unknown"
        ));
        if identity.source_state == "local-unknown" {
            assert_eq!(identity.git_commit, "unknown");
        } else {
            assert!(full_sha(identity.git_commit));
        }
        assert_eq!(identity.protocol.api, crate::api::API_VERSION);
        assert_eq!(
            identity.protocol.secure_transport_wire,
            crate::secure::WIRE_VERSION
        );
        assert_eq!(identity.protocol.tree, [1, 2]);
        assert_eq!(identity.protocol.ws_data, crate::ws_data::WIRE_VERSIONS);
        assert_eq!(
            identity.protocol.root_outcome,
            crate::root_outcome::PROTOCOL_VERSION
        );
        for capability in ["ws-data-v1", "ws-data-v2", "root-outcome-v1"] {
            assert!(crate::api::SERVER_CAPABILITIES.contains(&capability));
        }

        let encoded = canonical_json();
        assert!(!encoded.contains('\n'));
        let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(parsed["schema"], SCHEMA);
        assert_eq!(parsed["git_commit"], identity.git_commit);
        assert_eq!(parsed["protocol"]["secure_transport_wire"], 2);
    }
}
