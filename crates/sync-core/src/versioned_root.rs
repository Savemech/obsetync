//! Coexistence boundary for persisted Tree v1 and Tree v2 roots.
//!
//! Production callers must decode history through [`VersionedRoot`] instead
//! of guessing a layout from a version field inside one format. V1 remains a
//! FlatBuffer `RootNode`; v2 has the disjoint, strict `OVR2` encoding.

use crate::chunk::{ChunkError, FileEntry, RootNode};
use crate::hash::FileHash;
use crate::store::ChunkStore;
use crate::tree_v2::RootNodeV2;

pub const TREE_V1: u32 = 1;
pub const TREE_V2: u32 = 2;

#[derive(Debug, Clone)]
pub enum VersionedRoot {
    V1(RootNode),
    V2(RootNodeV2),
}

impl VersionedRoot {
    pub fn deserialize(bytes: &[u8]) -> Result<Self, ChunkError> {
        if bytes.starts_with(b"OVR2") {
            return RootNodeV2::deserialize(bytes).map(Self::V2);
        }
        let root = RootNode::deserialize(bytes)?;
        if root.version != TREE_V1 {
            return Err(ChunkError::Deserialize(format!(
                "unsupported Tree v1 root version {}",
                root.version
            )));
        }
        Ok(Self::V1(root))
    }

    pub fn serialize(&self) -> Result<Vec<u8>, ChunkError> {
        match self {
            Self::V1(root) => {
                if root.version != TREE_V1 {
                    return Err(ChunkError::Deserialize(format!(
                        "unsupported Tree v1 root version {}",
                        root.version
                    )));
                }
                Ok(root.serialize())
            }
            Self::V2(root) => root.serialize(),
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            Self::V1(_) => TREE_V1,
            Self::V2(_) => TREE_V2,
        }
    }

    pub fn hash(&self) -> FileHash {
        match self {
            Self::V1(root) => root.hash(),
            Self::V2(root) => root.hash(),
        }
    }

    pub fn vault_id(&self) -> &str {
        match self {
            Self::V1(root) => &root.vault_id,
            Self::V2(root) => &root.vault_id,
        }
    }

    pub fn created_ms(&self) -> u64 {
        match self {
            Self::V1(root) => root.created_ms,
            Self::V2(root) => root.created_ms,
        }
    }

    pub fn total_files(&self) -> u64 {
        match self {
            Self::V1(root) => root.total_files,
            Self::V2(root) => root.total_files(),
        }
    }

    pub fn parent_hash(&self) -> Option<FileHash> {
        match self {
            Self::V1(root) => root.parent_hash,
            Self::V2(root) => root.parent_hash,
        }
    }

    pub fn device_id(&self) -> &str {
        match self {
            Self::V1(root) => &root.device_id,
            Self::V2(root) => &root.device_id,
        }
    }

    pub fn set_history_metadata(
        &mut self,
        created_ms: u64,
        parent_hash: Option<FileHash>,
        device_id: impl Into<String>,
    ) {
        let device_id = device_id.into();
        match self {
            Self::V1(root) => {
                root.created_ms = created_ms;
                root.parent_hash = parent_hash;
                root.device_id = device_id;
            }
            Self::V2(root) => {
                root.created_ms = created_ms;
                root.parent_hash = parent_hash;
                root.device_id = device_id;
            }
        }
    }

    pub fn as_v1(&self) -> Option<&RootNode> {
        match self {
            Self::V1(root) => Some(root),
            Self::V2(_) => None,
        }
    }

    pub fn as_v2(&self) -> Option<&RootNodeV2> {
        match self {
            Self::V1(_) => None,
            Self::V2(root) => Some(root),
        }
    }
}

pub async fn build_root<S: ChunkStore>(
    store: &S,
    entries: Vec<FileEntry>,
    version: u32,
    vault_id: &str,
    device_id: &str,
) -> Result<VersionedRoot, ChunkError> {
    match version {
        TREE_V1 => crate::tree::build_tree(store, entries, vault_id, device_id)
            .await
            .map(VersionedRoot::V1),
        TREE_V2 => {
            let tree = crate::tree_v2::build_tree(store, entries).await?;
            RootNodeV2::new(vault_id, device_id, tree).map(VersionedRoot::V2)
        }
        other => Err(ChunkError::Deserialize(format!(
            "unsupported tree version {other}"
        ))),
    }
}

pub fn empty_root(
    version: u32,
    vault_id: &str,
    device_id: &str,
) -> Result<VersionedRoot, ChunkError> {
    match version {
        TREE_V1 => Ok(VersionedRoot::V1(RootNode {
            vault_id: vault_id.to_owned(),
            created_ms: 0,
            version: TREE_V1,
            children: Vec::new(),
            total_files: 0,
            parent_hash: None,
            device_id: device_id.to_owned(),
        })),
        TREE_V2 => RootNodeV2::new(
            vault_id,
            device_id,
            crate::tree_v2::TreeV2Root {
                version: TREE_V2,
                total_files: 0,
                child: None,
            },
        )
        .map(VersionedRoot::V2),
        other => Err(ChunkError::Deserialize(format!(
            "unsupported tree version {other}"
        ))),
    }
}

pub async fn load_all_entries<S: ChunkStore>(
    store: &S,
    root: &VersionedRoot,
) -> Result<Vec<FileEntry>, ChunkError> {
    if root.total_files() > crate::tree_v2::MAX_LOADED_ENTRIES as u64 {
        return Err(ChunkError::Deserialize(
            "versioned root exceeds the entry limit".into(),
        ));
    }
    let entries = match root {
        VersionedRoot::V1(root) => {
            let capacity = usize::try_from(root.total_files)
                .unwrap_or(0)
                .min(crate::tree_v2::MAX_LOADED_ENTRIES);
            let mut entries = Vec::with_capacity(capacity);
            for (_, hash) in &root.children {
                entries.extend(crate::tree::load_all_entries(store, hash).await?);
            }
            entries.sort();
            entries
        }
        VersionedRoot::V2(root) => crate::tree_v2::load_all_entries(store, &root.tree).await?,
    };
    if entries.len() as u64 != root.total_files() {
        return Err(ChunkError::Deserialize(format!(
            "root declares {} files but contains {}",
            root.total_files(),
            entries.len()
        )));
    }
    if entries.windows(2).any(|pair| pair[0].path >= pair[1].path) {
        return Err(ChunkError::Deserialize(
            "versioned root entries are not strictly ordered".into(),
        ));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;
    use crate::store::MemoryChunkStore;

    fn entry(path: &str, revision: u64) -> FileEntry {
        FileEntry::new(
            path.to_owned(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            revision,
            revision + 1,
        )
    }

    #[tokio::test]
    async fn dual_decoder_roundtrips_disjoint_formats_and_semantics() {
        let entries = vec![entry("a.md", 1), entry("notes/b.md", 2)];
        let v1_store = MemoryChunkStore::new();
        let v1 = build_root(&v1_store, entries.clone(), TREE_V1, "vault", "device")
            .await
            .unwrap();
        let v2_store = MemoryChunkStore::new();
        let v2 = build_root(&v2_store, entries.clone(), TREE_V2, "vault", "device")
            .await
            .unwrap();

        let v1_bytes = v1.serialize().unwrap();
        let v2_bytes = v2.serialize().unwrap();
        assert!(!v1_bytes.starts_with(b"OVR2"));
        assert!(v2_bytes.starts_with(b"OVR2"));
        assert_eq!(VersionedRoot::deserialize(&v1_bytes).unwrap().version(), 1);
        assert_eq!(VersionedRoot::deserialize(&v2_bytes).unwrap().version(), 2);
        assert!(RootNode::deserialize(&v2_bytes).is_err());
        assert_eq!(load_all_entries(&v1_store, &v1).await.unwrap(), entries);
        assert_eq!(load_all_entries(&v2_store, &v2).await.unwrap(), entries);
    }

    #[test]
    fn versioned_decoder_rejects_non_v1_flatbuffer_root() {
        let root = RootNode {
            vault_id: "vault".into(),
            created_ms: 0,
            version: 9,
            children: Vec::new(),
            total_files: 0,
            parent_hash: None,
            device_id: "device".into(),
        };
        assert!(VersionedRoot::deserialize(&root.serialize()).is_err());
        assert!(VersionedRoot::V1(root).serialize().is_err());
    }

    #[tokio::test]
    async fn rebuilds_are_canonical_within_each_version() {
        let entries = (0..5_000)
            .map(|index| entry(&format!("wide/{index:08}.md"), index as u64))
            .collect::<Vec<_>>();
        for version in [TREE_V1, TREE_V2] {
            let first_store = MemoryChunkStore::new();
            let first = build_root(&first_store, entries.clone(), version, "vault", "one")
                .await
                .unwrap();
            let second_store = MemoryChunkStore::new();
            let mut reversed = entries.clone();
            reversed.reverse();
            let second = build_root(&second_store, reversed, version, "vault", "two")
                .await
                .unwrap();
            assert_eq!(first.hash(), second.hash());
        }
    }
}
