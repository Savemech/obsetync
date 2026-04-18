use crate::hash::{FileHash, IncrementalHasher};
use flatbuffers::FlatBufferBuilder;
use sync_schema::sync_chunk;

/// A single file entry inside a leaf chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Vault-relative path, e.g. "notes/2024/jan.md"
    pub path: String,
    /// Blake3 hash of file content
    pub hash: FileHash,
    /// Modification time in milliseconds since epoch
    pub mtime_ms: u64,
    /// File size in bytes
    pub size_bytes: u64,
}

impl FileEntry {
    pub fn new(path: String, hash: FileHash, mtime_ms: u64, size_bytes: u64) -> Self {
        Self {
            path,
            hash,
            mtime_ms,
            size_bytes,
        }
    }
}

impl PartialOrd for FileEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.path.cmp(&other.path)
    }
}

/// A leaf chunk: sorted list of FileEntry covering a path range.
/// Target size: 500-2000 entries.
#[derive(Debug, Clone)]
pub struct LeafChunk {
    pub entries: Vec<FileEntry>, // sorted by path
}

impl LeafChunk {
    pub fn new(mut entries: Vec<FileEntry>) -> Self {
        entries.sort();
        Self { entries }
    }

    /// Deterministic content hash of this chunk.
    /// Defined as Blake3(serialize()) so the server can verify
    /// hash_bytes(uploaded_bytes) == expected_hash on PUT /chunk/{hash}.
    pub fn hash(&self) -> FileHash {
        crate::hash::hash_bytes(&self.serialize())
    }

    /// Serialize to FlatBuffers bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::<flatbuffers::DefaultAllocator>::with_capacity(self.entries.len() * 80);

        let entries: Vec<_> = self
            .entries
            .iter()
            .map(|e| {
                let path = builder.create_string(&e.path);
                let hash = builder.create_vector(&e.hash);
                sync_chunk::FileEntry::create(
                    &mut builder,
                    &sync_chunk::FileEntryArgs {
                        path: Some(path),
                        hash: Some(hash),
                        mtime_ms: e.mtime_ms,
                        size_bytes: e.size_bytes,
                    },
                )
            })
            .collect();

        let entries_vec = builder.create_vector(&entries);
        let leaf = sync_chunk::LeafChunk::create(
            &mut builder,
            &sync_chunk::LeafChunkArgs {
                version: 1,
                entries: Some(entries_vec),
            },
        );

        let envelope = sync_chunk::ChunkEnvelope::create(
            &mut builder,
            &sync_chunk::ChunkEnvelopeArgs {
                node_type: sync_chunk::NodeType::LeafChunk,
                node: Some(leaf.as_union_value()),
            },
        );

        builder.finish(envelope, None);
        builder.finished_data().to_vec()
    }

    /// Deserialize from FlatBuffers bytes.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, ChunkError> {
        let envelope = flatbuffers::root::<sync_chunk::ChunkEnvelope>(bytes)
            .map_err(|e| ChunkError::Deserialize(e.to_string()))?;

        if envelope.node_type() != sync_chunk::NodeType::LeafChunk {
            return Err(ChunkError::Deserialize(format!(
                "expected LeafChunk, got {:?}",
                envelope.node_type()
            )));
        }

        let leaf = envelope
            .node_as_leaf_chunk()
            .ok_or_else(|| ChunkError::Deserialize("missing LeafChunk node".into()))?;

        let fb_entries = leaf.entries();

        let mut entries = Vec::with_capacity(fb_entries.len());
        for fb_entry in fb_entries.iter() {
            let path = fb_entry.path().to_string();
            let hash_vec = fb_entry.hash();
            if hash_vec.len() != 32 {
                return Err(ChunkError::Deserialize(format!(
                    "hash length {} != 32",
                    hash_vec.len()
                )));
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(hash_vec.bytes());
            entries.push(FileEntry {
                path,
                hash,
                mtime_ms: fb_entry.mtime_ms(),
                size_bytes: fb_entry.size_bytes(),
            });
        }

        Ok(Self { entries })
    }
}

/// An internal node: maps directory prefix -> child chunk hash.
#[derive(Debug, Clone)]
pub struct InternalNode {
    pub children: Vec<(String, FileHash)>, // (prefix, chunk_hash), sorted by prefix
}

impl InternalNode {
    pub fn new(mut children: Vec<(String, FileHash)>) -> Self {
        children.sort_by(|a, b| a.0.cmp(&b.0));
        Self { children }
    }

    /// Deterministic content hash of this chunk.
    /// Blake3(serialize()) for consistency with server CAS validation.
    pub fn hash(&self) -> FileHash {
        crate::hash::hash_bytes(&self.serialize())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::<flatbuffers::DefaultAllocator>::with_capacity(self.children.len() * 48);

        let children: Vec<_> = self
            .children
            .iter()
            .map(|(prefix, hash)| {
                let prefix_str = builder.create_string(prefix);
                let hash_vec = builder.create_vector(hash);
                sync_chunk::ChildRef::create(
                    &mut builder,
                    &sync_chunk::ChildRefArgs {
                        prefix: Some(prefix_str),
                        hash: Some(hash_vec),
                    },
                )
            })
            .collect();

        let children_vec = builder.create_vector(&children);
        let node = sync_chunk::InternalNode::create(
            &mut builder,
            &sync_chunk::InternalNodeArgs {
                version: 1,
                children: Some(children_vec),
            },
        );

        let envelope = sync_chunk::ChunkEnvelope::create(
            &mut builder,
            &sync_chunk::ChunkEnvelopeArgs {
                node_type: sync_chunk::NodeType::InternalNode,
                node: Some(node.as_union_value()),
            },
        );

        builder.finish(envelope, None);
        builder.finished_data().to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, ChunkError> {
        let envelope = flatbuffers::root::<sync_chunk::ChunkEnvelope>(bytes)
            .map_err(|e| ChunkError::Deserialize(e.to_string()))?;

        if envelope.node_type() != sync_chunk::NodeType::InternalNode {
            return Err(ChunkError::Deserialize(format!(
                "expected InternalNode, got {:?}",
                envelope.node_type()
            )));
        }

        let node = envelope
            .node_as_internal_node()
            .ok_or_else(|| ChunkError::Deserialize("missing InternalNode".into()))?;

        let fb_children = node.children();

        let mut children = Vec::with_capacity(fb_children.len());
        for child in fb_children.iter() {
            let prefix = child.prefix().to_string();
            let hash_vec = child.hash();
            if hash_vec.len() != 32 {
                return Err(ChunkError::Deserialize(format!(
                    "hash length {} != 32",
                    hash_vec.len()
                )));
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(hash_vec.bytes());
            children.push((prefix, hash));
        }

        Ok(Self { children })
    }
}

/// Root node: single entry point into the tree.
#[derive(Debug, Clone)]
pub struct RootNode {
    pub vault_id: String,
    pub created_ms: u64,
    pub version: u32,
    pub children: Vec<(String, FileHash)>, // top-level dir -> internal node hash
    pub total_files: u64,
    pub parent_hash: Option<FileHash>,
    pub device_id: String,
}

impl RootNode {
    pub fn hash(&self) -> FileHash {
        let mut hasher = IncrementalHasher::new();
        for (prefix, child_hash) in &self.children {
            hasher.update_str(prefix);
            hasher.update(child_hash);
        }
        hasher.finalize()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::<flatbuffers::DefaultAllocator>::with_capacity(self.children.len() * 48 + 128);

        let vault_id = builder.create_string(&self.vault_id);
        let device_id = builder.create_string(&self.device_id);
        let root_hash_val = self.hash();
        let root_hash = builder.create_vector(&root_hash_val);
        let parent_hash = self
            .parent_hash
            .as_ref()
            .map(|h| builder.create_vector(h));

        let children: Vec<_> = self
            .children
            .iter()
            .map(|(prefix, hash)| {
                let prefix_str = builder.create_string(prefix);
                let hash_vec = builder.create_vector(hash);
                sync_chunk::ChildRef::create(
                    &mut builder,
                    &sync_chunk::ChildRefArgs {
                        prefix: Some(prefix_str),
                        hash: Some(hash_vec),
                    },
                )
            })
            .collect();

        let children_vec = builder.create_vector(&children);

        let root = sync_chunk::RootNode::create(
            &mut builder,
            &sync_chunk::RootNodeArgs {
                version: self.version,
                vault_id: Some(vault_id),
                created_ms: self.created_ms,
                total_files: self.total_files,
                children: Some(children_vec),
                root_hash: Some(root_hash),
                parent_hash,
                device_id: Some(device_id),
            },
        );

        let envelope = sync_chunk::ChunkEnvelope::create(
            &mut builder,
            &sync_chunk::ChunkEnvelopeArgs {
                node_type: sync_chunk::NodeType::RootNode,
                node: Some(root.as_union_value()),
            },
        );

        builder.finish(envelope, None);
        builder.finished_data().to_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, ChunkError> {
        let envelope = flatbuffers::root::<sync_chunk::ChunkEnvelope>(bytes)
            .map_err(|e| ChunkError::Deserialize(e.to_string()))?;

        if envelope.node_type() != sync_chunk::NodeType::RootNode {
            return Err(ChunkError::Deserialize(format!(
                "expected RootNode, got {:?}",
                envelope.node_type()
            )));
        }

        let root = envelope
            .node_as_root_node()
            .ok_or_else(|| ChunkError::Deserialize("missing RootNode".into()))?;

        let vault_id = root.vault_id().to_string();

        let device_id = root
            .device_id()
            .unwrap_or("")
            .to_string();

        let parent_hash = root.parent_hash().and_then(|h| {
            if h.len() == 32 {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(h.bytes());
                Some(hash)
            } else {
                None
            }
        });

        let fb_children = root.children();

        let mut children = Vec::with_capacity(fb_children.len());
        for child in fb_children.iter() {
            let prefix = child.prefix().to_string();
            let hash_vec = child.hash();
            if hash_vec.len() != 32 {
                return Err(ChunkError::Deserialize(format!(
                    "hash length {} != 32",
                    hash_vec.len()
                )));
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(hash_vec.bytes());
            children.push((prefix, hash));
        }

        Ok(Self {
            vault_id,
            created_ms: root.created_ms(),
            version: root.version(),
            children,
            total_files: root.total_files(),
            parent_hash,
            device_id,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ChunkError {
    #[error("invalid chunk bytes: {0}")]
    Deserialize(String),
    #[error("chunk not found: {0}")]
    NotFound(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::hash_bytes;

    #[test]
    fn leaf_chunk_roundtrip() {
        let entries = vec![
            FileEntry::new("a.md".into(), hash_bytes(b"aaa"), 1000, 100),
            FileEntry::new("b.md".into(), hash_bytes(b"bbb"), 2000, 200),
            FileEntry::new("c.md".into(), hash_bytes(b"ccc"), 3000, 300),
        ];
        let chunk = LeafChunk::new(entries);
        let bytes = chunk.serialize();
        let decoded = LeafChunk::deserialize(&bytes).unwrap();

        assert_eq!(chunk.entries.len(), decoded.entries.len());
        for (a, b) in chunk.entries.iter().zip(decoded.entries.iter()) {
            assert_eq!(a.path, b.path);
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.mtime_ms, b.mtime_ms);
            assert_eq!(a.size_bytes, b.size_bytes);
        }
        assert_eq!(chunk.hash(), decoded.hash());
    }

    #[test]
    fn leaf_chunk_hash_deterministic() {
        let entries = vec![
            FileEntry::new("x.md".into(), hash_bytes(b"xxx"), 5000, 500),
        ];
        let c1 = LeafChunk::new(entries.clone());
        let c2 = LeafChunk::new(entries);
        assert_eq!(c1.hash(), c2.hash());
    }

    #[test]
    fn internal_node_roundtrip() {
        let children = vec![
            ("assets/".into(), hash_bytes(b"child1")),
            ("notes/".into(), hash_bytes(b"child2")),
        ];
        let node = InternalNode::new(children);
        let bytes = node.serialize();
        let decoded = InternalNode::deserialize(&bytes).unwrap();

        assert_eq!(node.children.len(), decoded.children.len());
        for (a, b) in node.children.iter().zip(decoded.children.iter()) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1, b.1);
        }
        assert_eq!(node.hash(), decoded.hash());
    }

    #[test]
    fn root_node_roundtrip() {
        let root = RootNode {
            vault_id: "test-vault".into(),
            created_ms: 1720000000000,
            version: 1,
            children: vec![
                ("notes/".into(), hash_bytes(b"n")),
                ("assets/".into(), hash_bytes(b"a")),
            ],
            total_files: 5000,
            parent_hash: Some(hash_bytes(b"parent")),
            device_id: "desktop-home".into(),
        };
        let bytes = root.serialize();
        let decoded = RootNode::deserialize(&bytes).unwrap();

        assert_eq!(root.vault_id, decoded.vault_id);
        assert_eq!(root.created_ms, decoded.created_ms);
        assert_eq!(root.total_files, decoded.total_files);
        assert_eq!(root.parent_hash, decoded.parent_hash);
        assert_eq!(root.device_id, decoded.device_id);
        assert_eq!(root.hash(), decoded.hash());
    }

    #[test]
    fn root_node_no_parent() {
        let root = RootNode {
            vault_id: "test".into(),
            created_ms: 0,
            version: 1,
            children: vec![],
            total_files: 0,
            parent_hash: None,
            device_id: "dev".into(),
        };
        let bytes = root.serialize();
        let decoded = RootNode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.parent_hash, None);
    }
}
