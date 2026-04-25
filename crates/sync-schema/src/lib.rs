#[allow(warnings, clippy::all)]
mod chunk_generated;

pub use chunk_generated::sync_chunk;

#[cfg(test)]
mod tests {
    use super::sync_chunk;
    use flatbuffers::FlatBufferBuilder;

    #[test]
    fn node_type_constants_match_enum_values() {
        assert_eq!(sync_chunk::NodeType::NONE.0, 0);
        assert_eq!(sync_chunk::NodeType::LeafChunk.0, 1);
        assert_eq!(sync_chunk::NodeType::InternalNode.0, 2);
        assert_eq!(sync_chunk::NodeType::RootNode.0, 3);
    }

    #[test]
    fn node_type_variant_names() {
        assert_eq!(
            sync_chunk::NodeType::LeafChunk.variant_name(),
            Some("LeafChunk")
        );
        assert_eq!(
            sync_chunk::NodeType::InternalNode.variant_name(),
            Some("InternalNode")
        );
        assert_eq!(
            sync_chunk::NodeType::RootNode.variant_name(),
            Some("RootNode")
        );
        assert_eq!(sync_chunk::NodeType::NONE.variant_name(), Some("NONE"));
        // Out-of-range value returns None and Debug-prints as <UNKNOWN _>.
        let unknown = sync_chunk::NodeType(99);
        assert!(unknown.variant_name().is_none());
        assert!(format!("{:?}", unknown).contains("UNKNOWN"));
    }

    #[test]
    fn leaf_chunk_envelope_roundtrip_through_schema() {
        // Build a minimal LeafChunk envelope using only the schema crate, then
        // read it back. Catches schema-level breakage independently of sync-core.
        let mut b = FlatBufferBuilder::<flatbuffers::DefaultAllocator>::with_capacity(256);

        let path = b.create_string("a.md");
        let hash = b.create_vector(&[0u8; 32]);
        let entry = sync_chunk::FileEntry::create(
            &mut b,
            &sync_chunk::FileEntryArgs {
                path: Some(path),
                hash: Some(hash),
                mtime_ms: 1234,
                size_bytes: 7,
            },
        );
        let entries = b.create_vector(&[entry]);
        let leaf = sync_chunk::LeafChunk::create(
            &mut b,
            &sync_chunk::LeafChunkArgs {
                version: 1,
                entries: Some(entries),
            },
        );
        let envelope = sync_chunk::ChunkEnvelope::create(
            &mut b,
            &sync_chunk::ChunkEnvelopeArgs {
                node_type: sync_chunk::NodeType::LeafChunk,
                node: Some(leaf.as_union_value()),
            },
        );
        b.finish(envelope, None);
        let bytes = b.finished_data().to_vec();

        let env = flatbuffers::root::<sync_chunk::ChunkEnvelope>(&bytes).unwrap();
        assert_eq!(env.node_type(), sync_chunk::NodeType::LeafChunk);
        let leaf = env.node_as_leaf_chunk().unwrap();
        assert_eq!(leaf.version(), 1);
        let entries = leaf.entries();
        assert_eq!(entries.len(), 1);
        let e = entries.get(0);
        assert_eq!(e.path(), "a.md");
        assert_eq!(e.mtime_ms(), 1234);
        assert_eq!(e.size_bytes(), 7);
        assert_eq!(e.hash().bytes(), &[0u8; 32]);
    }

    #[test]
    fn internal_node_envelope_roundtrip_through_schema() {
        let mut b = FlatBufferBuilder::<flatbuffers::DefaultAllocator>::with_capacity(256);
        let prefix = b.create_string("notes/");
        let hash = b.create_vector(&[1u8; 32]);
        let child = sync_chunk::ChildRef::create(
            &mut b,
            &sync_chunk::ChildRefArgs {
                prefix: Some(prefix),
                hash: Some(hash),
            },
        );
        let kids = b.create_vector(&[child]);
        let node = sync_chunk::InternalNode::create(
            &mut b,
            &sync_chunk::InternalNodeArgs {
                version: 1,
                children: Some(kids),
            },
        );
        let env = sync_chunk::ChunkEnvelope::create(
            &mut b,
            &sync_chunk::ChunkEnvelopeArgs {
                node_type: sync_chunk::NodeType::InternalNode,
                node: Some(node.as_union_value()),
            },
        );
        b.finish(env, None);
        let bytes = b.finished_data().to_vec();

        let env = flatbuffers::root::<sync_chunk::ChunkEnvelope>(&bytes).unwrap();
        assert_eq!(env.node_type(), sync_chunk::NodeType::InternalNode);
        let inner = env.node_as_internal_node().unwrap();
        assert_eq!(inner.children().len(), 1);
        assert_eq!(inner.children().get(0).prefix(), "notes/");
    }

    #[test]
    fn empty_buffer_fails_to_parse() {
        // Sanity: garbage bytes must yield a parse error, not a crash.
        let res = flatbuffers::root::<sync_chunk::ChunkEnvelope>(&[]);
        assert!(res.is_err());
    }
}
