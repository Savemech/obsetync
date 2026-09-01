//! Experimental Tree v2 range Merkle tree.
//!
//! This module is deliberately not wired into the production root format yet.
//! Slice 11 uses it to prove deterministic path-only boundaries, localized
//! updates, recursive range diff, strict validation, and measurable churn
//! before Slice 12 adds mixed-version persistence/migration.

use crate::chunk::{ChunkError, FileEntry};
use crate::diff::{self, DiffResult, DiffStats, FileDelta};
use crate::hash::{hash_bytes, FileHash};
use crate::store::ChunkStore;
use std::collections::HashSet;

const LEAF_MAGIC: &[u8; 4] = b"OVL2";
const INTERNAL_MAGIC: &[u8; 4] = b"OVI2";
const ROOT_MAGIC: &[u8; 4] = b"OVR2";
const LEAF_HEADER_BYTES: usize = 8;
const INTERNAL_HEADER_BYTES: usize = 12;
const STORED_ROOT_HEADER_BYTES: usize = 64;
const MAX_STORED_ROOT_BYTES: usize = 16 * 1024;
const MAX_ROOT_ID_BYTES: usize = 1_024;

pub const TREE_VERSION: u32 = 2;
pub const MIN_LEAF_ENTRIES: usize = 128;
pub const MAX_LEAF_ENTRIES: usize = 1_024;
pub const MIN_LEAF_BYTES: usize = 32 * 1024;
pub const MAX_NODE_BYTES: usize = 256 * 1024;
pub const MAX_PATH_BYTES: usize = 4_096;
pub const MAX_TREE_DEPTH: u16 = 16;
pub const MAX_VISITED_NODES: usize = 1_000_000;
pub const MAX_LOADED_ENTRIES: usize = 10_000_000;

const LEAF_ANCHOR_MASK: u64 = 0x7f;
const MIN_INTERNAL_CHILDREN: usize = 32;
const MAX_INTERNAL_CHILDREN: usize = 256;
const INTERNAL_ANCHOR_MASK: u64 = 0x1f;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRef {
    pub min_path: String,
    pub max_path: String,
    pub hash: FileHash,
    pub file_count: u64,
    pub serialized_bytes: u32,
    /// Zero names a leaf; every internal level increments it by one.
    pub height: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeV2Root {
    pub version: u32,
    pub total_files: u64,
    pub child: Option<RangeRef>,
}

/// Persisted/history wrapper for a Tree v2 semantic projection. History
/// metadata intentionally remains outside [`TreeV2Root::hash`], matching the
/// Tree v1 rule that a state has one identity regardless of author or time.
///
/// The canonical `OVR2` encoding is deliberately separate from the v1
/// FlatBuffer envelope. An old decoder fails closed instead of interpreting a
/// v2 root with v1 layout semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootNodeV2 {
    pub vault_id: String,
    pub created_ms: u64,
    pub tree: TreeV2Root,
    pub parent_hash: Option<FileHash>,
    pub device_id: String,
}

impl RootNodeV2 {
    pub fn new(
        vault_id: impl Into<String>,
        device_id: impl Into<String>,
        tree: TreeV2Root,
    ) -> Result<Self, ChunkError> {
        let root = Self {
            vault_id: vault_id.into(),
            created_ms: 0,
            tree,
            parent_hash: None,
            device_id: device_id.into(),
        };
        root.validate()?;
        Ok(root)
    }

    pub fn hash(&self) -> FileHash {
        self.tree.hash()
    }

    pub fn total_files(&self) -> u64 {
        self.tree.total_files
    }

    pub fn serialize(&self) -> Result<Vec<u8>, ChunkError> {
        self.validate()?;
        let mut capacity = STORED_ROOT_HEADER_BYTES
            .checked_add(self.vault_id.len())
            .and_then(|value| value.checked_add(self.device_id.len()))
            .ok_or_else(|| invalid("Tree v2 stored root length overflow"))?;
        if self.parent_hash.is_some() {
            capacity = capacity
                .checked_add(32)
                .ok_or_else(|| invalid("Tree v2 stored root length overflow"))?;
        }
        if let Some(child) = &self.tree.child {
            capacity = capacity
                .checked_add(2)
                .and_then(|value| value.checked_add(range_record_len(child)))
                .ok_or_else(|| invalid("Tree v2 stored root length overflow"))?;
        }
        if capacity > MAX_STORED_ROOT_BYTES {
            return Err(invalid("Tree v2 stored root exceeds the byte cap"));
        }

        let vault_len = u16::try_from(self.vault_id.len())
            .map_err(|_| invalid("Tree v2 vault id is too long"))?;
        let device_len = u16::try_from(self.device_id.len())
            .map_err(|_| invalid("Tree v2 device id is too long"))?;
        let mut flags = 0u8;
        if self.parent_hash.is_some() {
            flags |= 1;
        }
        if self.tree.child.is_some() {
            flags |= 2;
        }

        let mut output = Vec::with_capacity(capacity);
        output.extend_from_slice(ROOT_MAGIC);
        output.extend_from_slice(&TREE_VERSION.to_le_bytes());
        output.extend_from_slice(&self.created_ms.to_le_bytes());
        output.extend_from_slice(&self.tree.total_files.to_le_bytes());
        output.extend_from_slice(&vault_len.to_le_bytes());
        output.extend_from_slice(&device_len.to_le_bytes());
        output.push(flags);
        output.extend_from_slice(&[0; 3]);
        output.extend_from_slice(&self.hash());
        if let Some(parent) = self.parent_hash {
            output.extend_from_slice(&parent);
        }
        if let Some(child) = &self.tree.child {
            output.extend_from_slice(&child.height.to_le_bytes());
            encode_range_ref(child, &mut output)?;
        }
        output.extend_from_slice(self.vault_id.as_bytes());
        output.extend_from_slice(self.device_id.as_bytes());
        debug_assert_eq!(output.len(), capacity);
        Ok(output)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, ChunkError> {
        if bytes.len() < STORED_ROOT_HEADER_BYTES || bytes.len() > MAX_STORED_ROOT_BYTES {
            return Err(invalid("Tree v2 stored root length is invalid"));
        }
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != ROOT_MAGIC {
            return Err(invalid("unknown Tree v2 root magic"));
        }
        if reader.u32()? != TREE_VERSION {
            return Err(invalid("unsupported Tree v2 root version"));
        }
        let created_ms = reader.u64()?;
        let total_files = reader.u64()?;
        let vault_len = reader.u16()? as usize;
        let device_len = reader.u16()? as usize;
        let flags = reader.u8()?;
        if flags & !0b11 != 0 || reader.take(3)? != [0; 3] {
            return Err(invalid("Tree v2 stored root flags are invalid"));
        }
        let declared_hash: FileHash = reader
            .take(32)?
            .try_into()
            .map_err(|_| invalid("Tree v2 root hash is truncated"))?;
        let parent_hash = if flags & 1 != 0 {
            Some(
                reader
                    .take(32)?
                    .try_into()
                    .map_err(|_| invalid("Tree v2 parent hash is truncated"))?,
            )
        } else {
            None
        };
        let child = if flags & 2 != 0 {
            let height = reader.u16()?;
            Some(decode_range_ref(&mut reader, height)?)
        } else {
            None
        };
        if vault_len == 0 || vault_len > MAX_ROOT_ID_BYTES || device_len > MAX_ROOT_ID_BYTES {
            return Err(invalid("Tree v2 root identifier length is invalid"));
        }
        let vault_id = std::str::from_utf8(reader.take(vault_len)?)
            .map_err(|_| invalid("Tree v2 vault id is not UTF-8"))?
            .to_owned();
        let device_id = std::str::from_utf8(reader.take(device_len)?)
            .map_err(|_| invalid("Tree v2 device id is not UTF-8"))?
            .to_owned();
        reader.finish()?;

        let root = Self {
            vault_id,
            created_ms,
            tree: TreeV2Root {
                version: TREE_VERSION,
                total_files,
                child,
            },
            parent_hash,
            device_id,
        };
        root.validate()?;
        if root.hash() != declared_hash {
            return Err(invalid("Tree v2 declared root hash is invalid"));
        }
        Ok(root)
    }

    fn validate(&self) -> Result<(), ChunkError> {
        validate_root_shape(&self.tree)?;
        validate_root_id("vault", &self.vault_id, false)?;
        validate_root_id("device", &self.device_id, true)
    }
}

impl TreeV2Root {
    pub fn hash(&self) -> FileHash {
        let mut bytes = Vec::with_capacity(96);
        bytes.extend_from_slice(ROOT_MAGIC);
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(&self.total_files.to_le_bytes());
        match &self.child {
            Some(child) => {
                bytes.push(1);
                bytes.extend_from_slice(&child.height.to_le_bytes());
                encode_range_ref(child, &mut bytes)
                    .expect("validated Tree v2 root descriptor must encode");
            }
            None => bytes.push(0),
        }
        hash_bytes(&bytes)
    }

    pub fn height(&self) -> u16 {
        self.child.as_ref().map_or(0, |child| child.height)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct UpdateStats {
    pub leaves_loaded: u64,
    pub entries_loaded: u64,
    pub replacement_leaves: u64,
    pub internal_nodes_emitted: u64,
    pub resync_leaves_scanned: u64,
    pub reached_tree_end: bool,
}

#[derive(Debug, Clone)]
enum V2Node {
    Leaf(Vec<FileEntry>),
    Internal {
        child_height: u16,
        children: Vec<RangeRef>,
    },
}

#[derive(Debug, Clone, Copy)]
struct Segment {
    start: usize,
    end: usize,
    stable_end: bool,
}

#[derive(Default)]
struct TraversalBudget {
    nodes: usize,
    entries: usize,
}

impl TraversalBudget {
    fn node(&mut self, depth: u16) -> Result<(), ChunkError> {
        if depth > MAX_TREE_DEPTH {
            return Err(invalid("Tree v2 exceeds the depth limit"));
        }
        self.nodes = self.nodes.saturating_add(1);
        if self.nodes > MAX_VISITED_NODES {
            return Err(invalid("Tree v2 exceeds the node limit"));
        }
        Ok(())
    }

    fn entries(&mut self, count: usize) -> Result<(), ChunkError> {
        self.entries = self.entries.saturating_add(count);
        if self.entries > MAX_LOADED_ENTRIES {
            return Err(invalid("Tree v2 exceeds the entry limit"));
        }
        Ok(())
    }
}

/// Build the canonical Tree v2 projection from a flat semantic state.
pub async fn build_tree<S: ChunkStore>(
    store: &S,
    mut entries: Vec<FileEntry>,
) -> Result<TreeV2Root, ChunkError> {
    entries.sort();
    validate_entries(&entries)?;
    if entries.is_empty() {
        return Ok(empty_root());
    }

    let segments = segment_entries(&entries)?;
    let mut leaves = Vec::with_capacity(segments.len());
    for segment in segments {
        leaves.push(store_leaf(store, &entries[segment.start..segment.end]).await?);
    }
    root_from_leaves(store, leaves).await.map(|(root, _)| root)
}

/// Apply final per-path states while loading only the leaf range between the
/// first changed path and the next old canonical boundary that reappears.
/// Internal descriptor levels are cheap and rebuilt deterministically.
pub async fn update_tree<S: ChunkStore>(
    store: &S,
    root: &TreeV2Root,
    changed: &[FileEntry],
    deleted: &[String],
) -> Result<(TreeV2Root, UpdateStats), ChunkError> {
    validate_root_shape(root)?;
    for entry in changed {
        validate_path(&entry.path)?;
    }
    for path in deleted {
        validate_path(path)?;
    }
    if changed.is_empty() && deleted.is_empty() {
        return Ok((root.clone(), UpdateStats::default()));
    }

    let mut operation_paths: Vec<&str> = changed.iter().map(|entry| entry.path.as_str()).collect();
    operation_paths.extend(deleted.iter().map(String::as_str));
    operation_paths.sort_unstable();
    let first_path = operation_paths[0];
    let last_path = operation_paths[operation_paths.len() - 1];

    let mut budget = TraversalBudget::default();
    let leaves = collect_leaf_refs(store, root, &mut budget).await?;
    if leaves.is_empty() {
        let merged =
            crate::tree::merge_prefix_entries(Vec::new(), changed.to_vec(), deleted.to_vec());
        let replacement_leaves = segment_entries(&merged)?.len() as u64;
        let next = build_tree(store, merged).await?;
        return Ok((
            next,
            UpdateStats {
                replacement_leaves,
                reached_tree_end: true,
                ..UpdateStats::default()
            },
        ));
    }

    let start = leaves
        .partition_point(|range| range.max_path.as_str() < first_path)
        .min(leaves.len() - 1);
    let mut end = leaves
        .partition_point(|range| range.max_path.as_str() < last_path)
        .min(leaves.len() - 1)
        .max(start);
    let mut stats = UpdateStats::default();
    let loaded = load_leaf_span(store, &leaves[start..=end], &mut budget).await?;
    stats.leaves_loaded = (end - start + 1) as u64;
    stats.entries_loaded = loaded.len() as u64;
    let mut replacement_entries =
        crate::tree::merge_prefix_entries(loaded, changed.to_vec(), deleted.to_vec());
    let replacement_segments;
    loop {
        let segments = segment_entries(&replacement_entries)?;
        let reached_tree_end = end + 1 == leaves.len();
        let resynchronized = reached_tree_end
            || segments.last().is_some_and(|segment| {
                segment.stable_end
                    && replacement_entries
                        .get(segment.end.saturating_sub(1))
                        .is_some_and(|entry| entry.path == leaves[end].max_path)
            });
        if resynchronized {
            stats.reached_tree_end = reached_tree_end;
            stats.resync_leaves_scanned = (end - start + 1) as u64;
            replacement_segments = segments;
            break;
        }
        end += 1;
        let appended =
            load_leaf_span(store, std::slice::from_ref(&leaves[end]), &mut budget).await?;
        stats.leaves_loaded = stats.leaves_loaded.saturating_add(1);
        stats.entries_loaded = stats.entries_loaded.saturating_add(appended.len() as u64);
        replacement_entries.extend(appended);
    }

    let mut replacement_refs = Vec::with_capacity(replacement_segments.len());
    for segment in replacement_segments {
        replacement_refs
            .push(store_leaf(store, &replacement_entries[segment.start..segment.end]).await?);
    }
    stats.replacement_leaves = replacement_refs.len() as u64;

    let mut next_leaves =
        Vec::with_capacity(leaves.len() - (end - start + 1) + replacement_refs.len());
    next_leaves.extend_from_slice(&leaves[..start]);
    next_leaves.append(&mut replacement_refs);
    next_leaves.extend_from_slice(&leaves[end + 1..]);
    validate_ranges(&next_leaves, 0)?;

    let (next, internal_nodes) = root_from_leaves(store, next_leaves).await?;
    stats.internal_nodes_emitted = internal_nodes as u64;
    Ok((next, stats))
}

/// Recursively compare range nodes. Equal child hashes are skipped before any
/// leaf entries are materialized; boundary drift is isolated until the next
/// common range end.
pub async fn compute_deltas_with_stats<S: ChunkStore>(
    store: &S,
    from: &TreeV2Root,
    to: &TreeV2Root,
) -> Result<DiffResult, ChunkError> {
    validate_root_shape(from)?;
    validate_root_shape(to)?;
    let mut stats = DiffStats {
        nodes_visited: 1,
        ..DiffStats::default()
    };
    if from.hash() == to.hash() {
        stats.nodes_skipped = 1;
        return Ok(DiffResult {
            deltas: Vec::new(),
            stats,
        });
    }

    let mut raw = Vec::new();
    let mut budget = TraversalBudget::default();
    let mut tasks = vec![(
        from.child.iter().cloned().collect::<Vec<_>>(),
        to.child.iter().cloned().collect::<Vec<_>>(),
    )];
    while let Some((from_ranges, to_ranges)) = tasks.pop() {
        for (left, right) in align_range_windows(&from_ranges, &to_ranges)
            .into_iter()
            .rev()
        {
            stats.nodes_visited = stats
                .nodes_visited
                .saturating_add((left.len() + right.len()) as u64);
            if left.len() == 1
                && right.len() == 1
                && left[0].min_path == right[0].min_path
                && left[0].max_path == right[0].max_path
                && left[0].hash == right[0].hash
            {
                stats.nodes_skipped = stats.nodes_skipped.saturating_add(1);
                continue;
            }

            let can_expand = left.iter().chain(&right).any(|range| range.height > 0);
            if can_expand {
                let expanded_left = expand_one_level(store, left, &mut budget).await?;
                let expanded_right = expand_one_level(store, right, &mut budget).await?;
                tasks.push((expanded_left, expanded_right));
                continue;
            }

            let from_entries = load_leaf_span(store, &left, &mut budget).await?;
            let to_entries = load_leaf_span(store, &right, &mut budget).await?;
            stats.entries_materialized = stats
                .entries_materialized
                .saturating_add((from_entries.len() + to_entries.len()) as u64);
            diff::diff_entries(&from_entries, &to_entries, &mut raw);
        }
    }

    raw.sort_by(|left, right| delta_path(left).cmp(delta_path(right)));
    Ok(DiffResult {
        deltas: diff::detect_renames(raw),
        stats,
    })
}

pub async fn compute_deltas<S: ChunkStore>(
    store: &S,
    from: &TreeV2Root,
    to: &TreeV2Root,
) -> Result<Vec<FileDelta>, ChunkError> {
    Ok(compute_deltas_with_stats(store, from, to).await?.deltas)
}

/// Load one bounded semantic page after an exact path cursor. This proves the
/// prototype can drive a future diff iterator without flattening the tree.
pub async fn range_page<S: ChunkStore>(
    store: &S,
    root: &TreeV2Root,
    after_path: Option<&str>,
    max_entries: usize,
) -> Result<Vec<FileEntry>, ChunkError> {
    if max_entries == 0 || max_entries > 8_192 {
        return Err(invalid("Tree v2 range page limit is invalid"));
    }
    if let Some(path) = after_path {
        validate_path(path)?;
    }
    let mut budget = TraversalBudget::default();
    let leaves = collect_leaf_refs(store, root, &mut budget).await?;
    let mut output = Vec::with_capacity(max_entries);
    for range in leaves {
        if after_path.is_some_and(|path| range.max_path.as_str() <= path) {
            continue;
        }
        let entries = load_leaf_span(store, std::slice::from_ref(&range), &mut budget).await?;
        for entry in entries {
            if after_path.is_some_and(|path| entry.path.as_str() <= path) {
                continue;
            }
            output.push(entry);
            if output.len() == max_entries {
                return Ok(output);
            }
        }
    }
    Ok(output)
}

pub async fn load_all_entries<S: ChunkStore>(
    store: &S,
    root: &TreeV2Root,
) -> Result<Vec<FileEntry>, ChunkError> {
    let mut budget = TraversalBudget::default();
    let leaves = collect_leaf_refs(store, root, &mut budget).await?;
    let entries = load_leaf_span(store, &leaves, &mut budget).await?;
    if entries.len() as u64 != root.total_files {
        return Err(invalid("Tree v2 root total does not match its leaves"));
    }
    Ok(entries)
}

/// Expose immutable range descriptors for diagnostics/property benchmarks.
/// Leaf bytes themselves remain lazy.
pub async fn leaf_ranges<S: ChunkStore>(
    store: &S,
    root: &TreeV2Root,
) -> Result<Vec<RangeRef>, ChunkError> {
    let mut budget = TraversalBudget::default();
    collect_leaf_refs(store, root, &mut budget).await
}

pub async fn reachable_hashes<S: ChunkStore>(
    store: &S,
    root: &TreeV2Root,
) -> Result<HashSet<FileHash>, ChunkError> {
    validate_root_shape(root)?;
    let mut output = HashSet::new();
    let mut budget = TraversalBudget::default();
    let mut pending: Vec<RangeRef> = root.child.iter().cloned().collect();
    while let Some(range) = pending.pop() {
        if !output.insert(range.hash) {
            continue;
        }
        budget.node(range.height)?;
        match load_node(store, &range).await? {
            V2Node::Leaf(entries) => budget.entries(entries.len())?,
            V2Node::Internal { children, .. } => pending.extend(children),
        }
    }
    Ok(output)
}

async fn root_from_leaves<S: ChunkStore>(
    store: &S,
    mut ranges: Vec<RangeRef>,
) -> Result<(TreeV2Root, usize), ChunkError> {
    if ranges.is_empty() {
        return Ok((empty_root(), 0));
    }
    validate_ranges(&ranges, 0)?;
    let total_files = ranges.iter().try_fold(0u64, |sum, range| {
        sum.checked_add(range.file_count)
            .ok_or_else(|| invalid("Tree v2 file count overflow"))
    })?;
    let mut emitted = 0usize;
    while ranges.len() > 1 {
        let child_height = ranges[0].height;
        if child_height >= MAX_TREE_DEPTH {
            return Err(invalid("Tree v2 exceeds the depth limit"));
        }
        validate_ranges(&ranges, child_height)?;
        let segments = segment_ranges(&ranges)?;
        let mut parents = Vec::with_capacity(segments.len());
        for segment in segments {
            parents.push(
                store_internal(store, child_height, &ranges[segment.start..segment.end]).await?,
            );
            emitted += 1;
        }
        ranges = parents;
    }
    let child = ranges.pop().expect("non-empty Tree v2 level");
    Ok((
        TreeV2Root {
            version: TREE_VERSION,
            total_files,
            child: Some(child),
        },
        emitted,
    ))
}

fn empty_root() -> TreeV2Root {
    TreeV2Root {
        version: TREE_VERSION,
        total_files: 0,
        child: None,
    }
}

fn delta_path(delta: &FileDelta) -> &str {
    match delta {
        FileDelta::Added { path, .. }
        | FileDelta::Modified { path, .. }
        | FileDelta::Deleted { path, .. }
        | FileDelta::Renamed { path, .. } => path,
    }
}

async fn store_leaf<S: ChunkStore>(
    store: &S,
    entries: &[FileEntry],
) -> Result<RangeRef, ChunkError> {
    let bytes = encode_leaf(entries)?;
    let hash = hash_bytes(&bytes);
    let serialized_bytes = u32::try_from(bytes.len())
        .map_err(|_| invalid("Tree v2 leaf length does not fit its descriptor"))?;
    store.put(hash, bytes).await?;
    Ok(RangeRef {
        min_path: entries[0].path.clone(),
        max_path: entries[entries.len() - 1].path.clone(),
        hash,
        file_count: entries.len() as u64,
        serialized_bytes,
        height: 0,
    })
}

async fn store_internal<S: ChunkStore>(
    store: &S,
    child_height: u16,
    children: &[RangeRef],
) -> Result<RangeRef, ChunkError> {
    let bytes = encode_internal(child_height, children)?;
    let hash = hash_bytes(&bytes);
    let serialized_bytes = u32::try_from(bytes.len())
        .map_err(|_| invalid("Tree v2 internal length does not fit its descriptor"))?;
    store.put(hash, bytes).await?;
    let file_count = children.iter().try_fold(0u64, |sum, child| {
        sum.checked_add(child.file_count)
            .ok_or_else(|| invalid("Tree v2 file count overflow"))
    })?;
    Ok(RangeRef {
        min_path: children[0].min_path.clone(),
        max_path: children[children.len() - 1].max_path.clone(),
        hash,
        file_count,
        serialized_bytes,
        height: child_height + 1,
    })
}

fn encode_leaf(entries: &[FileEntry]) -> Result<Vec<u8>, ChunkError> {
    validate_entries(entries)?;
    if entries.len() > MAX_LEAF_ENTRIES {
        return Err(invalid("Tree v2 leaf exceeds the entry cap"));
    }
    let capacity = entries.iter().try_fold(LEAF_HEADER_BYTES, |sum, entry| {
        sum.checked_add(leaf_record_len(entry))
            .ok_or_else(|| invalid("Tree v2 leaf length overflow"))
    })?;
    if capacity > MAX_NODE_BYTES {
        return Err(invalid("Tree v2 leaf exceeds the byte cap"));
    }
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(LEAF_MAGIC);
    output.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        output.extend_from_slice(&(entry.path.len() as u16).to_le_bytes());
        output.extend_from_slice(entry.path.as_bytes());
        output.extend_from_slice(&entry.hash);
        output.extend_from_slice(&entry.mtime_ms.to_le_bytes());
        output.extend_from_slice(&entry.size_bytes.to_le_bytes());
    }
    Ok(output)
}

fn encode_internal(child_height: u16, children: &[RangeRef]) -> Result<Vec<u8>, ChunkError> {
    if children.is_empty() || children.len() > MAX_INTERNAL_CHILDREN {
        return Err(invalid("Tree v2 internal child count is invalid"));
    }
    validate_ranges(children, child_height)?;
    let capacity = children
        .iter()
        .try_fold(INTERNAL_HEADER_BYTES, |sum, child| {
            sum.checked_add(range_record_len(child))
                .ok_or_else(|| invalid("Tree v2 internal length overflow"))
        })?;
    if capacity > MAX_NODE_BYTES {
        return Err(invalid("Tree v2 internal node exceeds the byte cap"));
    }
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(INTERNAL_MAGIC);
    output.extend_from_slice(&child_height.to_le_bytes());
    output.extend_from_slice(&0u16.to_le_bytes());
    output.extend_from_slice(&(children.len() as u32).to_le_bytes());
    for child in children {
        encode_range_ref(child, &mut output)?;
    }
    Ok(output)
}

fn decode_node(bytes: &[u8]) -> Result<V2Node, ChunkError> {
    if bytes.len() > MAX_NODE_BYTES || bytes.len() < LEAF_HEADER_BYTES {
        return Err(invalid("Tree v2 node length is invalid"));
    }
    if bytes.get(..4) == Some(LEAF_MAGIC.as_slice()) {
        let mut reader = Reader::new(bytes);
        reader.take(4)?;
        let count = reader.u32()? as usize;
        if count == 0 || count > MAX_LEAF_ENTRIES {
            return Err(invalid("Tree v2 leaf entry count is invalid"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let path_len = reader.u16()? as usize;
            if path_len == 0 || path_len > MAX_PATH_BYTES {
                return Err(invalid("Tree v2 leaf path length is invalid"));
            }
            let path = std::str::from_utf8(reader.take(path_len)?)
                .map_err(|_| invalid("Tree v2 leaf path is not UTF-8"))?
                .to_owned();
            validate_path(&path)?;
            let hash: FileHash = reader
                .take(32)?
                .try_into()
                .map_err(|_| invalid("Tree v2 leaf hash is truncated"))?;
            let mtime_ms = reader.u64()?;
            let size_bytes = reader.u64()?;
            entries.push(FileEntry::new(path, hash, mtime_ms, size_bytes));
        }
        reader.finish()?;
        validate_entries(&entries)?;
        return Ok(V2Node::Leaf(entries));
    }
    if bytes.get(..4) == Some(INTERNAL_MAGIC.as_slice()) {
        if bytes.len() < INTERNAL_HEADER_BYTES {
            return Err(invalid("Tree v2 internal header is truncated"));
        }
        let mut reader = Reader::new(bytes);
        reader.take(4)?;
        let child_height = reader.u16()?;
        if child_height >= MAX_TREE_DEPTH || reader.u16()? != 0 {
            return Err(invalid("Tree v2 internal header is invalid"));
        }
        let count = reader.u32()? as usize;
        if count == 0 || count > MAX_INTERNAL_CHILDREN {
            return Err(invalid("Tree v2 internal child count is invalid"));
        }
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            children.push(decode_range_ref(&mut reader, child_height)?);
        }
        reader.finish()?;
        validate_ranges(&children, child_height)?;
        return Ok(V2Node::Internal {
            child_height,
            children,
        });
    }
    Err(invalid("unknown Tree v2 node magic"))
}

async fn load_node<S: ChunkStore>(store: &S, range: &RangeRef) -> Result<V2Node, ChunkError> {
    validate_range(range)?;
    let bytes = store.get(&range.hash).await?;
    if bytes.len() != range.serialized_bytes as usize || hash_bytes(&bytes) != range.hash {
        return Err(invalid("Tree v2 child failed content-address validation"));
    }
    let node = decode_node(&bytes)?;
    let actual = descriptor_for_node(range.hash, bytes.len(), &node)?;
    if actual != *range {
        return Err(invalid("Tree v2 child descriptor does not match its bytes"));
    }
    Ok(node)
}

fn descriptor_for_node(
    hash: FileHash,
    serialized_bytes: usize,
    node: &V2Node,
) -> Result<RangeRef, ChunkError> {
    match node {
        V2Node::Leaf(entries) => Ok(RangeRef {
            min_path: entries[0].path.clone(),
            max_path: entries[entries.len() - 1].path.clone(),
            hash,
            file_count: entries.len() as u64,
            serialized_bytes: serialized_bytes as u32,
            height: 0,
        }),
        V2Node::Internal {
            child_height,
            children,
        } => Ok(RangeRef {
            min_path: children[0].min_path.clone(),
            max_path: children[children.len() - 1].max_path.clone(),
            hash,
            file_count: children.iter().try_fold(0u64, |sum, child| {
                sum.checked_add(child.file_count)
                    .ok_or_else(|| invalid("Tree v2 file count overflow"))
            })?,
            serialized_bytes: serialized_bytes as u32,
            height: child_height + 1,
        }),
    }
}

async fn collect_leaf_refs<S: ChunkStore>(
    store: &S,
    root: &TreeV2Root,
    budget: &mut TraversalBudget,
) -> Result<Vec<RangeRef>, ChunkError> {
    validate_root_shape(root)?;
    let mut pending: Vec<RangeRef> = root.child.iter().cloned().collect();
    let mut leaves = Vec::new();
    while let Some(range) = pending.pop() {
        budget.node(range.height)?;
        if range.height == 0 {
            leaves.push(range);
            continue;
        }
        match load_node(store, &range).await? {
            V2Node::Internal { children, .. } => pending.extend(children.into_iter().rev()),
            V2Node::Leaf(_) => {
                return Err(invalid("Tree v2 internal descriptor names a leaf"));
            }
        }
    }
    validate_ranges(&leaves, 0)?;
    let total = leaves.iter().try_fold(0u64, |sum, leaf| {
        sum.checked_add(leaf.file_count)
            .ok_or_else(|| invalid("Tree v2 file count overflow"))
    })?;
    if total != root.total_files {
        return Err(invalid("Tree v2 root total does not match its ranges"));
    }
    Ok(leaves)
}

async fn load_leaf_span<S: ChunkStore>(
    store: &S,
    leaves: &[RangeRef],
    budget: &mut TraversalBudget,
) -> Result<Vec<FileEntry>, ChunkError> {
    for leaf in leaves {
        validate_range(leaf)?;
        if leaf.height != 0 {
            return Err(invalid("Tree v2 leaf span contains an internal node"));
        }
    }
    let capacity = leaves.iter().try_fold(0usize, |sum, leaf| {
        let count = usize::try_from(leaf.file_count)
            .map_err(|_| invalid("Tree v2 leaf count does not fit this platform"))?;
        sum.checked_add(count)
            .ok_or_else(|| invalid("Tree v2 entry capacity overflow"))
    })?;
    let mut output = Vec::with_capacity(capacity);
    for leaf in leaves {
        budget.node(0)?;
        let V2Node::Leaf(entries) = load_node(store, leaf).await? else {
            return Err(invalid("Tree v2 leaf descriptor names an internal node"));
        };
        budget.entries(entries.len())?;
        output.extend(entries);
    }
    validate_entries(&output)?;
    Ok(output)
}

async fn expand_one_level<S: ChunkStore>(
    store: &S,
    ranges: Vec<RangeRef>,
    budget: &mut TraversalBudget,
) -> Result<Vec<RangeRef>, ChunkError> {
    let mut output = Vec::new();
    for range in ranges {
        if range.height == 0 {
            output.push(range);
            continue;
        }
        budget.node(range.height)?;
        let V2Node::Internal { children, .. } = load_node(store, &range).await? else {
            return Err(invalid("Tree v2 internal descriptor names a leaf"));
        };
        output.extend(children);
    }
    Ok(output)
}

fn align_range_windows(from: &[RangeRef], to: &[RangeRef]) -> Vec<(Vec<RangeRef>, Vec<RangeRef>)> {
    let mut output = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < from.len() && j < to.len() {
        if from[i].min_path == to[j].min_path && from[i].max_path == to[j].max_path {
            output.push((vec![from[i].clone()], vec![to[j].clone()]));
            i += 1;
            j += 1;
            continue;
        }
        let start_i = i;
        let start_j = j;
        loop {
            match from[i].max_path.cmp(&to[j].max_path) {
                std::cmp::Ordering::Equal => {
                    i += 1;
                    j += 1;
                    break;
                }
                std::cmp::Ordering::Less => {
                    i += 1;
                    if i == from.len() {
                        j = to.len();
                        break;
                    }
                }
                std::cmp::Ordering::Greater => {
                    j += 1;
                    if j == to.len() {
                        i = from.len();
                        break;
                    }
                }
            }
        }
        output.push((from[start_i..i].to_vec(), to[start_j..j].to_vec()));
    }
    if i < from.len() || j < to.len() {
        output.push((from[i..].to_vec(), to[j..].to_vec()));
    }
    output
}

fn segment_entries(entries: &[FileEntry]) -> Result<Vec<Segment>, ChunkError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    validate_entries(entries)?;
    segment_by(
        entries.len(),
        LEAF_HEADER_BYTES,
        MIN_LEAF_ENTRIES,
        MAX_LEAF_ENTRIES,
        MIN_LEAF_BYTES,
        LEAF_ANCHOR_MASK,
        |index| Ok(leaf_record_len(&entries[index])),
        |index| entries[index].path.as_bytes(),
    )
}

fn segment_ranges(ranges: &[RangeRef]) -> Result<Vec<Segment>, ChunkError> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    segment_by(
        ranges.len(),
        INTERNAL_HEADER_BYTES,
        MIN_INTERNAL_CHILDREN,
        MAX_INTERNAL_CHILDREN,
        0,
        INTERNAL_ANCHOR_MASK,
        |index| Ok(range_record_len(&ranges[index])),
        |index| ranges[index].max_path.as_bytes(),
    )
}

#[allow(clippy::too_many_arguments)]
fn segment_by<'a>(
    count: usize,
    header_bytes: usize,
    min_records: usize,
    max_records: usize,
    min_bytes: usize,
    anchor_mask: u64,
    mut record_len: impl FnMut(usize) -> Result<usize, ChunkError>,
    mut key: impl FnMut(usize) -> &'a [u8],
) -> Result<Vec<Segment>, ChunkError> {
    let mut output = Vec::new();
    let mut start = 0usize;
    let mut bytes = header_bytes;
    for index in 0..count {
        let length = record_len(index)?;
        if header_bytes.saturating_add(length) > MAX_NODE_BYTES {
            return Err(invalid("one Tree v2 record exceeds the node cap"));
        }
        if index > start
            && (index - start >= max_records || bytes.saturating_add(length) > MAX_NODE_BYTES)
        {
            output.push(Segment {
                start,
                end: index,
                stable_end: true,
            });
            start = index;
            bytes = header_bytes;
        }
        bytes = bytes
            .checked_add(length)
            .ok_or_else(|| invalid("Tree v2 segment length overflow"))?;
        let records = index - start + 1;
        let natural = records >= min_records
            && bytes >= min_bytes
            && gear_hash(key(index)) & anchor_mask == 0;
        let forced = records >= max_records || bytes == MAX_NODE_BYTES;
        if natural || forced {
            output.push(Segment {
                start,
                end: index + 1,
                stable_end: true,
            });
            start = index + 1;
            bytes = header_bytes;
        }
    }
    if start < count {
        output.push(Segment {
            start,
            end: count,
            stable_end: false,
        });
    }
    Ok(output)
}

/// Gear-style rolling fingerprint with a generated, stable 256-word table.
/// Only path bytes participate, never content hash, mtime, or size.
pub fn gear_hash(path: &[u8]) -> u64 {
    path.iter().fold(0u64, |state, byte| {
        state
            .rotate_left(1)
            .wrapping_add(gear_word(u64::from(*byte)))
    })
}

fn gear_word(value: u64) -> u64 {
    let mut mixed = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^ (mixed >> 31)
}

fn validate_root_shape(root: &TreeV2Root) -> Result<(), ChunkError> {
    if root.version != TREE_VERSION {
        return Err(invalid("unsupported Tree v2 root version"));
    }
    match &root.child {
        None if root.total_files == 0 => Ok(()),
        Some(child)
            if root.total_files > 0
                && child.file_count == root.total_files
                && child.height <= MAX_TREE_DEPTH =>
        {
            validate_range(child)
        }
        _ => Err(invalid("Tree v2 root shape is inconsistent")),
    }
}

fn validate_entries(entries: &[FileEntry]) -> Result<(), ChunkError> {
    if entries.len() > MAX_LOADED_ENTRIES {
        return Err(invalid("Tree v2 exceeds the entry limit"));
    }
    for entry in entries {
        validate_path(&entry.path)?;
    }
    if entries.windows(2).any(|pair| pair[0].path >= pair[1].path) {
        return Err(invalid("Tree v2 entries are not strictly ordered"));
    }
    Ok(())
}

fn validate_ranges(ranges: &[RangeRef], expected_height: u16) -> Result<(), ChunkError> {
    for range in ranges {
        if range.height != expected_height {
            return Err(invalid("Tree v2 range height is inconsistent"));
        }
        validate_range(range)?;
    }
    if ranges
        .windows(2)
        .any(|pair| pair[0].max_path >= pair[1].min_path || pair[0].max_path >= pair[1].max_path)
    {
        return Err(invalid("Tree v2 ranges overlap or are unordered"));
    }
    Ok(())
}

fn validate_range(range: &RangeRef) -> Result<(), ChunkError> {
    validate_path(&range.min_path)?;
    validate_path(&range.max_path)?;
    if range.min_path > range.max_path
        || range.file_count == 0
        || range.file_count > MAX_LOADED_ENTRIES as u64
        || (range.height == 0 && range.file_count > MAX_LEAF_ENTRIES as u64)
        || range.serialized_bytes == 0
        || range.serialized_bytes as usize > MAX_NODE_BYTES
        || range.height > MAX_TREE_DEPTH
    {
        return Err(invalid("Tree v2 range descriptor is invalid"));
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<(), ChunkError> {
    if path.is_empty() || path.len() > MAX_PATH_BYTES || path.starts_with('/') {
        return Err(invalid("Tree v2 path length is invalid"));
    }
    if path.contains('\\')
        || path
            .chars()
            .any(|character| character.is_control() || character == '\u{7f}')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(invalid("unsafe Tree v2 vault-relative path"));
    }
    Ok(())
}

fn validate_root_id(kind: &str, value: &str, allow_empty: bool) -> Result<(), ChunkError> {
    if (!allow_empty && value.is_empty())
        || value.len() > MAX_ROOT_ID_BYTES
        || value
            .chars()
            .any(|character| character.is_control() || character == '\u{7f}')
    {
        return Err(invalid(format!("Tree v2 {kind} id is invalid")));
    }
    Ok(())
}

fn leaf_record_len(entry: &FileEntry) -> usize {
    2 + entry.path.len() + 32 + 8 + 8
}

fn range_record_len(range: &RangeRef) -> usize {
    2 + 2 + range.min_path.len() + range.max_path.len() + 32 + 8 + 4
}

fn encode_range_ref(range: &RangeRef, output: &mut Vec<u8>) -> Result<(), ChunkError> {
    validate_range(range)?;
    let min_len = u16::try_from(range.min_path.len())
        .map_err(|_| invalid("Tree v2 minimum path is too long"))?;
    let max_len = u16::try_from(range.max_path.len())
        .map_err(|_| invalid("Tree v2 maximum path is too long"))?;
    output.extend_from_slice(&min_len.to_le_bytes());
    output.extend_from_slice(&max_len.to_le_bytes());
    output.extend_from_slice(range.min_path.as_bytes());
    output.extend_from_slice(range.max_path.as_bytes());
    output.extend_from_slice(&range.hash);
    output.extend_from_slice(&range.file_count.to_le_bytes());
    output.extend_from_slice(&range.serialized_bytes.to_le_bytes());
    Ok(())
}

fn decode_range_ref(reader: &mut Reader<'_>, height: u16) -> Result<RangeRef, ChunkError> {
    let min_len = reader.u16()? as usize;
    let max_len = reader.u16()? as usize;
    if min_len == 0 || min_len > MAX_PATH_BYTES || max_len == 0 || max_len > MAX_PATH_BYTES {
        return Err(invalid("Tree v2 range path length is invalid"));
    }
    let min_path = std::str::from_utf8(reader.take(min_len)?)
        .map_err(|_| invalid("Tree v2 minimum path is not UTF-8"))?
        .to_owned();
    let max_path = std::str::from_utf8(reader.take(max_len)?)
        .map_err(|_| invalid("Tree v2 maximum path is not UTF-8"))?
        .to_owned();
    let hash = reader
        .take(32)?
        .try_into()
        .map_err(|_| invalid("Tree v2 range hash is truncated"))?;
    let file_count = reader.u64()?;
    let serialized_bytes = reader.u32()?;
    let range = RangeRef {
        min_path,
        max_path,
        hash,
        file_count,
        serialized_bytes,
        height,
    };
    validate_range(&range)?;
    Ok(range)
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ChunkError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid("Tree v2 node is truncated"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16, ChunkError> {
        Ok(u16::from_le_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| invalid("Tree v2 u16 is truncated"))?,
        ))
    }

    fn u8(&mut self) -> Result<u8, ChunkError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ChunkError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| invalid("Tree v2 u32 is truncated"))?,
        ))
    }

    fn u64(&mut self) -> Result<u64, ChunkError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| invalid("Tree v2 u64 is truncated"))?,
        ))
    }

    fn finish(self) -> Result<(), ChunkError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("Tree v2 node has trailing bytes"))
        }
    }
}

fn invalid(message: impl Into<String>) -> ChunkError {
    ChunkError::Deserialize(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryChunkStore;
    use std::collections::BTreeMap;

    fn entry(path: &str, revision: u64) -> FileEntry {
        FileEntry::new(
            path.to_owned(),
            hash_bytes(format!("{path}:{revision}").as_bytes()),
            1_800_000_000_000 + revision,
            1_024 + revision % 8_192,
        )
    }

    fn entries(count: usize) -> Vec<FileEntry> {
        (0..count)
            .map(|index| entry(&format!("wide/{index:08}.md"), index as u64))
            .collect()
    }

    async fn v1_entries(store: &MemoryChunkStore, root: &crate::chunk::RootNode) -> Vec<FileEntry> {
        let mut output = Vec::new();
        for (_, hash) in &root.children {
            output.extend(crate::tree::load_all_entries(store, hash).await.unwrap());
        }
        output.sort();
        output
    }

    #[derive(Clone, Copy)]
    struct TestRng(u64);

    impl TestRng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }

        fn below(&mut self, upper: usize) -> usize {
            (self.next() as usize) % upper
        }
    }

    #[tokio::test]
    async fn build_is_deterministic_and_boundaries_ignore_metadata() {
        // This pins the generated Gear table and update rule. Changing it would
        // silently move every content-defined boundary and must be treated as
        // an on-disk format change.
        assert_eq!(gear_hash(b"notes/2026/a.md"), 0x9fb1_f98a_76cf_7a30);

        let original = entries(8_000);
        let mut reversed = original.clone();
        reversed.reverse();
        let first_store = MemoryChunkStore::new();
        let first = build_tree(&first_store, original.clone()).await.unwrap();
        let second_store = MemoryChunkStore::new();
        let second = build_tree(&second_store, reversed).await.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.hash(), second.hash());

        let first_ranges = leaf_ranges(&first_store, &first).await.unwrap();
        assert!(first_ranges.len() > 4);
        assert!(first_ranges.iter().all(|range| {
            range.file_count <= MAX_LEAF_ENTRIES as u64
                && range.serialized_bytes as usize <= MAX_NODE_BYTES
        }));

        let mut metadata_changed = original;
        let middle = metadata_changed.len() / 2;
        metadata_changed[middle].hash = hash_bytes(b"different content");
        metadata_changed[middle].mtime_ms += 10_000;
        metadata_changed[middle].size_bytes += 55;
        let changed_store = MemoryChunkStore::new();
        let changed = build_tree(&changed_store, metadata_changed).await.unwrap();
        let changed_ranges = leaf_ranges(&changed_store, &changed).await.unwrap();
        let boundaries = |ranges: &[RangeRef]| {
            ranges
                .iter()
                .map(|range| {
                    (
                        range.min_path.clone(),
                        range.max_path.clone(),
                        range.file_count,
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(boundaries(&first_ranges), boundaries(&changed_ranges));
        assert_ne!(first.hash(), changed.hash());
    }

    #[tokio::test]
    async fn incremental_update_and_diff_match_v1_and_flat_oracles() {
        let initial = entries(2_000);
        let mut expected: BTreeMap<String, FileEntry> = initial
            .iter()
            .cloned()
            .map(|entry| (entry.path.clone(), entry))
            .collect();
        let v1_store = MemoryChunkStore::new();
        let mut v1_root = crate::tree::build_tree(&v1_store, initial.clone(), "v", "d")
            .await
            .unwrap();
        let v2_store = MemoryChunkStore::new();
        let mut v2_root = build_tree(&v2_store, initial).await.unwrap();
        let mut rng = TestRng(0x7472_6565_2d76_3201);

        for batch in 0..250u64 {
            let before_v1 = v1_root.clone();
            let before_v2 = v2_root.clone();
            let mut changed = Vec::new();
            let mut deleted = Vec::new();
            for operation in 0..(1 + rng.below(8)) {
                let index = rng.below(3_000);
                let path = format!("wide/{index:08}.md");
                if rng.next() & 3 == 0 {
                    deleted.push(path);
                } else {
                    changed.push(entry(&path, batch * 100 + operation as u64 + 50_000));
                }
            }

            for path in &deleted {
                expected.remove(path);
            }
            for item in &changed {
                expected.insert(item.path.clone(), item.clone());
            }
            v1_root = crate::tree::update_tree(&v1_store, &v1_root, &changed, &deleted)
                .await
                .unwrap();
            v2_root = update_tree(&v2_store, &v2_root, &changed, &deleted)
                .await
                .unwrap()
                .0;

            let oracle_store = MemoryChunkStore::new();
            let oracle = build_tree(&oracle_store, expected.values().cloned().collect())
                .await
                .unwrap();
            assert_eq!(
                v2_root.hash(),
                oracle.hash(),
                "root mismatch at batch {batch}"
            );
            assert_eq!(
                load_all_entries(&v2_store, &v2_root).await.unwrap(),
                expected.values().cloned().collect::<Vec<_>>(),
                "flat state mismatch at batch {batch}",
            );
            assert_eq!(
                v1_entries(&v1_store, &v1_root).await,
                expected.values().cloned().collect::<Vec<_>>(),
                "v1 semantic mismatch at batch {batch}",
            );
            assert_eq!(
                crate::diff::compute_deltas(&v1_store, &before_v1, &v1_root)
                    .await
                    .unwrap(),
                compute_deltas(&v2_store, &before_v2, &v2_root)
                    .await
                    .unwrap(),
                "v1/v2 diff mismatch at batch {batch}",
            );
        }
    }

    #[tokio::test]
    async fn one_content_edit_has_bounded_churn_and_recursive_diff() {
        let initial = entries(20_000);
        let store = MemoryChunkStore::new();
        let before = build_tree(&store, initial.clone()).await.unwrap();
        let before_ranges = leaf_ranges(&store, &before).await.unwrap();
        let changed = entry("wide/00010000.md", 999_999);
        let (after, update_stats) =
            update_tree(&store, &before, std::slice::from_ref(&changed), &[])
                .await
                .unwrap();
        assert_eq!(update_stats.leaves_loaded, 1);
        assert!(update_stats.entries_loaded <= MAX_LEAF_ENTRIES as u64);

        let after_ranges = leaf_ranges(&store, &after).await.unwrap();
        assert_eq!(before_ranges.len(), after_ranges.len());
        assert_eq!(
            before_ranges
                .iter()
                .zip(&after_ranges)
                .filter(|(left, right)| left.hash != right.hash)
                .count(),
            1,
        );
        let before_reachable = reachable_hashes(&store, &before).await.unwrap();
        let after_reachable = reachable_hashes(&store, &after).await.unwrap();
        let new_nodes = after_reachable.difference(&before_reachable).count();
        assert!(new_nodes <= usize::from(before.height()) + 1);

        let v2_diff = compute_deltas_with_stats(&store, &before, &after)
            .await
            .unwrap();
        assert_eq!(v2_diff.deltas.len(), 1);
        assert!(v2_diff.stats.entries_materialized <= 2 * MAX_LEAF_ENTRIES as u64);

        let v1_store = MemoryChunkStore::new();
        let v1_before = crate::tree::build_tree(&v1_store, initial, "v", "d")
            .await
            .unwrap();
        let v1_after =
            crate::tree::update_tree(&v1_store, &v1_before, std::slice::from_ref(&changed), &[])
                .await
                .unwrap();
        let v1_diff = crate::diff::compute_deltas_with_stats(&v1_store, &v1_before, &v1_after)
            .await
            .unwrap();
        assert_eq!(v1_diff.deltas, v2_diff.deltas);
        assert!(v1_diff.stats.entries_materialized > v2_diff.stats.entries_materialized * 10);
    }

    #[tokio::test]
    async fn deleting_a_boundary_resynchronizes_with_a_fresh_build() {
        let initial = entries(20_000);
        let store = MemoryChunkStore::new();
        let before = build_tree(&store, initial.clone()).await.unwrap();
        let ranges = leaf_ranges(&store, &before).await.unwrap();
        let deleted = vec![ranges[2].max_path.clone()];
        let (after, stats) = update_tree(&store, &before, &[], &deleted).await.unwrap();
        let expected: Vec<_> = initial
            .into_iter()
            .filter(|entry| entry.path != deleted[0])
            .collect();
        let oracle_store = MemoryChunkStore::new();
        let oracle = build_tree(&oracle_store, expected).await.unwrap();
        assert_eq!(after.hash(), oracle.hash());
        assert!(stats.resync_leaves_scanned >= 2);
        assert!(stats.resync_leaves_scanned < ranges.len() as u64);
    }

    #[tokio::test]
    async fn distant_rename_matches_v1_canonical_delta() {
        let initial = entries(5_000);
        let source = initial[100].clone();
        let old_path = source.path.clone();
        let new_path = "wide/00900000.md".to_owned();
        let renamed = FileEntry::new(
            new_path.clone(),
            source.hash,
            source.mtime_ms,
            source.size_bytes,
        );

        let v1_store = MemoryChunkStore::new();
        let v1_before = crate::tree::build_tree(&v1_store, initial.clone(), "v", "d")
            .await
            .unwrap();
        let v1_after = crate::tree::update_tree(
            &v1_store,
            &v1_before,
            std::slice::from_ref(&renamed),
            std::slice::from_ref(&old_path),
        )
        .await
        .unwrap();

        let v2_store = MemoryChunkStore::new();
        let v2_before = build_tree(&v2_store, initial).await.unwrap();
        let v2_after = update_tree(
            &v2_store,
            &v2_before,
            std::slice::from_ref(&renamed),
            std::slice::from_ref(&old_path),
        )
        .await
        .unwrap()
        .0;

        let v1_deltas = crate::diff::compute_deltas(&v1_store, &v1_before, &v1_after)
            .await
            .unwrap();
        let v2_deltas = compute_deltas(&v2_store, &v2_before, &v2_after)
            .await
            .unwrap();
        assert_eq!(v2_deltas, v1_deltas);
        assert_eq!(v2_deltas.len(), 1);
        assert!(matches!(
            &v2_deltas[0],
            FileDelta::Renamed {
                path,
                old_path: previous,
                hash,
                size,
                mtime_ms,
            } if path == &new_path
                && previous == &old_path
                && hash == &source.hash
                && *size == source.size_bytes
                && *mtime_ms == source.mtime_ms
        ));
    }

    #[tokio::test]
    async fn range_cursor_pages_are_exact_and_non_overlapping() {
        let expected = entries(5_000);
        let store = MemoryChunkStore::new();
        let root = build_tree(&store, expected.clone()).await.unwrap();
        let mut actual = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = range_page(&store, &root, cursor.as_deref(), 137)
                .await
                .unwrap();
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|entry| entry.path.clone());
            actual.extend(page);
        }
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn stored_root_roundtrips_and_rejects_every_truncation() {
        let store = MemoryChunkStore::new();
        let tree = build_tree(&store, entries(2_000)).await.unwrap();
        let mut root = RootNodeV2::new("vault", "device", tree).unwrap();
        root.created_ms = 1_900_000_000_123;
        root.parent_hash = Some(hash_bytes(b"parent"));
        let bytes = root.serialize().unwrap();
        assert_eq!(&bytes[..4], ROOT_MAGIC);
        assert_eq!(RootNodeV2::deserialize(&bytes).unwrap(), root);
        for end in 0..bytes.len() {
            assert!(
                RootNodeV2::deserialize(&bytes[..end]).is_err(),
                "accepted stored-root prefix {end}"
            );
        }

        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(RootNodeV2::deserialize(&trailing).is_err());
        let mut wrong_hash = bytes.clone();
        wrong_hash[32] ^= 1;
        assert!(RootNodeV2::deserialize(&wrong_hash).is_err());
        let mut reserved = bytes.clone();
        reserved[29] = 1;
        assert!(RootNodeV2::deserialize(&reserved).is_err());
        let mut unknown_flag = bytes.clone();
        unknown_flag[28] |= 0x80;
        assert!(RootNodeV2::deserialize(&unknown_flag).is_err());

        let mut metadata_changed = root.clone();
        metadata_changed.created_ms += 1;
        metadata_changed.device_id = "other-device".into();
        metadata_changed.parent_hash = None;
        assert_eq!(metadata_changed.hash(), root.hash());
        assert_ne!(metadata_changed.serialize().unwrap(), bytes);

        let empty = RootNodeV2::new("vault", "", empty_root()).unwrap();
        assert_eq!(
            RootNodeV2::deserialize(&empty.serialize().unwrap()).unwrap(),
            empty
        );
    }

    #[tokio::test]
    async fn codecs_and_descriptors_fail_closed() {
        let store = MemoryChunkStore::new();
        let root = build_tree(&store, entries(2_000)).await.unwrap();
        let leaves = leaf_ranges(&store, &root).await.unwrap();
        let bytes = store.get(&leaves[0].hash).await.unwrap();
        assert!(decode_node(&bytes).is_ok());
        for end in 0..bytes.len() {
            assert!(decode_node(&bytes[..end]).is_err(), "accepted prefix {end}");
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode_node(&trailing).is_err());

        let top = root.child.as_ref().unwrap();
        assert!(top.height > 0);
        let internal = store.get(&top.hash).await.unwrap();
        assert!(decode_node(&internal).is_ok());
        for end in 0..internal.len() {
            assert!(
                decode_node(&internal[..end]).is_err(),
                "accepted internal prefix {end}"
            );
        }
        let mut internal_trailing = internal.clone();
        internal_trailing.push(0);
        assert!(decode_node(&internal_trailing).is_err());
        let mut reserved = internal.clone();
        reserved[6] = 1;
        assert!(decode_node(&reserved).is_err());
        let mut unknown_magic = bytes.clone();
        unknown_magic[..4].copy_from_slice(b"NOPE");
        assert!(decode_node(&unknown_magic).is_err());

        let mut wrong_length = leaves[0].clone();
        wrong_length.serialized_bytes += 1;
        assert!(load_node(&store, &wrong_length).await.is_err());
        let mut oversized_count = leaves[0].clone();
        oversized_count.file_count = u64::MAX;
        assert!(load_node(&store, &oversized_count).await.is_err());
        assert!(load_leaf_span(
            &store,
            std::slice::from_ref(&oversized_count),
            &mut TraversalBudget::default(),
        )
        .await
        .is_err());
        let mut wrong_total = root.clone();
        wrong_total.total_files += 1;
        assert!(load_all_entries(&store, &wrong_total).await.is_err());
        let mut wrong_height = root.clone();
        wrong_height.child.as_mut().unwrap().height += 1;
        assert_ne!(wrong_height.hash(), root.hash());
        assert!(reachable_hashes(&store, &wrong_height).await.is_err());
        let mut too_deep = root.clone();
        too_deep.child.as_mut().unwrap().height = MAX_TREE_DEPTH + 1;
        assert!(reachable_hashes(&store, &too_deep).await.is_err());
        assert!(
            build_tree(&MemoryChunkStore::new(), vec![entry("../escape", 1)])
                .await
                .is_err()
        );
        assert!(build_tree(
            &MemoryChunkStore::new(),
            vec![entry("same.md", 1), entry("same.md", 2)]
        )
        .await
        .is_err());
    }
}
