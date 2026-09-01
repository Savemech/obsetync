use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

pub const GIB: u64 = 1024 * 1024 * 1024;
pub const W2_TARGET_BYTES: u64 = 4 * GIB + GIB / 2;
const SCALE_DENOMINATOR: u64 = 1_000_000;
const DEFAULT_LARGE_DATASET_GUARD: u64 = 5 * GIB;
const IO_BUFFER_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "UPPERCASE")]
pub enum WorkloadId {
    W1,
    W2,
    W3,
    W4,
    W5,
    W6,
    W7,
    W8,
}

impl fmt::Display for WorkloadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scale {
    millionths: u64,
}

impl Scale {
    pub const FULL: Self = Self {
        millionths: SCALE_DENOMINATOR,
    };

    pub fn millionths(self) -> u64 {
        self.millionths
    }

    fn count(self, full: usize) -> usize {
        let scaled = (full as u128 * self.millionths as u128).div_ceil(SCALE_DENOMINATOR as u128);
        usize::try_from(scaled.max(1)).unwrap_or(usize::MAX)
    }

    fn bytes(self, full: u64) -> u64 {
        let scaled = (full as u128 * self.millionths as u128).div_ceil(SCALE_DENOMINATOR as u128);
        u64::try_from(scaled.max(1)).unwrap_or(u64::MAX)
    }
}

impl FromStr for Scale {
    type Err = HarnessError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() || value.starts_with('-') {
            return Err(HarnessError::InvalidScale(value.to_owned()));
        }
        let mut parts = value.split('.');
        let whole = parts
            .next()
            .ok_or_else(|| HarnessError::InvalidScale(value.to_owned()))?;
        let fraction = parts.next().unwrap_or("");
        if parts.next().is_some()
            || whole.chars().any(|ch| !ch.is_ascii_digit())
            || fraction.chars().any(|ch| !ch.is_ascii_digit())
            || fraction.len() > 6
        {
            return Err(HarnessError::InvalidScale(value.to_owned()));
        }
        let whole: u64 = whole
            .parse()
            .map_err(|_| HarnessError::InvalidScale(value.to_owned()))?;
        let mut fractional = fraction.to_owned();
        fractional.extend(std::iter::repeat_n('0', 6 - fraction.len()));
        let fractional: u64 = fractional
            .parse()
            .map_err(|_| HarnessError::InvalidScale(value.to_owned()))?;
        let millionths = whole
            .checked_mul(SCALE_DENOMINATOR)
            .and_then(|base| base.checked_add(fractional))
            .ok_or_else(|| HarnessError::InvalidScale(value.to_owned()))?;
        if millionths == 0 || millionths > SCALE_DENOMINATOR {
            return Err(HarnessError::InvalidScale(value.to_owned()));
        }
        Ok(Self { millionths })
    }
}

impl fmt::Display for Scale {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let whole = self.millionths / SCALE_DENOMINATOR;
        let fraction = self.millionths % SCALE_DENOMINATOR;
        if fraction == 0 {
            return write!(formatter, "{whole}");
        }
        let fraction = format!("{fraction:06}");
        write!(formatter, "{whole}.{}", fraction.trim_end_matches('0'))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    Markdown,
    Jpeg,
    Pdf,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileSpec {
    pub path: String,
    pub size: u64,
    pub kind: FileKind,
    pub content_seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ScenarioOperation {
    Modify {
        path: String,
        size: u64,
        content_seed: u64,
    },
    Rename {
        from: String,
        to: String,
    },
    Delete {
        path: String,
    },
    KillAfter {
        seconds: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadPlan {
    pub workload: WorkloadId,
    pub seed: u64,
    pub scale: Scale,
    pub files: Vec<FileSpec>,
    pub operations: Vec<ScenarioOperation>,
}

impl WorkloadPlan {
    pub fn build(workload: WorkloadId, seed: u64, scale: Scale) -> Result<Self, HarnessError> {
        let (files, operations) = match workload {
            WorkloadId::W1 => (markdown_files(seed, scale.count(100_000), "notes"), vec![]),
            WorkloadId::W2 => (mixed_w2_files(seed, scale)?, vec![]),
            WorkloadId::W3 => (binary_asset_files(seed, scale.count(10_000)), vec![]),
            WorkloadId::W4 => (
                vec![FileSpec {
                    path: "large/single-20gib.bin".into(),
                    size: scale.bytes(20 * GIB),
                    kind: FileKind::Binary,
                    content_seed: derive_seed(seed, 0),
                }],
                vec![],
            ),
            WorkloadId::W5 => {
                let files = markdown_files(seed, scale.count(100_000), "notes");
                let index = (mix64(seed) as usize) % files.len();
                let file = &files[index];
                let operation = ScenarioOperation::Modify {
                    path: file.path.clone(),
                    size: file.size,
                    content_seed: derive_seed(seed ^ 0x5755, index as u64),
                };
                (files, vec![operation])
            }
            WorkloadId::W6 => {
                let count = scale.count(10_000);
                let files = markdown_files(seed, count, "large-prefix");
                let operation_count = scale.count(10_000);
                let operations = (0..operation_count)
                    .map(|index| {
                        let file = &files[index % files.len()];
                        ScenarioOperation::Modify {
                            path: file.path.clone(),
                            size: file.size,
                            content_seed: derive_seed(seed ^ 0x6006, index as u64),
                        }
                    })
                    .collect();
                (files, operations)
            }
            WorkloadId::W7 => {
                let count = scale.count(50_000);
                let files = storm_files(seed, count);
                let rename_count = count / 2;
                let mut operations = Vec::with_capacity(count);
                for (index, file) in files.iter().enumerate().take(rename_count) {
                    operations.push(ScenarioOperation::Rename {
                        from: file.path.clone(),
                        to: format!("storm-renamed/{:03}/{index:06}.md", index % 256),
                    });
                }
                for file in files.iter().skip(rename_count) {
                    operations.push(ScenarioOperation::Delete {
                        path: file.path.clone(),
                    });
                }
                (files, operations)
            }
            WorkloadId::W8 => {
                let files = first_pull_files(seed, scale.count(68_000));
                let operations = (0..scale.count(16))
                    .map(|index| ScenarioOperation::KillAfter {
                        seconds: 5 + mix64(seed.wrapping_add(index as u64)) % 16,
                    })
                    .collect();
                (files, operations)
            }
        };
        Ok(Self {
            workload,
            seed,
            scale,
            files,
            operations,
        })
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.size).sum()
    }

    pub fn digest(&self) -> Result<String, HarnessError> {
        Ok(blake3::hash(&serde_json::to_vec(self)?)
            .to_hex()
            .to_string())
    }

    pub fn summary(&self) -> Result<PlanSummary, HarnessError> {
        Ok(PlanSummary {
            workload: self.workload,
            seed: self.seed,
            scale: self.scale,
            file_count: self.files.len() as u64,
            total_bytes: self.total_bytes(),
            operation_count: self.operations.len() as u64,
            plan_digest: self.digest()?,
            requires_large_dataset_opt_in: self.total_bytes() > DEFAULT_LARGE_DATASET_GUARD,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSummary {
    pub workload: WorkloadId,
    pub seed: u64,
    pub scale: Scale,
    pub file_count: u64,
    pub total_bytes: u64,
    pub operation_count: u64,
    pub plan_digest: String,
    pub requires_large_dataset_opt_in: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadManifest {
    pub schema_version: u8,
    pub generator_version: String,
    pub workload: WorkloadId,
    pub seed: u64,
    pub scale: Scale,
    pub file_count: u64,
    pub total_bytes: u64,
    pub operation_count: u64,
    pub plan_digest: String,
    pub dataset_digest: String,
    pub operations_digest: String,
}

#[derive(Debug, Clone, Copy)]
pub struct MaterializeOptions {
    pub allow_large: bool,
    pub large_dataset_guard_bytes: u64,
}

impl Default for MaterializeOptions {
    fn default() -> Self {
        Self {
            allow_large: false,
            large_dataset_guard_bytes: DEFAULT_LARGE_DATASET_GUARD,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub file_count: u64,
    pub total_bytes: u64,
    pub dataset_digest: Option<String>,
    pub deep: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub schema_version: u8,
    pub harness_version: String,
    pub workload: WorkloadId,
    pub seed: u64,
    pub scale: Scale,
    pub operating_system: String,
    pub architecture: String,
    pub file_count: u64,
    pub total_bytes: u64,
    pub iterations: Vec<BenchmarkIteration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkIteration {
    pub enumerate_and_hash_ns: u64,
    pub tree_build_ns: u64,
    pub files_per_second: f64,
    pub mebibytes_per_second: f64,
    pub initial_tree_chunks: u64,
    pub final_tree_chunks: u64,
    pub final_reachable_tree_chunks: u64,
    pub unreachable_tree_chunks: u64,
    pub gc_removed_chunks: u64,
    pub gc_removed_bytes: u64,
    pub initial_root: String,
    pub final_root: String,
    pub root_matches_flat_oracle: bool,
    pub scenario_operations: u64,
    pub scenario_elapsed_ns: u64,
    pub operation_latency_p50_ns: Option<u64>,
    pub operation_latency_p95_ns: Option<u64>,
    pub operation_latency_p99_ns: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("invalid scale {0:?}; expected a decimal in (0, 1] with at most six digits")]
    InvalidScale(String),
    #[error("output already exists; choose a new directory: {0}")]
    OutputExists(PathBuf),
    #[error(
        "planned dataset is {planned_bytes} bytes, above the {guard_bytes}-byte safety guard; pass --allow-large"
    )]
    LargeDatasetGuard {
        planned_bytes: u64,
        guard_bytes: u64,
    },
    #[error("manifest mismatch: {0}")]
    ManifestMismatch(String),
    #[error("unsafe generated relative path: {0}")]
    UnsafePath(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub fn materialize(
    plan: &WorkloadPlan,
    output: &Path,
    options: MaterializeOptions,
) -> Result<WorkloadManifest, HarnessError> {
    let total_bytes = plan.total_bytes();
    if total_bytes > options.large_dataset_guard_bytes && !options.allow_large {
        return Err(HarnessError::LargeDatasetGuard {
            planned_bytes: total_bytes,
            guard_bytes: options.large_dataset_guard_bytes,
        });
    }
    match fs::create_dir(output) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(HarnessError::OutputExists(output.to_path_buf()));
        }
        Err(error) => return Err(error.into()),
    }
    let vault = output.join("vault");
    fs::create_dir(&vault)?;

    let mut dataset_hasher = blake3::Hasher::new();
    for file in &plan.files {
        let relative = safe_relative_path(&file.path)?;
        let path = vault.join(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content_hash = write_file(&path, file)?;
        dataset_hasher.update(file.path.as_bytes());
        dataset_hasher.update(&[0]);
        dataset_hasher.update(&file.size.to_le_bytes());
        dataset_hasher.update(content_hash.as_bytes());
    }

    let operations_path = output.join("operations.ndjson");
    let operations_file = File::create(&operations_path)?;
    let mut operations_writer = BufWriter::new(operations_file);
    let mut operations_hasher = blake3::Hasher::new();
    for operation in &plan.operations {
        let encoded = serde_json::to_vec(operation)?;
        operations_writer.write_all(&encoded)?;
        operations_writer.write_all(b"\n")?;
        operations_hasher.update(&encoded);
        operations_hasher.update(b"\n");
    }
    operations_writer.flush()?;

    let manifest = WorkloadManifest {
        schema_version: 1,
        generator_version: env!("CARGO_PKG_VERSION").to_owned(),
        workload: plan.workload,
        seed: plan.seed,
        scale: plan.scale,
        file_count: plan.files.len() as u64,
        total_bytes,
        operation_count: plan.operations.len() as u64,
        plan_digest: plan.digest()?,
        dataset_digest: dataset_hasher.finalize().to_hex().to_string(),
        operations_digest: operations_hasher.finalize().to_hex().to_string(),
    };
    let manifest_file = File::create(output.join("manifest.json"))?;
    serde_json::to_writer_pretty(BufWriter::new(manifest_file), &manifest)?;
    Ok(manifest)
}

pub fn verify(input: &Path, deep: bool) -> Result<VerificationReport, HarnessError> {
    let manifest: WorkloadManifest =
        serde_json::from_reader(BufReader::new(File::open(input.join("manifest.json"))?))?;
    if manifest.schema_version != 1 {
        return Err(HarnessError::ManifestMismatch(format!(
            "unsupported schema {}",
            manifest.schema_version
        )));
    }
    let plan = WorkloadPlan::build(manifest.workload, manifest.seed, manifest.scale)?;
    if plan.digest()? != manifest.plan_digest
        || plan.files.len() as u64 != manifest.file_count
        || plan.total_bytes() != manifest.total_bytes
        || plan.operations.len() as u64 != manifest.operation_count
    {
        return Err(HarnessError::ManifestMismatch(
            "plan metadata does not match generator output".into(),
        ));
    }

    let vault = input.join("vault");
    let mut total_bytes = 0u64;
    let mut dataset_hasher = deep.then(blake3::Hasher::new);
    for file in &plan.files {
        let path = vault.join(safe_relative_path(&file.path)?);
        let metadata = fs::metadata(&path)
            .map_err(|error| HarnessError::ManifestMismatch(format!("{}: {error}", file.path)))?;
        if metadata.len() != file.size {
            return Err(HarnessError::ManifestMismatch(format!(
                "{} size is {}, expected {}",
                file.path,
                metadata.len(),
                file.size
            )));
        }
        total_bytes = total_bytes.saturating_add(metadata.len());
        if let Some(hasher) = dataset_hasher.as_mut() {
            let content_hash = hash_file(&path)?;
            hasher.update(file.path.as_bytes());
            hasher.update(&[0]);
            hasher.update(&file.size.to_le_bytes());
            hasher.update(content_hash.as_bytes());
        }
    }

    let actual_files = count_files(&vault)?;
    if actual_files != manifest.file_count {
        return Err(HarnessError::ManifestMismatch(format!(
            "vault contains {actual_files} files, expected {}",
            manifest.file_count
        )));
    }
    let operations_digest = hash_file(&input.join("operations.ndjson"))?;
    if operations_digest.to_hex().as_str() != manifest.operations_digest {
        return Err(HarnessError::ManifestMismatch(
            "operations digest differs".into(),
        ));
    }
    let dataset_digest = dataset_hasher.map(|hasher| hasher.finalize().to_hex().to_string());
    if dataset_digest
        .as_ref()
        .is_some_and(|digest| digest != &manifest.dataset_digest)
    {
        return Err(HarnessError::ManifestMismatch(
            "dataset digest differs".into(),
        ));
    }
    Ok(VerificationReport {
        file_count: actual_files,
        total_bytes,
        dataset_digest,
        deep,
    })
}

pub fn apply_operation(input: &Path, index: usize) -> Result<ScenarioOperation, HarnessError> {
    let manifest: WorkloadManifest =
        serde_json::from_reader(BufReader::new(File::open(input.join("manifest.json"))?))?;
    let plan = WorkloadPlan::build(manifest.workload, manifest.seed, manifest.scale)?;
    let operation = plan.operations.get(index).cloned().ok_or_else(|| {
        HarnessError::ManifestMismatch(format!(
            "operation {index} is out of range 0..{}",
            plan.operations.len()
        ))
    })?;
    let vault = input.join("vault");
    match &operation {
        ScenarioOperation::Modify {
            path,
            size,
            content_seed,
        } => {
            let spec = FileSpec {
                path: path.clone(),
                size: *size,
                kind: kind_from_path(path),
                content_seed: *content_seed,
            };
            write_file(&vault.join(safe_relative_path(path)?), &spec)?;
        }
        ScenarioOperation::Rename { from, to } => {
            let source = vault.join(safe_relative_path(from)?);
            let target = vault.join(safe_relative_path(to)?);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(source, target)?;
        }
        ScenarioOperation::Delete { path } => {
            fs::remove_file(vault.join(safe_relative_path(path)?))?;
        }
        ScenarioOperation::KillAfter { .. } => {}
    }
    Ok(operation)
}

pub async fn benchmark(
    input: &Path,
    iterations: usize,
    max_operations: Option<usize>,
) -> Result<BenchmarkReport, HarnessError> {
    if iterations == 0 {
        return Err(HarnessError::ManifestMismatch(
            "benchmark iterations must be positive".into(),
        ));
    }
    let manifest: WorkloadManifest =
        serde_json::from_reader(BufReader::new(File::open(input.join("manifest.json"))?))?;
    let plan = WorkloadPlan::build(manifest.workload, manifest.seed, manifest.scale)?;
    verify(input, false)?;

    let mut results = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let hash_started = Instant::now();
        let mut entries = Vec::with_capacity(plan.files.len());
        for (index, file) in plan.files.iter().enumerate() {
            let path = input.join("vault").join(safe_relative_path(&file.path)?);
            let hash = hash_file(&path)?;
            entries.push(sync_core::chunk::FileEntry::new(
                file.path.clone(),
                *hash.as_bytes(),
                index as u64 + 1,
                file.size,
            ));
        }
        let enumerate_and_hash_ns = elapsed_ns(hash_started.elapsed());

        let tree_started = Instant::now();
        let mut tree = sync_core::transactional_tree::TransactionalTree::new(
            "benchmark-vault",
            "perf-harness",
        );
        tree.rebuild(entries.clone())
            .await
            .map_err(|error| HarnessError::ManifestMismatch(error.to_string()))?;
        let tree_build_ns = elapsed_ns(tree_started.elapsed());
        let initial_tree_chunks = tree.store_len() as u64;
        let initial_root = sync_core::hash::hash_to_hex(
            &tree
                .committed_root_hash()
                .ok_or_else(|| HarnessError::ManifestMismatch("tree has no root".into()))?,
        );

        let mut entry_map: BTreeMap<String, sync_core::chunk::FileEntry> = entries
            .into_iter()
            .map(|entry| (entry.path.clone(), entry))
            .collect();
        let operation_limit = max_operations
            .unwrap_or(plan.operations.len())
            .min(plan.operations.len());
        let scenario_started = Instant::now();
        let mut operation_latencies = Vec::with_capacity(operation_limit);
        let mut applied_operations = 0u64;
        let mut gc_removed_chunks = 0u64;
        let mut gc_removed_bytes = 0u64;
        for (index, operation) in plan.operations.iter().take(operation_limit).enumerate() {
            let operation_started = Instant::now();
            let mut changed = Vec::new();
            let mut deleted = Vec::new();
            match operation {
                ScenarioOperation::Modify {
                    path,
                    size,
                    content_seed,
                } => {
                    let spec = FileSpec {
                        path: path.clone(),
                        size: *size,
                        kind: kind_from_path(path),
                        content_seed: *content_seed,
                    };
                    let entry = sync_core::chunk::FileEntry::new(
                        path.clone(),
                        *hash_spec(&spec)?.as_bytes(),
                        plan.files.len() as u64 + index as u64 + 1,
                        *size,
                    );
                    entry_map.insert(path.clone(), entry);
                    changed.push(entry_map[path].clone());
                }
                ScenarioOperation::Rename { from, to } => {
                    let mut entry = entry_map.remove(from).ok_or_else(|| {
                        HarnessError::ManifestMismatch(format!(
                            "rename source missing in scenario: {from}"
                        ))
                    })?;
                    entry.path = to.clone();
                    entry_map.insert(to.clone(), entry);
                    changed.push(entry_map[to].clone());
                    deleted.push(from.clone());
                }
                ScenarioOperation::Delete { path } => {
                    entry_map.remove(path).ok_or_else(|| {
                        HarnessError::ManifestMismatch(format!(
                            "delete source missing in scenario: {path}"
                        ))
                    })?;
                    deleted.push(path.clone());
                }
                ScenarioOperation::KillAfter { .. } => continue,
            }
            tree.begin_candidate()
                .map_err(|error| HarnessError::ManifestMismatch(error.to_string()))?;
            if let Err(error) = tree.apply_candidate(&changed, &deleted).await {
                let _ = tree.abort_candidate();
                return Err(HarnessError::ManifestMismatch(error.to_string()));
            }
            let gc = tree
                .commit_candidate()
                .map_err(|error| HarnessError::ManifestMismatch(error.to_string()))?;
            gc_removed_chunks = gc_removed_chunks.saturating_add(gc.removed);
            gc_removed_bytes = gc_removed_bytes.saturating_add(gc.bytes_removed);
            applied_operations += 1;
            operation_latencies.push(elapsed_ns(operation_started.elapsed()));
        }
        let scenario_elapsed_ns = elapsed_ns(scenario_started.elapsed());
        operation_latencies.sort_unstable();

        let final_root_hash = tree
            .committed_root_hash()
            .ok_or_else(|| HarnessError::ManifestMismatch("tree has no final root".into()))?;
        let final_reachable_tree_chunks = tree
            .committed_chunk_hashes()
            .map_err(|error| HarnessError::ManifestMismatch(error.to_string()))?
            .len() as u64;
        let final_tree_chunks = tree.store_len() as u64;
        let unreachable_tree_chunks = final_tree_chunks.saturating_sub(final_reachable_tree_chunks);

        // Rebuild from the flat final state as an independent semantic oracle.
        let oracle_store = sync_core::store::MemoryChunkStore::new();
        let oracle_root = sync_core::tree::build_tree(
            &oracle_store,
            entry_map.values().cloned().collect(),
            "benchmark-vault",
            "perf-harness",
        )
        .await
        .map_err(|error| HarnessError::ManifestMismatch(error.to_string()))?;

        let seconds = enumerate_and_hash_ns.max(1) as f64 / 1_000_000_000.0;
        results.push(BenchmarkIteration {
            enumerate_and_hash_ns,
            tree_build_ns,
            files_per_second: plan.files.len() as f64 / seconds,
            mebibytes_per_second: (plan.total_bytes() as f64 / 1_048_576.0) / seconds,
            initial_tree_chunks,
            final_tree_chunks,
            final_reachable_tree_chunks,
            unreachable_tree_chunks,
            gc_removed_chunks,
            gc_removed_bytes,
            initial_root,
            final_root: sync_core::hash::hash_to_hex(&final_root_hash),
            root_matches_flat_oracle: final_root_hash == oracle_root.hash(),
            scenario_operations: applied_operations,
            scenario_elapsed_ns,
            operation_latency_p50_ns: percentile(&operation_latencies, 50),
            operation_latency_p95_ns: percentile(&operation_latencies, 95),
            operation_latency_p99_ns: percentile(&operation_latencies, 99),
        });
    }

    Ok(BenchmarkReport {
        schema_version: 2,
        harness_version: env!("CARGO_PKG_VERSION").to_owned(),
        workload: plan.workload,
        seed: plan.seed,
        scale: plan.scale,
        operating_system: std::env::consts::OS.to_owned(),
        architecture: std::env::consts::ARCH.to_owned(),
        file_count: plan.files.len() as u64,
        total_bytes: plan.total_bytes(),
        iterations: results,
    })
}

fn markdown_files(seed: u64, count: usize, prefix: &str) -> Vec<FileSpec> {
    (0..count)
        .map(|index| FileSpec {
            path: format!("{prefix}/{:03}/{index:06}.md", index % 256),
            size: 1024 + mix64(seed.wrapping_add(index as u64)) % (15 * 1024 + 1),
            kind: FileKind::Markdown,
            content_seed: derive_seed(seed, index as u64),
        })
        .collect()
}

fn mixed_w2_files(seed: u64, scale: Scale) -> Result<Vec<FileSpec>, HarnessError> {
    let count = scale.count(25_000);
    let markdown_count = count * 80 / 100;
    let jpeg_count = count * 16 / 100;
    let target_bytes = scale.bytes(W2_TARGET_BYTES);
    let mut descriptors = Vec::with_capacity(count);
    for index in 0..count {
        let (kind, weight, directory, extension) = if index < markdown_count {
            (FileKind::Markdown, 1u64, "notes", "md")
        } else if index < markdown_count + jpeg_count {
            (FileKind::Jpeg, 16u64, "attachments/images", "jpg")
        } else {
            (FileKind::Pdf, 64u64, "attachments/pdfs", "pdf")
        };
        let jitter = 750 + mix64(seed ^ index as u64) % 501;
        descriptors.push((kind, weight * jitter, directory, extension));
    }
    let total_weight: u128 = descriptors.iter().map(|item| item.1 as u128).sum();
    let mut cumulative_weight = 0u128;
    let mut allocated = 0u64;
    let mut files = Vec::with_capacity(count);
    for (index, (kind, weight, directory, extension)) in descriptors.into_iter().enumerate() {
        cumulative_weight += weight as u128;
        let next_total = if index + 1 == count {
            target_bytes
        } else {
            u64::try_from(target_bytes as u128 * cumulative_weight / total_weight)
                .map_err(|_| HarnessError::ManifestMismatch("W2 size overflow".into()))?
        };
        let size = next_total.saturating_sub(allocated);
        allocated = next_total;
        files.push(FileSpec {
            path: format!("{directory}/{:03}/{index:06}.{extension}", index % 256),
            size,
            kind,
            content_seed: derive_seed(seed, index as u64),
        });
    }
    Ok(files)
}

fn binary_asset_files(seed: u64, count: usize) -> Vec<FileSpec> {
    const MIN: u64 = 100 * 1024;
    const RANGE: u64 = 20 * 1024 * 1024 - MIN;
    (0..count)
        .map(|index| {
            let random = mix64(seed.wrapping_add(index as u64));
            let fraction = (random & 0xffff) as u128;
            let skewed = fraction * fraction * fraction;
            let denominator = 0xffffu128.pow(3);
            let size = MIN + (RANGE as u128 * skewed / denominator) as u64;
            let (kind, extension) = if index % 2 == 0 {
                (FileKind::Jpeg, "jpg")
            } else {
                (FileKind::Pdf, "pdf")
            };
            FileSpec {
                path: format!("assets/{:03}/{index:06}.{extension}", index % 256),
                size,
                kind,
                content_seed: derive_seed(seed, index as u64),
            }
        })
        .collect()
}

fn storm_files(seed: u64, count: usize) -> Vec<FileSpec> {
    (0..count)
        .map(|index| FileSpec {
            path: format!("storm/{:03}/{index:06}.md", index % 256),
            size: 512 + mix64(seed.wrapping_add(index as u64)) % 3585,
            kind: FileKind::Markdown,
            content_seed: derive_seed(seed, index as u64),
        })
        .collect()
}

fn first_pull_files(seed: u64, count: usize) -> Vec<FileSpec> {
    (0..count)
        .map(|index| {
            let kind = if index % 20 == 0 {
                FileKind::Jpeg
            } else {
                FileKind::Markdown
            };
            let extension = if kind == FileKind::Jpeg { "jpg" } else { "md" };
            FileSpec {
                path: format!("first-pull/{:03}/{index:06}.{extension}", index % 512),
                size: 1024 + mix64(seed.wrapping_add(index as u64)) % (63 * 1024 + 1),
                kind,
                content_seed: derive_seed(seed, index as u64),
            }
        })
        .collect()
}

fn write_file(path: &Path, spec: &FileSpec) -> Result<blake3::Hash, HarnessError> {
    let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, File::create(path)?);
    let hash = generate_content(&mut writer, spec)?;
    writer.flush()?;
    Ok(hash)
}

fn hash_spec(spec: &FileSpec) -> Result<blake3::Hash, HarnessError> {
    generate_content(&mut std::io::sink(), spec)
}

fn generate_content(writer: &mut dyn Write, spec: &FileSpec) -> Result<blake3::Hash, HarnessError> {
    let mut content_hasher = blake3::Hasher::new();
    let mut source = blake3::Hasher::new();
    source.update(b"obsetync-perf-workload-v1");
    source.update(&spec.content_seed.to_le_bytes());
    source.update(&[spec.kind as u8]);
    let mut xof = source.finalize_xof();
    let mut remaining = spec.size;
    let mut offset = 0u64;
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    while remaining > 0 {
        let length = usize::try_from(remaining.min(buffer.len() as u64)).unwrap();
        let chunk = &mut buffer[..length];
        xof.fill(chunk);
        if spec.kind == FileKind::Markdown {
            const ALPHABET: &[u8; 32] = b" etaoinshrdlucmfwypvbgkqjxz0123\n";
            for byte in chunk.iter_mut() {
                *byte = ALPHABET[usize::from(*byte & 31)];
            }
        }
        overlay_format_markers(chunk, offset, spec.size, spec.kind, spec.content_seed);
        writer.write_all(chunk)?;
        content_hasher.update(chunk);
        offset += length as u64;
        remaining -= length as u64;
    }
    Ok(content_hasher.finalize())
}

fn overlay_format_markers(
    chunk: &mut [u8],
    offset: u64,
    total_size: u64,
    kind: FileKind,
    seed: u64,
) {
    let header = match kind {
        FileKind::Markdown => format!("# Obsetync benchmark note {seed:016x}\n\n").into_bytes(),
        FileKind::Jpeg => vec![0xff, 0xd8, 0xff, 0xe0, b'J', b'F', b'I', b'F', 0],
        FileKind::Pdf => b"%PDF-1.7\n% Obsetync benchmark payload\n".to_vec(),
        FileKind::Binary => b"OBSETYNC-BENCH-V1\0".to_vec(),
    };
    for (index, byte) in header.into_iter().enumerate() {
        let absolute = index as u64;
        if absolute >= offset && absolute < offset + chunk.len() as u64 && absolute < total_size {
            chunk[(absolute - offset) as usize] = byte;
        }
    }
    if kind == FileKind::Jpeg && total_size >= 2 {
        for (absolute, byte) in [(total_size - 2, 0xff), (total_size - 1, 0xd9)] {
            if absolute >= offset && absolute < offset + chunk.len() as u64 {
                chunk[(absolute - offset) as usize] = byte;
            }
        }
    }
}

fn hash_file(path: &Path) -> Result<blake3::Hash, HarnessError> {
    let mut reader = BufReader::with_capacity(IO_BUFFER_BYTES, File::open(path)?);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; IO_BUFFER_BYTES];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize())
}

fn count_files(root: &Path) -> Result<u64, HarnessError> {
    let mut count = 0u64;
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn safe_relative_path(value: &str) -> Result<&Path, HarnessError> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(HarnessError::UnsafePath(value.to_owned()));
    }
    Ok(path)
}

fn kind_from_path(path: &str) -> FileKind {
    if path.ends_with(".md") {
        FileKind::Markdown
    } else if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        FileKind::Jpeg
    } else if path.ends_with(".pdf") {
        FileKind::Pdf
    } else {
        FileKind::Binary
    }
}

fn derive_seed(seed: u64, index: u64) -> u64 {
    mix64(seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn elapsed_ns(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn percentile(sorted: &[u64], percentile: usize) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (sorted.len() * percentile).div_ceil(100).max(1);
    sorted.get(rank - 1).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_workload_plans_match_rfc_cardinality() {
        let cases = [
            (WorkloadId::W1, 100_000, 0),
            (WorkloadId::W2, 25_000, 0),
            (WorkloadId::W3, 10_000, 0),
            (WorkloadId::W4, 1, 0),
            (WorkloadId::W5, 100_000, 1),
            (WorkloadId::W6, 10_000, 10_000),
            (WorkloadId::W7, 50_000, 50_000),
            (WorkloadId::W8, 68_000, 16),
        ];
        for (id, files, operations) in cases {
            let plan = WorkloadPlan::build(id, 0x0b5e_71c0, Scale::FULL).unwrap();
            assert_eq!(plan.files.len(), files, "{id:?} file count");
            assert_eq!(plan.operations.len(), operations, "{id:?} operation count");
        }
    }

    #[test]
    fn byte_targets_and_ranges_are_exact() {
        let w1 = WorkloadPlan::build(WorkloadId::W1, 7, Scale::FULL).unwrap();
        assert!(w1
            .files
            .iter()
            .all(|file| (1024..=16 * 1024).contains(&file.size)));

        let w2 = WorkloadPlan::build(WorkloadId::W2, 7, Scale::FULL).unwrap();
        assert_eq!(w2.total_bytes(), W2_TARGET_BYTES);

        let w3 = WorkloadPlan::build(WorkloadId::W3, 7, Scale::FULL).unwrap();
        assert!(w3
            .files
            .iter()
            .all(|file| { (100 * 1024..=20 * 1024 * 1024).contains(&file.size) }));

        let w4 = WorkloadPlan::build(WorkloadId::W4, 7, Scale::FULL).unwrap();
        assert_eq!(w4.total_bytes(), 20 * GIB);
    }

    #[test]
    fn scenario_operations_have_the_promised_shape() {
        let w6 = WorkloadPlan::build(WorkloadId::W6, 9, Scale::FULL).unwrap();
        assert!(w6.operations.iter().all(|operation| {
            matches!(operation, ScenarioOperation::Modify { path, .. } if path.starts_with("large-prefix/"))
        }));

        let w7 = WorkloadPlan::build(WorkloadId::W7, 9, Scale::FULL).unwrap();
        assert_eq!(
            w7.operations
                .iter()
                .filter(|operation| matches!(operation, ScenarioOperation::Rename { .. }))
                .count(),
            25_000,
        );
        assert_eq!(
            w7.operations
                .iter()
                .filter(|operation| matches!(operation, ScenarioOperation::Delete { .. }))
                .count(),
            25_000,
        );

        let w8 = WorkloadPlan::build(WorkloadId::W8, 9, Scale::FULL).unwrap();
        let intervals: Vec<u64> = w8
            .operations
            .iter()
            .filter_map(|operation| match operation {
                ScenarioOperation::KillAfter { seconds } => Some(*seconds),
                _ => None,
            })
            .collect();
        assert_eq!(intervals.len(), 16);
        assert!(intervals.iter().all(|seconds| (5..=20).contains(seconds)));
    }

    #[test]
    fn scaled_materialization_is_byte_reproducible() {
        let scale = "0.00002".parse::<Scale>().unwrap();
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let first = first_dir.path().join("first");
        let second = second_dir.path().join("second");
        let plan = WorkloadPlan::build(WorkloadId::W1, 42, scale).unwrap();

        let first_manifest = materialize(&plan, &first, MaterializeOptions::default()).unwrap();
        let second_manifest = materialize(&plan, &second, MaterializeOptions::default()).unwrap();

        assert_eq!(first_manifest, second_manifest);
        verify(&first, true).unwrap();
        verify(&second, true).unwrap();
    }

    #[tokio::test]
    async fn scenario_benchmark_commits_transactionally_without_unreachable_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let output = temp.path().join("w6");
        let plan =
            WorkloadPlan::build(WorkloadId::W6, 42, "0.001".parse::<Scale>().unwrap()).unwrap();
        materialize(&plan, &output, MaterializeOptions::default()).unwrap();

        let report = benchmark(&output, 1, None).await.unwrap();
        let iteration = &report.iterations[0];
        assert_eq!(iteration.scenario_operations, 10);
        assert_eq!(iteration.unreachable_tree_chunks, 0);
        assert!(iteration.gc_removed_chunks > 0);
        assert!(iteration.root_matches_flat_oracle);
    }
}
