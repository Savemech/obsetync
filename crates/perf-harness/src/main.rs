use clap::{Parser, Subcommand};
use perf_harness::{
    apply_operation, benchmark, benchmark_diff_pages, benchmark_prefix_merge, benchmark_tree_v2,
    materialize, verify, write_json_file, MaterializeOptions, Scale, WorkloadId, WorkloadPlan,
};
use std::path::PathBuf;

const DEFAULT_SEED: u64 = 0x0b5e_71c0_1111_0001;

#[derive(Debug, Parser)]
#[command(
    name = "obsetync-perf",
    about = "Deterministic Obsetync W1-W8 workload generator and local benchmark"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Print the exact planned file/byte/operation counts without writing data.
    Describe {
        #[arg(value_enum)]
        workload: WorkloadId,
        #[arg(long, default_value_t = DEFAULT_SEED)]
        seed: u64,
        #[arg(long, default_value = "1")]
        scale: Scale,
    },
    /// Materialize a new benchmark directory containing vault/ and manifests.
    Generate {
        #[arg(value_enum)]
        workload: WorkloadId,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = DEFAULT_SEED)]
        seed: u64,
        #[arg(long, default_value = "1")]
        scale: Scale,
        /// Required when the planned dataset exceeds the 5 GiB safety guard.
        #[arg(long)]
        allow_large: bool,
    },
    /// Validate manifest/count/size and optionally every content digest.
    Verify {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        deep: bool,
    },
    /// Apply exactly one deterministic W5/W6/W7 operation to vault/.
    Apply {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        operation: usize,
    },
    /// Measure native streaming hash, tree build, and scenario update costs.
    Bench {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, default_value_t = 1)]
        iterations: usize,
        /// Bound long W6/W7 smoke runs without changing the generated dataset.
        #[arg(long)]
        max_operations: Option<usize>,
        /// Optional JSON report path; stdout is used when omitted.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Compare legacy repeated scans with the linear Tree-v1 prefix merge.
    PrefixBench {
        #[arg(long, default_value_t = 10_000)]
        entries: usize,
        #[arg(long, default_value_t = 5_000)]
        upserts: usize,
        #[arg(long, default_value_t = 5_000)]
        deletes: usize,
        #[arg(long, default_value_t = 5)]
        iterations: usize,
        #[arg(long, default_value_t = DEFAULT_SEED)]
        seed: u64,
        /// Optional JSON report path; stdout is used when omitted.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Verify OBD1 page/record caps over a deterministic large delta.
    DiffPageBench {
        #[arg(long, default_value_t = 85_000)]
        records: usize,
        #[arg(long, default_value_t = 512 * 1024)]
        page_bytes: usize,
        #[arg(long, default_value_t = 8_192)]
        page_records: usize,
        /// Optional JSON report path; stdout is used when omitted.
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Compare Tree v1 with the production-capable path-CDC range Tree v2.
    TreeV2Bench {
        #[arg(long, default_value_t = 100_000)]
        entries: usize,
        #[arg(long, default_value_t = 3)]
        iterations: usize,
        /// Optional JSON report path; stdout is used when omitted.
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.command {
        Command::Describe {
            workload,
            seed,
            scale,
        } => report(
            None,
            &WorkloadPlan::build(workload, seed, scale)?.summary()?,
        ),
        Command::Generate {
            workload,
            output,
            seed,
            scale,
            allow_large,
        } => {
            let plan = WorkloadPlan::build(workload, seed, scale)?;
            report(
                None,
                &materialize(
                    &plan,
                    &output,
                    MaterializeOptions {
                        allow_large,
                        ..MaterializeOptions::default()
                    },
                )?,
            )
        }
        Command::Verify { input, deep } => report(None, &verify(&input, deep)?),
        Command::Apply { input, operation } => report(None, &apply_operation(&input, operation)?),
        Command::Bench {
            input,
            iterations,
            max_operations,
            output,
        } => {
            let result = benchmark(&input, iterations, max_operations).await?;
            report(output, &result)
        }
        Command::PrefixBench {
            entries,
            upserts,
            deletes,
            iterations,
            seed,
            output,
        } => {
            let result =
                benchmark_prefix_merge(entries, upserts, deletes, iterations, seed).await?;
            report(output, &result)
        }
        Command::DiffPageBench {
            records,
            page_bytes,
            page_records,
            output,
        } => {
            let result = benchmark_diff_pages(records, page_bytes, page_records)?;
            report(output, &result)
        }
        Command::TreeV2Bench {
            entries,
            iterations,
            output,
        } => {
            let result = benchmark_tree_v2(entries, iterations).await?;
            report(output, &result)
        }
    }
}

fn report(
    output: Option<PathBuf>,
    value: &impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = output {
        write_json_file(&path, value)?;
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::to_value(value)?)?
        );
    }
    Ok(())
}
