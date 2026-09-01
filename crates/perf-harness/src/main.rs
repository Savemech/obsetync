use clap::{Parser, Subcommand};
use perf_harness::{
    apply_operation, benchmark, benchmark_prefix_merge, materialize, verify, MaterializeOptions,
    Scale, WorkloadId, WorkloadPlan,
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
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let value = match cli.command {
        Command::Describe {
            workload,
            seed,
            scale,
        } => serde_json::to_value(WorkloadPlan::build(workload, seed, scale)?.summary()?)?,
        Command::Generate {
            workload,
            output,
            seed,
            scale,
            allow_large,
        } => {
            let plan = WorkloadPlan::build(workload, seed, scale)?;
            serde_json::to_value(materialize(
                &plan,
                &output,
                MaterializeOptions {
                    allow_large,
                    ..MaterializeOptions::default()
                },
            )?)?
        }
        Command::Verify { input, deep } => serde_json::to_value(verify(&input, deep)?)?,
        Command::Apply { input, operation } => {
            serde_json::to_value(apply_operation(&input, operation)?)?
        }
        Command::Bench {
            input,
            iterations,
            max_operations,
            output,
        } => {
            let report = benchmark(&input, iterations, max_operations).await?;
            if let Some(path) = output {
                let file = std::fs::File::create(path)?;
                serde_json::to_writer_pretty(std::io::BufWriter::new(file), &report)?;
                return Ok(());
            }
            serde_json::to_value(report)?
        }
        Command::PrefixBench {
            entries,
            upserts,
            deletes,
            iterations,
            seed,
            output,
        } => {
            let report =
                benchmark_prefix_merge(entries, upserts, deletes, iterations, seed).await?;
            if let Some(path) = output {
                let file = std::fs::File::create(path)?;
                serde_json::to_writer_pretty(std::io::BufWriter::new(file), &report)?;
                return Ok(());
            }
            serde_json::to_value(report)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
