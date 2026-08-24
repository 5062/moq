//! Offline analysis for `moq-trace` JSONL files.

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

mod analysis;

#[derive(Debug, Parser)]
#[command(about = "Analyze MoQ relay trace files.")]
struct Args {
	#[command(subcommand)]
	command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
	/// Validate and analyze one relay JSONL trace.
	Analyze {
		/// Input JSONL trace.
		input: PathBuf,
		/// Directory for CSV and JSON artifacts.
		#[arg(long)]
		output: PathBuf,
		/// Expected inbound object payload size in bytes.
		#[arg(long)]
		object_size: u64,
		/// Expected number of outbound copies per object.
		#[arg(long)]
		subscribers: usize,
		/// Seconds excluded from the beginning of the workload.
		#[arg(long, default_value_t = 0.0)]
		warmup: f64,
		/// Seconds excluded from the end of the workload.
		#[arg(long, default_value_t = 0.0)]
		cooldown: f64,
	},
}

fn main() -> anyhow::Result<()> {
	match Args::parse().command {
		Command::Analyze {
			input,
			output,
			object_size,
			subscribers,
			warmup,
			cooldown,
		} => analysis::run(
			&input,
			&output,
			analysis::Options {
				object_size,
				subscribers,
				warmup_seconds: warmup,
				cooldown_seconds: cooldown,
			},
		)
		.with_context(|| format!("failed to analyze {}", input.display())),
	}
}
