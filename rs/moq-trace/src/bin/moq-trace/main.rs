//! Offline analysis for `moq-trace` JSONL files.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

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
		object_size: NonZeroU64,
		/// Expected number of outbound copies per object.
		#[arg(long)]
		subscribers: NonZeroUsize,
		/// Duration excluded from the beginning of the workload.
		#[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
		warmup: Duration,
		/// Duration excluded from the end of the workload.
		#[arg(long, default_value = "0s", value_parser = humantime::parse_duration)]
		cooldown: Duration,
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
				warmup,
				cooldown,
			},
		)
		.with_context(|| format!("failed to analyze {}", input.display())),
	}
}
