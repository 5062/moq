//! Capture, analyze, compare, and plot `moq-trace` relay experiments.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};

mod analysis;
mod experiment;

#[derive(Debug, Parser)]
#[command(about = "Capture and analyze MoQ relay latency traces.")]
struct Args {
	#[command(subcommand)]
	command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
	/// Validate and analyze one relay LTTng CTF trace.
	Analyze {
		/// Input LTTng CTF directory.
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
		/// Python interpreter providing the Babeltrace 2 bindings.
		#[arg(long, default_value = "python3")]
		python: PathBuf,
	},
	/// Run one local relay workload or a workload comparison.
	Experiment(experiment::Args),
	/// Render Matplotlib figures from an existing experiment directory.
	Plot(experiment::PlotArgs),
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
			python,
		} => {
			analysis::run(
				analysis::Source {
					ctf: &input,
					python: &python,
					expected_pid: None,
				},
				&output,
				analysis::Options {
					object_size,
					subscribers,
					warmup,
					cooldown,
				},
			)
			.with_context(|| format!("failed to analyze {}", input.display()))?;
			Ok(())
		}
		Command::Experiment(args) => experiment::run(args),
		Command::Plot(args) => experiment::plot(args),
	}
}
