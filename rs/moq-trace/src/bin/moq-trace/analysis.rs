//! Python and DuckDB analysis process boundary.

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Options {
	pub object_size: NonZeroU64,
	pub subscribers: NonZeroUsize,
	pub warmup: Duration,
	pub cooldown: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Statistics {
	pub count: usize,
	pub mean: f64,
	pub p50: f64,
	pub p95: f64,
	pub p99: f64,
	pub max: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Report {
	pub statistics: BTreeMap<String, Statistics>,
	pub quic_object_statistics: BTreeMap<String, Statistics>,
	pub packet_statistics: BTreeMap<String, Statistics>,
	pub packet_count: usize,
	pub group_count: usize,
	pub correlated_objects: usize,
	pub correlated_object_copies: usize,
}

pub(crate) struct Source<'a> {
	pub repo: &'a Path,
	pub ctf: &'a Path,
	pub python: &'a Path,
	pub expected_pid: Option<u32>,
}

pub(crate) fn run(source: Source<'_>, output: &Path, options: Options) -> Result<Report> {
	let script = source.repo.join("rs/moq-trace/scripts/analyze.py");
	if !script.is_file() {
		bail!("analysis script is missing: {}", script.display());
	}
	let mut command = Command::new(source.python);
	command
		.arg(&script)
		.arg(source.ctf)
		.arg("--output")
		.arg(output)
		.arg("--object-size")
		.arg(options.object_size.to_string())
		.arg("--subscribers")
		.arg(options.subscribers.to_string())
		.arg("--warmup-ns")
		.arg(options.warmup.as_nanos().min(u128::from(u64::MAX)).to_string())
		.arg("--cooldown-ns")
		.arg(options.cooldown.as_nanos().min(u128::from(u64::MAX)).to_string())
		.current_dir(source.repo);
	if let Some(pid) = source.expected_pid {
		command.arg("--expected-pid").arg(pid.to_string());
	}
	let result = command
		.output()
		.with_context(|| format!("failed to run DuckDB analyzer with {}", source.python.display()))?;
	if !result.status.success() {
		let stderr = String::from_utf8_lossy(&result.stderr).trim().to_owned();
		bail!("DuckDB analyzer failed with {}: {stderr}", result.status);
	}
	let manifest = output.join("manifest.json");
	serde_json::from_slice(&std::fs::read(&manifest).with_context(|| format!("failed to read {}", manifest.display()))?)
		.with_context(|| format!("failed to parse {}", manifest.display()))
}
