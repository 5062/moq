//! Native CTF conversion and validation.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Summary {
	events: u64,
	discarded_events: u64,
	discarded_packets: u64,
}

pub(super) fn convert(
	python: &Path,
	script: &Path,
	schema: &Path,
	input: &Path,
	output: &Path,
	expected_pid: u32,
) -> Result<()> {
	let result = Command::new(python)
		.arg(script)
		.arg(input)
		.arg(output)
		.arg("--schema")
		.arg(schema)
		.arg("--expected-pid")
		.arg(expected_pid.to_string())
		.output()
		.with_context(|| format!("failed to run {}", script.display()))?;
	if !result.status.success() {
		bail!(
			"CTF conversion failed with status {}: {}",
			result.status,
			String::from_utf8_lossy(&result.stderr).trim()
		);
	}
	let summary: Summary = serde_json::from_slice(&result.stdout).with_context(|| {
		format!(
			"CTF converter returned invalid summary: {}; stderr: {}",
			String::from_utf8_lossy(&result.stdout),
			String::from_utf8_lossy(&result.stderr).trim()
		)
	})?;
	if summary.events == 0 {
		bail!("CTF trace contains no moq_trace events");
	}
	if summary.discarded_events != 0 || summary.discarded_packets != 0 {
		bail!(
			"CTF capture lost {} events and {} packets",
			summary.discarded_events,
			summary.discarded_packets
		);
	}
	Ok(())
}
