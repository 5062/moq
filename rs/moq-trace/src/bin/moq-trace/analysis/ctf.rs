//! Babeltrace adapter for native LTTng CTF traces.

use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use moq_trace::Event;

const DECODER: &str = include_str!("../../../../scripts/ctf_events.py");
const SCHEMA: &str = include_str!("../../../../schema/events.json");

pub(super) fn read(input: &Path, python: &Path, expected_pid: Option<u32>) -> Result<Vec<Event>> {
	let mut command = Command::new(python);
	command
		.arg("-c")
		.arg(DECODER)
		.arg(input)
		.arg("--schema-json")
		.arg(SCHEMA)
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	if let Some(pid) = expected_pid {
		command.arg("--expected-pid").arg(pid.to_string());
	}
	let mut child = command
		.spawn()
		.with_context(|| format!("failed to start CTF decoder with {}", python.display()))?;
	let stdout = child.stdout.take().expect("piped decoder stdout is available");
	let mut stderr = child.stderr.take().expect("piped decoder stderr is available");
	let stderr = std::thread::spawn(move || {
		let mut output = String::new();
		stderr.read_to_string(&mut output).map(|_| output)
	});

	let mut events = Vec::new();
	let mut decode_error = None;
	for (line, value) in BufReader::new(stdout).lines().enumerate() {
		match value
			.with_context(|| format!("failed to read decoded CTF event {}", line + 1))
			.and_then(|value| {
				serde_json::from_str(&value).with_context(|| format!("invalid decoded CTF event {}", line + 1))
			}) {
			Ok(event) if decode_error.is_none() => events.push(event),
			Ok(_) => {}
			Err(error) if decode_error.is_none() => decode_error = Some(error),
			Err(_) => {}
		}
	}
	let status = child.wait().context("failed to wait for CTF decoder")?;
	let stderr = stderr
		.join()
		.map_err(|_| anyhow::anyhow!("CTF decoder stderr reader panicked"))?
		.context("failed to read CTF decoder stderr")?;
	if !status.success() {
		bail!("CTF decoder failed with status {status}: {}", stderr.trim());
	}
	if let Some(error) = decode_error {
		return Err(error);
	}
	if events.is_empty() {
		bail!("CTF trace contains no moq_trace events");
	}
	Ok(events)
}
