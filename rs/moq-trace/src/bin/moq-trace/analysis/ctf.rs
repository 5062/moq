//! Babeltrace adapter for native LTTng CTF traces.

use std::io::{BufRead, BufReader, Read};
use std::process::{Child, ChildStdout, Command, Stdio};

use anyhow::{Context, Result, bail};
use moq_trace::Event;

use super::Source;

const DECODER: &str = include_str!("../../../../scripts/ctf_events.py");

pub(super) struct Decoder {
	child: Child,
	lines: std::iter::Enumerate<std::io::Lines<BufReader<ChildStdout>>>,
	stderr: Option<std::thread::JoinHandle<std::io::Result<String>>>,
	finished: bool,
}

impl Decoder {
	pub(super) fn spawn(source: Source<'_>) -> Result<Self> {
		let mut command = Command::new(source.python);
		command
			.arg("-c")
			.arg(DECODER)
			.arg(source.ctf)
			.stdout(Stdio::piped())
			.stderr(Stdio::piped());
		if let Some(pid) = source.expected_pid {
			command.arg("--expected-pid").arg(pid.to_string());
		}
		let mut child = command
			.spawn()
			.with_context(|| format!("failed to start CTF decoder with {}", source.python.display()))?;
		let stdout = child.stdout.take().expect("piped decoder stdout is available");
		let mut stderr = child.stderr.take().expect("piped decoder stderr is available");
		let stderr = std::thread::spawn(move || {
			let mut output = String::new();
			stderr.read_to_string(&mut output).map(|_| output)
		});
		Ok(Self {
			child,
			lines: BufReader::new(stdout).lines().enumerate(),
			stderr: Some(stderr),
			finished: false,
		})
	}

	fn finish(&mut self) -> Result<()> {
		let status = self.child.wait().context("failed to wait for CTF decoder")?;
		let stderr = self
			.stderr
			.take()
			.expect("decoder stderr reader is available")
			.join()
			.map_err(|_| anyhow::anyhow!("CTF decoder stderr reader panicked"))?
			.context("failed to read CTF decoder stderr")?;
		if !status.success() {
			bail!("CTF decoder failed with status {status}: {}", stderr.trim());
		}
		Ok(())
	}
}

impl Iterator for Decoder {
	type Item = Result<Event>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.finished {
			return None;
		}
		let Some((line, value)) = self.lines.next() else {
			self.finished = true;
			return self.finish().err().map(Err);
		};
		Some(
			value
				.with_context(|| format!("failed to read decoded CTF event {}", line + 1))
				.and_then(|value| {
					serde_json::from_str(&value).with_context(|| format!("invalid decoded CTF event {}", line + 1))
				}),
		)
	}
}

impl Drop for Decoder {
	fn drop(&mut self) {
		if !self.finished {
			let _ = self.child.kill();
			let _ = self.child.wait();
		}
		if let Some(stderr) = self.stderr.take() {
			let _ = stderr.join();
		}
	}
}
