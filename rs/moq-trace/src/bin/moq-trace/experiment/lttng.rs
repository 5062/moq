//! LTTng recording session lifecycle.

use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

const CHANNEL: &str = "moq";
static NEXT_SESSION: AtomicU64 = AtomicU64::new(0);

pub(super) struct Session {
	name: String,
	active: bool,
}

impl Session {
	pub(super) fn create(output: &Path) -> Result<Self> {
		let nonce = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.context("system clock is before the Unix epoch")?
			.as_nanos();
		let sequence = NEXT_SESSION.fetch_add(1, Ordering::Relaxed);
		let name = format!("moq-trace-{}-{nonce}-{sequence}", std::process::id());
		let mut session = Self { name, active: false };
		session.command(["create", &session.name, "--output", &output.display().to_string()])?;
		session.active = true;
		session.command([
			"enable-channel",
			"--userspace",
			"--session",
			&session.name,
			"--discard",
			"--subbuf-size",
			"8M",
			"--num-subbuf",
			"8",
			CHANNEL,
		])?;
		Ok(session)
	}

	pub(super) fn wait_for_provider(&self, pid: u32, timeout: Duration) -> Result<()> {
		let deadline = Instant::now() + timeout;
		loop {
			let output = Command::new("lttng")
				.args(["list", "--userspace"])
				.output()
				.context("failed to inspect LTTng userspace providers")?;
			let listing = String::from_utf8_lossy(&output.stdout);
			if output.status.success() && provider_listed(&listing, pid, "moq_trace:udp_socket_end") {
				return Ok(());
			}
			if Instant::now() >= deadline {
				bail!("timed out waiting for LTTng provider moq_trace from relay PID {pid}");
			}
			thread::sleep(Duration::from_millis(50));
		}
	}

	pub(super) fn start(&self, pid: u32) -> Result<()> {
		self.command(["untrack", "--userspace", "--session", &self.name, "--vpid", "--all"])?;
		let pid = pid.to_string();
		self.command([
			"track",
			"--userspace",
			"--session",
			&self.name,
			&format!("--vpid={pid}"),
		])?;
		self.command([
			"add-context",
			"--userspace",
			"--session",
			&self.name,
			"--channel",
			CHANNEL,
			"--type",
			"vpid",
		])?;
		self.command([
			"enable-event",
			"--userspace",
			"--session",
			&self.name,
			"--channel",
			CHANNEL,
			"moq_trace:*",
		])?;
		self.command(["start", &self.name])
	}

	pub(super) fn finish(mut self) -> Result<()> {
		self.command(["stop", &self.name])?;
		self.command(["destroy", &self.name])?;
		self.active = false;
		Ok(())
	}

	fn command<const N: usize>(&self, args: [&str; N]) -> Result<()> {
		let status = Command::new("lttng")
			.args(args)
			.status()
			.context("failed to execute lttng; install LTTng tools 2.13 or newer")?;
		if !status.success() {
			bail!("lttng command failed with status {status}");
		}
		Ok(())
	}
}

fn provider_listed(listing: &str, pid: u32, event: &str) -> bool {
	let mut target = false;
	for line in listing.lines() {
		let line = line.trim();
		if let Some(value) = line.strip_prefix("PID: ") {
			target = value.split_whitespace().next().and_then(|value| value.parse().ok()) == Some(pid);
		} else if target && line.split_whitespace().next() == Some(event) {
			return true;
		}
	}
	false
}

impl Drop for Session {
	fn drop(&mut self) {
		if self.active {
			let _ = Command::new("lttng").args(["destroy", &self.name]).status();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::provider_listed;

	#[test]
	fn provider_must_belong_to_exact_pid() {
		let listing = "PID: 1234 - Name: other\n  moq_trace:udp_socket_end\nPID: 123 - Name: relay\n  moq_trace:udp_socket_end_extra\n";
		assert!(!provider_listed(listing, 123, "moq_trace:udp_socket_end"));
		assert!(provider_listed(listing, 1234, "moq_trace:udp_socket_end"));
	}
}
