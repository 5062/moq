//! LTTng recording session lifecycle.

use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

const CHANNEL: &str = "moq";

pub(super) struct Session {
	name: String,
	active: bool,
}

impl Session {
	pub(super) fn create(output: &Path) -> Result<Self> {
		let name = format!("moq-trace-{}", std::process::id());
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
			if output.status.success()
				&& listing.contains(&format!("PID: {pid}"))
				&& listing.contains("moq_trace:udp_socket_end")
			{
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

impl Drop for Session {
	fn drop(&mut self) {
		if self.active {
			let _ = Command::new("lttng").args(["destroy", &self.name]).status();
		}
	}
}
