#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::Command;

use moq_trace::{Config, Direction, Handle, LogicalId, ObjectContext, ObjectIdentity};

const DECODER: &str = include_str!("../scripts/ctf_events.py");
const SCHEMA: &str = include_str!("../schema/events.json");

struct Session {
	name: String,
	output: PathBuf,
	active: bool,
}

impl Session {
	fn new(root: &Path, suffix: &str, event: &str, pid: u32) -> Self {
		let name = format!("moq-trace-test-{}-{suffix}", std::process::id());
		let output = root.join(format!("{suffix}.ctf"));
		run(["create", &name, "--output", output.to_str().unwrap()]);
		run(["enable-channel", "--userspace", "--session", &name, "--discard", "moq"]);
		run(["untrack", "--userspace", "--session", &name, "--vpid", "--all"]);
		run(["track", "--userspace", "--session", &name, &format!("--vpid={pid}")]);
		run([
			"add-context",
			"--userspace",
			"--session",
			&name,
			"--channel",
			"moq",
			"--type",
			"vpid",
		]);
		run([
			"enable-event",
			"--userspace",
			"--session",
			&name,
			"--channel",
			"moq",
			event,
		]);
		run(["start", &name]);
		Self {
			name,
			output,
			active: true,
		}
	}

	fn finish(mut self) -> PathBuf {
		run(["stop", &self.name]);
		run(["destroy", &self.name]);
		self.active = false;
		self.output.clone()
	}
}

impl Drop for Session {
	fn drop(&mut self) {
		if self.active {
			let _ = Command::new("lttng").args(["destroy", &self.name]).status();
		}
	}
}

fn run<const N: usize>(args: [&str; N]) {
	let status = Command::new("lttng").args(args).status().unwrap();
	assert!(status.success());
}

fn emit_object(handle: &Handle, group: u64) {
	handle
		.object(ObjectContext::new(
			Direction::Rx,
			ObjectIdentity::new(1, group, 3),
			LogicalId::new(group, 3),
		))
		.finish();
}

fn decode(ctf: &Path) -> Vec<serde_json::Value> {
	let result = Command::new("python3")
		.arg("-c")
		.arg(DECODER)
		.arg(ctf)
		.arg("--schema-json")
		.arg(SCHEMA)
		.arg("--expected-pid")
		.arg(std::process::id().to_string())
		.output()
		.unwrap();
	assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
	String::from_utf8(result.stdout)
		.unwrap()
		.lines()
		.map(|line| serde_json::from_str(line).unwrap())
		.collect()
}

#[test]
fn helper_process() {
	let Ok(group) = std::env::var("MOQ_TRACE_TEST_HELPER_GROUP") else {
		return;
	};
	let handle = Handle::new(Config::default());
	emit_object(&handle, group.parse().unwrap());
}

#[test]
fn records_real_ctf_with_event_and_process_filtering() {
	if !Command::new("lttng")
		.arg("--version")
		.status()
		.is_ok_and(|status| status.success())
		|| !Command::new("python3")
			.args(["-c", "import bt2"])
			.status()
			.is_ok_and(|status| status.success())
	{
		return;
	}

	let root = tempfile::tempdir().unwrap();
	let handle = Handle::new(Config::default());
	let end = Session::new(root.path(), "end", "moq_trace:moq_object_end", std::process::id());
	emit_object(&handle, 111);
	let events = decode(&end.finish());
	assert_eq!(events.len(), 1);
	assert_eq!(events[0]["type"], "moq_object_end");

	let all = Session::new(root.path(), "isolated", "moq_trace:*", std::process::id());
	let helper = Command::new(std::env::current_exe().unwrap())
		.args(["--exact", "helper_process"])
		.env("MOQ_TRACE_TEST_HELPER_GROUP", "999")
		.status()
		.unwrap();
	assert!(helper.success());
	emit_object(&handle, 222);
	let events = decode(&all.finish());
	assert!(events.iter().any(|event| event["logical_id"]["group"] == 222));
	assert!(!events.iter().any(|event| event["logical_id"]["group"] == 999));
}
