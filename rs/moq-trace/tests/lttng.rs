#![cfg(target_os = "linux")]

use std::io::{BufRead, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use moq_trace::{Config, Direction, Handle, LogicalId, ObjectContext, ObjectIdentity, ObjectOutcome, ObjectPhase};

const INSPECT: &str = r#"
import pathlib
import sys
from relay_latency_lib.ctf import batches

for name, batch in batches(pathlib.Path(sys.argv[1]), int(sys.argv[2])):
    if name != "moq_object_start":
        continue
    for row in batch.to_pylist():
        print(f"{row['logical_group']},{row['direction']},{row['protocol']}")
"#;

#[path = "../src/bin/moq-trace/experiment/lttng.rs"]
mod lttng;

fn emit_object(handle: &Handle, group: u64) {
	let mut trace = handle.object(ObjectContext::new(
		Direction::Rx,
		ObjectIdentity::new(1, group, 3),
		LogicalId::new(group, 3),
	));
	trace.phase(ObjectPhase::Create).finish(ObjectOutcome::Success);
	trace.finish();
}

#[test]
fn helper_process() {
	let Ok(group) = std::env::var("MOQ_TRACE_TEST_HELPER_GROUP") else {
		return;
	};
	moq_trace::install(Config::default()).unwrap();
	println!("ready");
	std::io::stdout().flush().unwrap();
	std::io::stdin().read_exact(&mut [0]).unwrap();
	emit_object(&moq_trace::global(), group.parse().unwrap());
}

#[test]
fn records_real_ctf_with_event_and_process_filtering() {
	if !Command::new("lttng")
		.arg("--version")
		.status()
		.is_ok_and(|status| status.success())
		|| !Command::new("python3")
			.args(["-c", "import bt2, pyarrow"])
			.status()
			.is_ok_and(|status| status.success())
	{
		return;
	}

	let root = tempfile::tempdir().unwrap();
	moq_trace::install(Config::default()).unwrap();
	let parent_handle = moq_trace::global();
	let ctf = root.path().join("trace.ctf");
	let session = lttng::Session::create(&ctf).unwrap();
	let mut helper = Command::new(std::env::current_exe().unwrap())
		.args(["--exact", "helper_process", "--nocapture"])
		.env("MOQ_TRACE_TEST_HELPER_GROUP", "999")
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.spawn()
		.unwrap();
	let mut output = std::io::BufReader::new(helper.stdout.take().unwrap());
	let mut ready = String::new();
	loop {
		ready.clear();
		assert!(
			output.read_line(&mut ready).unwrap() > 0,
			"helper exited before becoming ready"
		);
		if ready == "ready\n" {
			break;
		}
	}
	session.wait_for_provider(helper.id(), Duration::from_secs(10)).unwrap();
	session.start(helper.id()).unwrap();
	emit_object(&parent_handle, 222);
	helper.stdin.take().unwrap().write_all(&[0]).unwrap();
	let helper_pid = helper.id();
	assert!(helper.wait().unwrap().success());
	session.finish().unwrap();
	let result = Command::new("python3")
		.arg("-c")
		.arg(INSPECT)
		.arg(&ctf)
		.arg(helper_pid.to_string())
		.env("PYTHONPATH", Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts"))
		.output()
		.unwrap();
	assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
	assert_eq!(String::from_utf8(result.stdout).unwrap(), "999,rx,moq_transport\n");
}
