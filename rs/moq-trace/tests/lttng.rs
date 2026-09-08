#![cfg(target_os = "linux")]

use std::io::{BufRead, Read, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use moq_trace::{Config, Direction, Handle, LogicalId, ObjectContext, ObjectIdentity};

const DECODER: &str = include_str!("../scripts/ctf_events.py");

#[path = "../src/bin/moq-trace/experiment/lttng.rs"]
mod lttng;

fn emit_object(handle: &Handle, group: u64) {
	handle
		.object(ObjectContext::new(
			Direction::Rx,
			ObjectIdentity::new(1, group, 3),
			LogicalId::new(group, 3),
		))
		.finish();
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
			.args(["-c", "import bt2"])
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
		.arg(DECODER)
		.arg(&ctf)
		.arg("--expected-pid")
		.arg(helper_pid.to_string())
		.output()
		.unwrap();
	assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
	let events = String::from_utf8(result.stdout)
		.unwrap()
		.lines()
		.map(|line| serde_json::from_str(line).unwrap())
		.collect::<Vec<serde_json::Value>>();
	let start = events
		.iter()
		.find(|event| event["type"] == "moq_object_start" && event["logical_id"]["group"] == 999)
		.unwrap();
	assert_eq!(start["direction"], "rx");
	assert_eq!(start["protocol"], "moq_transport");
	assert!(!events.iter().any(|event| event["logical_id"]["group"] == 222));
}
