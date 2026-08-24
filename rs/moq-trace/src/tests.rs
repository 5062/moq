use std::io;
use std::sync::Arc;

use super::*;

fn trace() -> (tempfile::TempDir, PathBuf, Handle) {
	let directory = tempfile::tempdir().unwrap();
	let path = directory.path().join("trace.jsonl");
	let handle = Handle::new(Config {
		path: Some(path.clone()),
		..Config::default()
	})
	.unwrap();
	(directory, path, handle)
}

fn read(path: &std::path::Path) -> Vec<Event> {
	std::fs::read_to_string(path)
		.unwrap()
		.lines()
		.map(|line| serde_json::from_str(line).unwrap())
		.collect()
}

#[test]
fn writer_starts_with_exact_header() {
	let (_directory, path, handle) = trace();
	drop(handle);
	assert_eq!(
		read(&path),
		vec![Event::TraceHeader(TraceHeaderEvent {
			revision: TRACE_REVISION,
			clock: TraceClock::MonotonicNs,
		})]
	);
}

#[test]
fn object_children_only_reference_the_start_record() {
	let (_directory, path, handle) = trace();
	let logical_id = LogicalId::new(9, 4);
	let mut object = handle.object(
		ObjectContext::new(Direction::Rx, ObjectIdentity::new(11, 12, 13), logical_id)
			.with_session_id(7)
			.with_connection_id(42)
			.with_stream_id(16)
			.with_stream_offset_start(100),
	);
	let mut phase = object.phase(ObjectPhase::PayloadRead);
	phase.set_payload_bytes(44);
	phase.set_stream_offset_end(144);
	phase.finish(ObjectOutcome::Success);
	object.finish();
	drop(handle);

	let events = read(&path);
	assert_eq!(events.len(), 5);
	let trace_id = events[1].trace_id().unwrap();
	assert!(events[2..].iter().all(|event| event.trace_id() == Some(trace_id)));
	let start = serde_json::to_value(&events[1]).unwrap();
	assert_eq!(start["logical_id"]["group"], 9);
	assert_eq!(start["logical_id"]["frame"], 4);
	assert!(matches!(
		events.last(),
		Some(Event::MoqObjectEnd(ObjectEndEvent {
			payload_bytes: 44,
			stream_offset_end: Some(144),
			..
		}))
	));
	let phase = serde_json::to_value(&events[2]).unwrap();
	assert!(phase.get("session_id").is_none());
	assert!(phase.get("direction").is_none());
}

#[test]
fn logical_identity_samples_ingress_and_copies_together() {
	let directory = tempfile::tempdir().unwrap();
	let path = directory.path().join("trace.jsonl");
	let handle = Handle::new(Config {
		path: Some(path.clone()),
		object_sample: 2,
		..Config::default()
	})
	.unwrap();
	let sampled = (0..)
		.map(|frame| LogicalId::new(9, frame))
		.find(|logical_id| handle.object_sample_rate(*logical_id).is_some())
		.unwrap();
	for direction in [Direction::Rx, Direction::Tx, Direction::Tx] {
		handle
			.object(ObjectContext::new(direction, ObjectIdentity::new(1, 2, 3), sampled))
			.finish();
	}
	let skipped = (0..)
		.map(|frame| LogicalId::new(10, frame))
		.find(|logical_id| handle.object_sample_rate(*logical_id).is_none())
		.unwrap();
	for direction in [Direction::Rx, Direction::Tx] {
		handle
			.object(ObjectContext::new(direction, ObjectIdentity::new(1, 2, 4), skipped))
			.finish();
	}
	drop(handle);

	let starts = read(&path)
		.into_iter()
		.filter_map(|event| match event {
			Event::MoqObjectStart(event) => Some(event),
			_ => None,
		})
		.collect::<Vec<_>>();
	assert_eq!(starts.len(), 3);
	assert!(starts.iter().all(|event| event.logical_id == sampled));
}

#[test]
fn packet_end_contains_metadata_discovered_after_start() {
	let (_directory, path, handle) = trace();
	let mut packet = handle.packet(PacketContext::new(Direction::Rx, 7).with_byte_len(1200));
	packet.set_number(91);
	packet.set_space(PacketSpace::Data);
	packet.phase(PacketPhase::Routing).finish(PacketOutcome::Success);
	packet.finish(PacketOutcome::Success);
	drop(handle);

	assert!(matches!(
		read(&path).last(),
		Some(Event::PacketEnd(PacketEndEvent {
			packet_number: Some(91),
			packet_space: Some(PacketSpace::Data),
			byte_len: Some(1200),
			outcome: PacketOutcome::Success,
			..
		}))
	));
}

#[test]
fn strict_records_reject_unknown_fields() {
	let json = r#"{"type":"trace_header","revision":1,"clock":"monotonic_ns","legacy":true}"#;
	assert!(serde_json::from_str::<Event>(json).is_err());
}

#[test]
fn disabled_handle_is_noop() {
	let handle = Handle::new(Config::disabled()).unwrap();
	handle
		.object(ObjectContext::new(
			Direction::Rx,
			ObjectIdentity::new(1, 2, 3),
			LogicalId::new(4, 5),
		))
		.finish();
	assert_eq!(handle.emitted(), 0);
}

#[test]
fn global_destination_is_install_once() {
	let handle = Handle::disabled();
	install_global(&handle).unwrap();
	assert!(matches!(install_global(&handle), Err(Error::GlobalAlreadyInstalled)));
}

struct FailingWriter;

impl Write for FailingWriter {
	fn write(&mut self, _buffer: &[u8]) -> io::Result<usize> {
		Err(io::Error::other("expected test failure"))
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

#[test]
fn records_header_write_failure() {
	let failed = Arc::new(AtomicBool::new(false));
	let (sender, receiver) = std::sync::mpsc::sync_channel(1);
	drop(sender);
	write_events(FailingWriter, receiver, failed.clone());
	assert!(failed.load(Ordering::Relaxed));
}
