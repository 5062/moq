use super::*;

fn trace() -> Handle {
	Handle::new(Config::default())
}

#[test]
fn object_children_only_reference_the_start_record() {
	let handle = trace();
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
	let events = handle.events();
	assert_eq!(events.len(), 4);
	let trace_id = events[0].trace_id().unwrap();
	assert!(events[1..].iter().all(|event| event.trace_id() == Some(trace_id)));
	let start = serde_json::to_value(&events[0]).unwrap();
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
	let phase = serde_json::to_value(&events[1]).unwrap();
	assert!(phase.get("session_id").is_none());
	assert!(phase.get("direction").is_none());
}

#[test]
fn packet_end_contains_metadata_discovered_after_start() {
	let handle = trace();
	let mut packet = handle.packet(PacketContext::new(Direction::Rx, 7).with_byte_len(1200));
	packet.set_number(91);
	packet.set_space(PacketSpace::Data);
	packet.phase(PacketPhase::Routing).finish(PacketOutcome::Success);
	packet.finish(PacketOutcome::Success);
	assert!(matches!(
		handle.events().last(),
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
	let json = r#"{"type":"moq_object_end","timestamp_ns":1,"trace_id":1,"stream_offset_end":null,"payload_bytes":1,"legacy":true}"#;
	assert!(serde_json::from_str::<Event>(json).is_err());
}

#[test]
fn disabled_handle_is_noop() {
	let handle = Handle::disabled();
	handle
		.object(ObjectContext::new(
			Direction::Rx,
			ObjectIdentity::new(1, 2, 3),
			LogicalId::new(4, 5),
		))
		.finish();
	assert!(handle.events().is_empty());
}

#[test]
fn global_destination_is_install_once() {
	install(Config::default()).unwrap();
	assert!(matches!(install(Config::default()), Err(Error::GlobalAlreadyInstalled)));
}
