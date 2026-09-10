use super::*;

fn trace() -> Handle {
	Handle::new()
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
	object.finish(ObjectOutcome::Success);
	let events = handle.events();
	assert_eq!(events.len(), 4);
	let trace_id = events[0].trace_id();
	assert!(events[1..].iter().all(|event| event.trace_id() == trace_id));
	assert!(matches!(
		events.first(),
		Some(backend::Event::ObjectStart { logical_id, .. })
			if logical_id.group() == 9 && logical_id.frame() == 4
	));
	assert!(matches!(
		events.last(),
		Some(backend::Event::ObjectEnd {
			payload_bytes: 44,
			stream_offset_end: Some(144),
			outcome: ObjectOutcome::Success,
			..
		})
	));
	let Some(backend::Event::ObjectPhase { span_id: start, .. }) = events.get(1) else {
		panic!("expected phase start");
	};
	let Some(backend::Event::ObjectPhase { span_id: done, .. }) = events.get(2) else {
		panic!("expected phase completion");
	};
	assert_ne!(*start, 0);
	assert_eq!(start, done);
}

#[test]
fn dropping_an_object_records_abandonment() {
	let handle = trace();
	let object = handle.object(ObjectContext::new(
		Direction::Rx,
		ObjectIdentity::new(1, 2, 3),
		LogicalId::new(4, 5),
	));
	drop(object);

	assert!(matches!(
		handle.events().last(),
		Some(backend::Event::ObjectEnd {
			outcome: ObjectOutcome::Abandoned,
			..
		})
	));
}

#[test]
fn packet_end_contains_metadata_discovered_after_start() {
	let handle = trace();
	let mut packet = handle.packet(PacketContext::new(Direction::Rx, 7).with_byte_len(1200));
	packet.set_number(91);
	packet.set_space(PacketSpace::Data);
	packet.phase(PacketPhase::Routing).finish(PacketOutcome::Success);
	packet.finish(PacketOutcome::Success);
	let events = handle.events();
	assert!(matches!(
		events.first(),
		Some(backend::Event::PacketStart {
			connection_id: 7,
			direction: Direction::Rx,
			..
		})
	));
	let Some(backend::Event::PacketPhase { span_id: start, .. }) = events.get(1) else {
		panic!("expected phase start");
	};
	let Some(backend::Event::PacketPhase { span_id: finish, .. }) = events.get(2) else {
		panic!("expected phase completion");
	};
	assert_eq!(start, finish);
	assert!(matches!(
		events.last(),
		Some(backend::Event::PacketEnd {
			packet_number: Some(91),
			packet_space: Some(PacketSpace::Data),
			byte_len: Some(1200),
			outcome: PacketOutcome::Success,
			..
		})
	));
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
		.finish(ObjectOutcome::Success);
	assert!(handle.events().is_empty());
}

#[test]
fn trace_ids_are_unique_across_handles() {
	let first = trace();
	let second = trace();
	first
		.socket(Direction::Rx, None)
		.unwrap()
		.finish(SocketOutcome::Success, SocketStats::default());
	second
		.socket(Direction::Rx, None)
		.unwrap()
		.finish(SocketOutcome::Success, SocketStats::default());
	assert_ne!(first.events()[0].trace_id(), second.events()[0].trace_id());
}

#[test]
#[cfg(not(feature = "lttng"))]
fn disabled_global_is_a_noop_without_the_lttng_feature() {
	let handle = global();
	handle
		.object(ObjectContext::new(
			Direction::Rx,
			ObjectIdentity::new(1, 2, 3),
			LogicalId::new(4, 5),
		))
		.finish(ObjectOutcome::Success);
	assert!(handle.events().is_empty());
}
