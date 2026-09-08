//! LTTng-UST event adapter.

use crate::Event;

#[allow(dead_code, non_camel_case_types, non_upper_case_globals)]
#[cfg(target_os = "linux")]
mod ffi {
	include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[cfg(all(target_os = "linux", not(test)))]
pub(crate) fn object_enabled() -> bool {
	unsafe {
		ffi::moq_trace_moq_object_start_enabled()
			|| ffi::moq_trace_moq_object_phase_enabled()
			|| ffi::moq_trace_moq_object_end_enabled()
	}
}

#[cfg(all(target_os = "linux", not(test)))]
pub(crate) fn packet_enabled() -> bool {
	unsafe {
		ffi::moq_trace_quic_packet_start_enabled()
			|| ffi::moq_trace_quic_packet_phase_enabled()
			|| ffi::moq_trace_quic_stream_frame_enabled()
			|| ffi::moq_trace_quic_packet_end_enabled()
	}
}

#[cfg(all(target_os = "linux", not(test)))]
pub(crate) fn socket_enabled() -> bool {
	unsafe { ffi::moq_trace_udp_socket_start_enabled() || ffi::moq_trace_udp_socket_end_enabled() }
}

#[cfg(target_os = "linux")]
pub(crate) fn emit(event: &Event) -> bool {
	unsafe {
		match event {
			Event::MoqObjectStart(event) if ffi::moq_trace_moq_object_start_enabled() => {
				let (has_session_id, session_id) = optional(event.session_id);
				let (has_connection_id, connection_id) = optional(event.connection_id);
				let (has_stream_id, stream_id) = optional(event.stream_id);
				let (has_stream_offset_start, stream_offset_start) = optional(event.stream_offset_start);
				ffi::moq_trace_moq_object_start(&ffi::moq_trace_moq_object_start {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					logical_group: event.logical_id.group(),
					logical_frame: event.logical_id.frame(),
					has_session_id,
					session_id,
					has_connection_id,
					connection_id,
					direction: direction(event.direction),
					protocol: protocol(event.protocol),
					track_alias: event.track_alias,
					group_id: event.group_id,
					object_id: event.object_id,
					has_stream_id,
					stream_id,
					has_stream_offset_start,
					stream_offset_start,
					sample_rate: event.sample_rate,
				});
				true
			}
			Event::MoqObjectEnd(event) if ffi::moq_trace_moq_object_end_enabled() => {
				let (has_stream_offset_end, stream_offset_end) = optional(event.stream_offset_end);
				ffi::moq_trace_moq_object_end(&ffi::moq_trace_moq_object_end {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					has_stream_offset_end,
					stream_offset_end,
					payload_bytes: event.payload_bytes,
				});
				true
			}
			Event::MoqObjectPhase(event) if ffi::moq_trace_moq_object_phase_enabled() => {
				let (has_outcome, outcome) = optional_enum(event.outcome, object_outcome);
				ffi::moq_trace_moq_object_phase(&ffi::moq_trace_moq_object_phase {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					phase: object_phase(event.phase),
					edge: edge(event.edge),
					has_outcome,
					outcome,
				});
				true
			}
			Event::PacketStart(event) if ffi::moq_trace_quic_packet_start_enabled() => {
				let (has_packet_number, packet_number) = optional(event.packet_number);
				let (has_packet_space, packet_space) = optional_enum(event.packet_space, packet_space);
				let (has_byte_len, byte_len) = optional(event.byte_len.map(to_u64));
				ffi::moq_trace_quic_packet_start(&ffi::moq_trace_quic_packet_start {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					connection_id: event.connection_id,
					direction: direction(event.direction),
					has_packet_number,
					packet_number,
					has_packet_space,
					packet_space,
					has_byte_len,
					byte_len,
					sample_rate: event.sample_rate,
				});
				true
			}
			Event::PacketEnd(event) if ffi::moq_trace_quic_packet_end_enabled() => {
				let (has_packet_number, packet_number) = optional(event.packet_number);
				let (has_packet_space, packet_space) = optional_enum(event.packet_space, packet_space);
				let (has_byte_len, byte_len) = optional(event.byte_len.map(to_u64));
				ffi::moq_trace_quic_packet_end(&ffi::moq_trace_quic_packet_end {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					has_packet_number,
					packet_number,
					has_packet_space,
					packet_space,
					has_byte_len,
					byte_len,
					outcome: packet_outcome(event.outcome),
				});
				true
			}
			Event::PacketPhase(event) if ffi::moq_trace_quic_packet_phase_enabled() => {
				let (has_outcome, outcome) = optional_enum(event.outcome, packet_outcome);
				ffi::moq_trace_quic_packet_phase(&ffi::moq_trace_quic_packet_phase {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					phase: packet_phase(event.phase),
					edge: edge(event.edge),
					has_outcome,
					outcome,
				});
				true
			}
			Event::StreamFrame(event) if ffi::moq_trace_quic_stream_frame_enabled() => {
				ffi::moq_trace_quic_stream_frame(&ffi::moq_trace_quic_stream_frame {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					stream_id: event.stream_id,
					offset_start: event.offset_start,
					offset_end: event.offset_end,
					outcome: packet_outcome(event.outcome),
				});
				true
			}
			Event::SocketStart(event) if ffi::moq_trace_udp_socket_start_enabled() => {
				let (has_connection_id, connection_id) = optional(event.connection_id);
				ffi::moq_trace_udp_socket_start(&ffi::moq_trace_udp_socket_start {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					has_connection_id,
					connection_id,
					direction: direction(event.direction),
					sample_rate: event.sample_rate,
				});
				true
			}
			Event::SocketEnd(event) if ffi::moq_trace_udp_socket_end_enabled() => {
				ffi::moq_trace_udp_socket_end(&ffi::moq_trace_udp_socket_end {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					outcome: socket_outcome(event.outcome),
					buffers: to_u64(event.stats.buffers),
					datagrams: to_u64(event.stats.datagrams),
					bytes: to_u64(event.stats.bytes),
				});
				true
			}
			_ => false,
		}
	}
}

#[cfg(target_os = "linux")]
pub(crate) fn initialize() {
	unsafe { ffi::moq_trace_provider_init() };
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn emit(_event: &Event) -> bool {
	false
}

#[cfg(all(not(target_os = "linux"), not(test)))]
pub(crate) fn object_enabled() -> bool {
	false
}

#[cfg(all(not(target_os = "linux"), not(test)))]
pub(crate) fn packet_enabled() -> bool {
	false
}

#[cfg(all(not(target_os = "linux"), not(test)))]
pub(crate) fn socket_enabled() -> bool {
	false
}

#[cfg(test)]
pub(crate) fn object_enabled() -> bool {
	true
}

#[cfg(test)]
pub(crate) fn packet_enabled() -> bool {
	true
}

#[cfg(test)]
pub(crate) fn socket_enabled() -> bool {
	true
}

fn optional(value: Option<u64>) -> (u8, u64) {
	value.map_or((0, 0), |value| (1, value))
}
fn optional_enum<T>(value: Option<T>, convert: fn(T) -> u8) -> (u8, u8) {
	value.map_or((0, 0), |value| (1, convert(value)))
}
fn to_u64(value: usize) -> u64 {
	value.try_into().unwrap_or(u64::MAX)
}

fn direction(value: crate::Direction) -> u8 {
	match value {
		crate::Direction::Rx => ffi::moq_trace_direction_MOQ_TRACE_DIRECTION_RX as u8,
		crate::Direction::Tx => ffi::moq_trace_direction_MOQ_TRACE_DIRECTION_TX as u8,
	}
}

fn protocol(value: crate::Protocol) -> u8 {
	match value {
		crate::Protocol::MoqTransport => ffi::moq_trace_protocol_MOQ_TRACE_PROTOCOL_MOQ_TRANSPORT as u8,
	}
}

fn edge(value: crate::PhaseEdge) -> u8 {
	match value {
		crate::PhaseEdge::Start => ffi::moq_trace_edge_MOQ_TRACE_EDGE_START as u8,
		crate::PhaseEdge::Done => ffi::moq_trace_edge_MOQ_TRACE_EDGE_DONE as u8,
	}
}

fn packet_space(value: crate::PacketSpace) -> u8 {
	match value {
		crate::PacketSpace::Initial => ffi::moq_trace_packet_space_MOQ_TRACE_PACKET_SPACE_INITIAL as u8,
		crate::PacketSpace::Handshake => ffi::moq_trace_packet_space_MOQ_TRACE_PACKET_SPACE_HANDSHAKE as u8,
		crate::PacketSpace::ZeroRtt => ffi::moq_trace_packet_space_MOQ_TRACE_PACKET_SPACE_ZERO_RTT as u8,
		crate::PacketSpace::Data => ffi::moq_trace_packet_space_MOQ_TRACE_PACKET_SPACE_DATA as u8,
	}
}

fn object_phase(value: crate::ObjectPhase) -> u8 {
	match value {
		crate::ObjectPhase::HeaderParse => ffi::moq_trace_object_phase_MOQ_TRACE_OBJECT_PHASE_HEADER_PARSE as u8,
		crate::ObjectPhase::Create => ffi::moq_trace_object_phase_MOQ_TRACE_OBJECT_PHASE_CREATE as u8,
		crate::ObjectPhase::PayloadRead => ffi::moq_trace_object_phase_MOQ_TRACE_OBJECT_PHASE_PAYLOAD_READ as u8,
		crate::ObjectPhase::FrameCommit => ffi::moq_trace_object_phase_MOQ_TRACE_OBJECT_PHASE_FRAME_COMMIT as u8,
		crate::ObjectPhase::Clone => ffi::moq_trace_object_phase_MOQ_TRACE_OBJECT_PHASE_CLONE as u8,
		crate::ObjectPhase::HeaderEncode => ffi::moq_trace_object_phase_MOQ_TRACE_OBJECT_PHASE_HEADER_ENCODE as u8,
		crate::ObjectPhase::PayloadWrite => ffi::moq_trace_object_phase_MOQ_TRACE_OBJECT_PHASE_PAYLOAD_WRITE as u8,
	}
}

fn object_outcome(value: crate::ObjectOutcome) -> u8 {
	match value {
		crate::ObjectOutcome::Success => ffi::moq_trace_object_outcome_MOQ_TRACE_OBJECT_OUTCOME_SUCCESS as u8,
		crate::ObjectOutcome::Failed => ffi::moq_trace_object_outcome_MOQ_TRACE_OBJECT_OUTCOME_FAILED as u8,
		crate::ObjectOutcome::Abandoned => ffi::moq_trace_object_outcome_MOQ_TRACE_OBJECT_OUTCOME_ABANDONED as u8,
	}
}

fn packet_phase(value: crate::PacketPhase) -> u8 {
	match value {
		crate::PacketPhase::HeaderParse => ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_HEADER_PARSE as u8,
		crate::PacketPhase::Routing => ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_ROUTING as u8,
		crate::PacketPhase::Scheduling => ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_SCHEDULING as u8,
		crate::PacketPhase::HeaderUnprotect => {
			ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_HEADER_UNPROTECT as u8
		}
		crate::PacketPhase::PayloadDecrypt => ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_PAYLOAD_DECRYPT as u8,
		crate::PacketPhase::FrameProcess => ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_FRAME_PROCESS as u8,
		crate::PacketPhase::FrameEncode => ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_FRAME_ENCODE as u8,
		crate::PacketPhase::PacketEncrypt => ffi::moq_trace_packet_phase_MOQ_TRACE_PACKET_PHASE_PACKET_ENCRYPT as u8,
	}
}

fn packet_outcome(value: crate::PacketOutcome) -> u8 {
	match value {
		crate::PacketOutcome::Success => ffi::moq_trace_packet_outcome_MOQ_TRACE_PACKET_OUTCOME_SUCCESS as u8,
		crate::PacketOutcome::Malformed => ffi::moq_trace_packet_outcome_MOQ_TRACE_PACKET_OUTCOME_MALFORMED as u8,
		crate::PacketOutcome::AuthenticationFailed => {
			ffi::moq_trace_packet_outcome_MOQ_TRACE_PACKET_OUTCOME_AUTHENTICATION_FAILED as u8
		}
		crate::PacketOutcome::Dropped => ffi::moq_trace_packet_outcome_MOQ_TRACE_PACKET_OUTCOME_DROPPED as u8,
		crate::PacketOutcome::Abandoned => ffi::moq_trace_packet_outcome_MOQ_TRACE_PACKET_OUTCOME_ABANDONED as u8,
	}
}

fn socket_outcome(value: crate::SocketOutcome) -> u8 {
	match value {
		crate::SocketOutcome::Success => ffi::moq_trace_socket_outcome_MOQ_TRACE_SOCKET_OUTCOME_SUCCESS as u8,
		crate::SocketOutcome::Pending => ffi::moq_trace_socket_outcome_MOQ_TRACE_SOCKET_OUTCOME_PENDING as u8,
		crate::SocketOutcome::WouldBlock => ffi::moq_trace_socket_outcome_MOQ_TRACE_SOCKET_OUTCOME_WOULD_BLOCK as u8,
		crate::SocketOutcome::ConnectionReset => {
			ffi::moq_trace_socket_outcome_MOQ_TRACE_SOCKET_OUTCOME_CONNECTION_RESET as u8
		}
		crate::SocketOutcome::Error => ffi::moq_trace_socket_outcome_MOQ_TRACE_SOCKET_OUTCOME_ERROR as u8,
		crate::SocketOutcome::Abandoned => ffi::moq_trace_socket_outcome_MOQ_TRACE_SOCKET_OUTCOME_ABANDONED as u8,
	}
}
