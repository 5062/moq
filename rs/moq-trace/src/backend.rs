//! LTTng-UST event adapter.

use crate::Event;

#[allow(dead_code)]
mod ffi {
	include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

pub(crate) const TRACE_REVISION: u32 = ffi::TRACE_REVISION;

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
				ffi::moq_trace_moq_object_start(&ffi::MoqObjectStart {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					logical_group: event.logical_id.group(),
					logical_frame: event.logical_id.frame(),
					has_session_id,
					session_id,
					has_connection_id,
					connection_id,
					direction: ffi::direction(event.direction),
					protocol: ffi::protocol(event.protocol),
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
				ffi::moq_trace_moq_object_end(&ffi::MoqObjectEnd {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					has_stream_offset_end,
					stream_offset_end,
					payload_bytes: event.payload_bytes,
				});
				true
			}
			Event::MoqObjectPhase(event) if ffi::moq_trace_moq_object_phase_enabled() => {
				let (has_outcome, outcome) = optional_enum(event.outcome, ffi::object_outcome);
				ffi::moq_trace_moq_object_phase(&ffi::MoqObjectPhase {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					phase: ffi::object_phase(event.phase),
					edge: ffi::edge(event.edge),
					has_outcome,
					outcome,
				});
				true
			}
			Event::PacketStart(event) if ffi::moq_trace_quic_packet_start_enabled() => {
				let (has_packet_number, packet_number) = optional(event.packet_number);
				let (has_packet_space, packet_space) = optional_enum(event.packet_space, ffi::packet_space);
				let (has_byte_len, byte_len) = optional(event.byte_len.map(to_u64));
				ffi::moq_trace_quic_packet_start(&ffi::QuicPacketStart {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					connection_id: event.connection_id,
					direction: ffi::direction(event.direction),
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
				let (has_packet_space, packet_space) = optional_enum(event.packet_space, ffi::packet_space);
				let (has_byte_len, byte_len) = optional(event.byte_len.map(to_u64));
				ffi::moq_trace_quic_packet_end(&ffi::QuicPacketEnd {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					has_packet_number,
					packet_number,
					has_packet_space,
					packet_space,
					has_byte_len,
					byte_len,
					outcome: ffi::packet_outcome(event.outcome),
				});
				true
			}
			Event::PacketPhase(event) if ffi::moq_trace_quic_packet_phase_enabled() => {
				let (has_outcome, outcome) = optional_enum(event.outcome, ffi::packet_outcome);
				ffi::moq_trace_quic_packet_phase(&ffi::QuicPacketPhase {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					phase: ffi::packet_phase(event.phase),
					edge: ffi::edge(event.edge),
					has_outcome,
					outcome,
				});
				true
			}
			Event::StreamFrame(event) if ffi::moq_trace_quic_stream_frame_enabled() => {
				ffi::moq_trace_quic_stream_frame(&ffi::QuicStreamFrame {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					stream_id: event.stream_id,
					offset_start: event.offset_start,
					offset_end: event.offset_end,
					outcome: ffi::packet_outcome(event.outcome),
				});
				true
			}
			Event::SocketStart(event) if ffi::moq_trace_udp_socket_start_enabled() => {
				let (has_connection_id, connection_id) = optional(event.connection_id);
				ffi::moq_trace_udp_socket_start(&ffi::UdpSocketStart {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					has_connection_id,
					connection_id,
					direction: ffi::direction(event.direction),
					sample_rate: event.sample_rate,
				});
				true
			}
			Event::SocketEnd(event) if ffi::moq_trace_udp_socket_end_enabled() => {
				ffi::moq_trace_udp_socket_end(&ffi::UdpSocketEnd {
					timestamp_ns: event.timestamp_ns,
					trace_id: event.trace_id,
					outcome: ffi::socket_outcome(event.outcome),
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
	// Keep the statically linked provider's registration section in downstream binaries.
	unsafe { ffi::moq_trace_provider_keep_sections() };
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn emit(_event: &Event) -> bool {
	false
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn initialize() {}

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
