use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};

use crate::{Direction, Event, Handle, PacketSpace, now_ns};

/// Result of packet or packet-phase processing.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PacketOutcome {
	/// Processing completed successfully.
	Success,
	/// Input could not be parsed.
	Malformed,
	/// Packet authentication failed.
	AuthenticationFailed,
	/// Processing deliberately discarded the packet.
	Dropped,
	/// The trace token was dropped before an outcome was recorded.
	Abandoned,
}

/// A measured step in the QUIC packet lifecycle.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PacketPhase {
	/// Parse the protected packet header.
	HeaderParse,
	/// Route a decoded packet to its connection.
	Routing,
	/// Wait for the connection task to begin processing the packet.
	Scheduling,
	/// Remove QUIC header protection.
	HeaderUnprotect,
	/// Decrypt and authenticate the packet payload.
	PayloadDecrypt,
	/// Process one decoded QUIC frame.
	FrameProcess,
	/// Encode frames into a packet payload.
	FrameEncode,
	/// Encrypt the packet and apply header protection.
	PacketEncrypt,
}

impl PacketPhase {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::HeaderParse => "header_parse",
			Self::Routing => "routing",
			Self::Scheduling => "scheduling",
			Self::HeaderUnprotect => "header_unprotect",
			Self::PayloadDecrypt => "payload_decrypt",
			Self::FrameProcess => "frame_process",
			Self::FrameEncode => "frame_encode",
			Self::PacketEncrypt => "packet_encrypt",
		}
	}
}

/// Whether a packet phase record starts or completes work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseEdge {
	Start,
	Done,
}

/// Metadata known when a packet trace begins.
#[derive(Clone, Debug)]
pub struct PacketContext {
	connection_id: u64,
	direction: Direction,
	packet_number: Option<u64>,
	packet_space: Option<PacketSpace>,
	byte_len: Option<usize>,
	start_ns: Option<u64>,
}

impl PacketContext {
	/// Create packet metadata for one Quinn connection and direction.
	pub fn new(direction: Direction, connection_id: u64) -> Self {
		Self {
			connection_id,
			direction,
			packet_number: None,
			packet_space: None,
			byte_len: None,
			start_ns: None,
		}
	}

	/// Attach the QUIC packet number used for TX sampling.
	pub fn with_number(mut self, number: u64) -> Self {
		self.packet_number = Some(number);
		self
	}

	/// Attach the QUIC packet number space.
	pub fn with_space(mut self, space: PacketSpace) -> Self {
		self.packet_space = Some(space);
		self
	}

	/// Attach the encoded packet length in bytes.
	pub fn with_byte_len(mut self, byte_len: usize) -> Self {
		self.byte_len = Some(byte_len);
		self
	}

	/// Attach a packet start timestamp captured before the trace was created.
	pub fn with_start_ns(mut self, start_ns: u64) -> Self {
		self.start_ns = Some(start_ns);
		self
	}
}

/// STREAM frame byte range carried by one QUIC packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamFrame {
	stream_id: u64,
	offset_start: u64,
	offset_end: u64,
}

impl StreamFrame {
	/// Create a STREAM frame mapping with an exclusive end offset.
	pub fn new(stream_id: u64, offset_start: u64, offset_end: u64) -> Self {
		Self {
			stream_id,
			offset_start,
			offset_end,
		}
	}
}

/// Fields shared by every record for one QUIC packet.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketEvent {
	/// Monotonic timestamp in nanoseconds from the local process clock.
	pub timestamp_ns: u64,
	/// Process-unique identifier shared by this packet's records.
	pub trace_id: u64,
	/// Quinn stable connection ID.
	pub connection_id: u64,
	/// Whether this packet is being received or transmitted.
	pub direction: Direction,
	/// QUIC packet number when known.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub packet_number: Option<u64>,
	/// QUIC packet number space when known.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub packet_space: Option<PacketSpace>,
	/// Encoded packet length in bytes when known.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub byte_len: Option<usize>,
	/// Sampling rate active for this packet.
	pub sample_rate: u64,
}

/// One boundary of a measured packet phase.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketPhaseEvent {
	/// Monotonic timestamp in nanoseconds from the local process clock.
	pub timestamp_ns: u64,
	/// Packet lifecycle identifier from [`PacketEvent::trace_id`].
	pub trace_id: u64,
	/// Packet lifecycle phase being measured.
	pub phase: PacketPhase,
	/// Whether this boundary starts or completes the phase.
	pub edge: PhaseEdge,
	/// Completion result, present only when `edge` is `done`.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub outcome: Option<PacketOutcome>,
}

/// STREAM frame mapping emitted as a child of one packet trace.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamFrameEvent {
	/// Monotonic timestamp in nanoseconds from the local process clock.
	pub timestamp_ns: u64,
	/// Packet lifecycle identifier from [`PacketEvent::trace_id`].
	pub trace_id: u64,
	/// QUIC stream identifier.
	pub stream_id: u64,
	/// Inclusive stream byte offset.
	pub offset_start: u64,
	/// Exclusive stream byte offset.
	pub offset_end: u64,
	/// Result of processing this frame.
	pub outcome: PacketOutcome,
}

/// Packet completion record with an explicit result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacketEndEvent {
	/// Monotonic completion timestamp in nanoseconds.
	pub timestamp_ns: u64,
	/// Packet lifecycle identifier from [`PacketEvent::trace_id`].
	pub trace_id: u64,
	/// QUIC packet number discovered during processing.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub packet_number: Option<u64>,
	/// QUIC packet number space discovered during processing.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub packet_space: Option<PacketSpace>,
	/// Final encoded packet length in bytes.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub byte_len: Option<usize>,
	/// Result of processing the packet.
	pub outcome: PacketOutcome,
}

/// A sampled QUIC packet whose completion consumes the token.
#[must_use = "dropping a packet trace records an abandoned packet"]
pub struct PacketTrace(Option<PacketTraceState>);

struct PacketTraceState {
	handle: Handle,
	packet: PacketEvent,
}

/// A sampled packet phase whose completion consumes the token.
#[must_use = "dropping a packet phase records an abandoned phase"]
pub struct PacketPhaseTrace(Option<PacketPhaseTraceState>);

struct PacketPhaseTraceState {
	handle: Handle,
	trace_id: u64,
	start_ns: u64,
	phase: PacketPhase,
}

impl Handle {
	/// Start a sampled QUIC packet trace.
	pub fn packet(&self, context: PacketContext) -> PacketTrace {
		let Some(inner) = self.inner.as_ref() else {
			return PacketTrace::disabled();
		};
		let sample_rate = inner.config.packet_sample;
		let sampled = context
			.packet_number
			.map(|number| number % sample_rate == sample_rate - 1)
			.unwrap_or_else(|| {
				let seen = inner.packet_seen.fetch_add(1, Ordering::Relaxed);
				seen % sample_rate == sample_rate - 1
			});
		if !sampled {
			return PacketTrace::disabled();
		}

		let packet = PacketEvent {
			timestamp_ns: context.start_ns.unwrap_or_else(now_ns),
			trace_id: inner.next_trace_id.fetch_add(1, Ordering::Relaxed),
			connection_id: context.connection_id,
			direction: context.direction,
			packet_number: context.packet_number,
			packet_space: context.packet_space,
			byte_len: context.byte_len,
			sample_rate,
		};
		inner.emit(Event::PacketStart(packet.clone()));
		PacketTrace(Some(PacketTraceState {
			handle: self.clone(),
			packet,
		}))
	}
}

impl PacketTrace {
	/// Return a disabled packet trace token.
	pub fn disabled() -> Self {
		Self(None)
	}

	/// Record the packet number discovered during RX processing.
	pub fn set_number(&mut self, number: u64) {
		if let Some(state) = &mut self.0 {
			state.packet.packet_number = Some(number);
		}
	}

	/// Record the packet number space discovered during RX processing.
	pub fn set_space(&mut self, space: PacketSpace) {
		if let Some(state) = &mut self.0 {
			state.packet.packet_space = Some(space);
		}
	}

	/// Record the final encoded packet length.
	pub fn set_byte_len(&mut self, byte_len: usize) {
		if let Some(state) = &mut self.0 {
			state.packet.byte_len = Some(byte_len);
		}
	}

	/// Start a measured packet lifecycle phase.
	pub fn phase(&self, phase: PacketPhase) -> PacketPhaseTrace {
		self.phase_at(phase, now_ns())
	}

	/// Start a measured packet phase at a previously captured timestamp.
	pub fn phase_at(&self, phase: PacketPhase, timestamp_ns: u64) -> PacketPhaseTrace {
		let Some(state) = &self.0 else {
			return PacketPhaseTrace::disabled();
		};
		state.handle.emit(Event::PacketPhase(PacketPhaseEvent {
			timestamp_ns,
			trace_id: state.packet.trace_id,
			phase,
			edge: PhaseEdge::Start,
			outcome: None,
		}));
		PacketPhaseTrace(Some(PacketPhaseTraceState {
			handle: state.handle.clone(),
			trace_id: state.packet.trace_id,
			start_ns: timestamp_ns,
			phase,
		}))
	}

	/// Record a STREAM frame carried by this packet.
	pub fn stream_frame(&self, frame: StreamFrame, outcome: PacketOutcome) {
		let Some(state) = &self.0 else {
			return;
		};
		state.handle.emit(Event::StreamFrame(StreamFrameEvent {
			timestamp_ns: now_ns(),
			trace_id: state.packet.trace_id,
			stream_id: frame.stream_id,
			offset_start: frame.offset_start,
			offset_end: frame.offset_end,
			outcome,
		}));
	}

	/// Finish the packet with an explicit result.
	pub fn finish(mut self, outcome: PacketOutcome) {
		if let Some(state) = self.0.take() {
			state.emit_end(outcome);
		}
	}
}

impl PacketTraceState {
	fn emit_end(self, outcome: PacketOutcome) {
		self.handle.emit(Event::PacketEnd(PacketEndEvent {
			timestamp_ns: now_ns(),
			trace_id: self.packet.trace_id,
			packet_number: self.packet.packet_number,
			packet_space: self.packet.packet_space,
			byte_len: self.packet.byte_len,
			outcome,
		}));
	}
}

impl Drop for PacketTrace {
	fn drop(&mut self) {
		if let Some(state) = self.0.take() {
			state.emit_end(PacketOutcome::Abandoned);
		}
	}
}

impl PacketPhaseTrace {
	fn disabled() -> Self {
		Self(None)
	}

	/// Finish the phase with an explicit result.
	pub fn finish(self, outcome: PacketOutcome) {
		self.finish_at(outcome, now_ns());
	}

	/// Finish the phase at a previously captured timestamp.
	pub fn finish_at(mut self, outcome: PacketOutcome, timestamp_ns: u64) {
		if let Some(state) = self.0.take() {
			state.emit_done_at(outcome, timestamp_ns);
		}
	}
}

impl PacketPhaseTraceState {
	fn emit_done(self, outcome: PacketOutcome) {
		self.emit_done_at(outcome, now_ns());
	}

	fn emit_done_at(self, outcome: PacketOutcome, timestamp_ns: u64) {
		debug_assert!(timestamp_ns >= self.start_ns);
		self.handle.emit(Event::PacketPhase(PacketPhaseEvent {
			timestamp_ns,
			trace_id: self.trace_id,
			phase: self.phase,
			edge: PhaseEdge::Done,
			outcome: Some(outcome),
		}));
	}
}

impl Drop for PacketPhaseTrace {
	fn drop(&mut self) {
		if let Some(state) = self.0.take() {
			state.emit_done(PacketOutcome::Abandoned);
		}
	}
}
