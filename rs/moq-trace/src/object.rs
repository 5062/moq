use std::sync::atomic::Ordering;

use crate::{Direction, Handle, PhaseEdge, now_ns};

/// Identity shared by ingress and every outbound copy of one logical object.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LogicalId {
	group: u64,
	frame: u64,
}

impl LogicalId {
	/// Create an identity from a process-unique group instance and frame ordinal.
	pub fn new(group: u64, frame: u64) -> Self {
		Self { group, frame }
	}

	/// Return the process-unique group instance.
	pub fn group(self) -> u64 {
		self.group
	}

	/// Return the zero-based frame ordinal within the group.
	pub fn frame(self) -> u64 {
		self.frame
	}
}

impl std::fmt::Display for LogicalId {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(formatter, "{}:{}", self.group, self.frame)
	}
}

/// A measured step in the moq-transport object lifecycle.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum ObjectPhase {
	/// Parse an inbound object header.
	HeaderParse,
	/// Create an inbound object in the relay model.
	Create,
	/// Read an inbound object payload.
	PayloadRead,
	/// Commit an inbound frame to the relay model.
	FrameCommit,
	/// Clone or select an outbound object from the relay model.
	Clone,
	/// Encode an outbound object header.
	HeaderEncode,
	/// Write an outbound object payload.
	PayloadWrite,
}

impl ObjectPhase {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::HeaderParse => "header_parse",
			Self::Create => "create",
			Self::PayloadRead => "payload_read",
			Self::FrameCommit => "frame_commit",
			Self::Clone => "clone",
			Self::HeaderEncode => "header_encode",
			Self::PayloadWrite => "payload_write",
		}
	}
}

/// Result of an object lifecycle or phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ObjectOutcome {
	/// Processing completed successfully.
	Success,
	/// Processing completed with an error.
	Failed,
	/// The phase token was dropped before an outcome was recorded.
	Abandoned,
}

/// Stable identity of one moq-transport object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectIdentity {
	pub(crate) track_alias: u64,
	pub(crate) group_id: u64,
	pub(crate) object_id: u64,
}

impl ObjectIdentity {
	pub fn new(track_alias: u64, group_id: u64, object_id: u64) -> Self {
		Self {
			track_alias,
			group_id,
			object_id,
		}
	}
}

/// Stable metadata known before a moq-transport object trace starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectContext {
	pub(crate) logical_id: LogicalId,
	pub(crate) identity: ObjectIdentity,
	pub(crate) session_id: Option<u64>,
	pub(crate) connection_id: Option<u64>,
	pub(crate) direction: Direction,
	pub(crate) stream_id: Option<u64>,
	pub(crate) stream_offset_start: Option<u64>,
	payload_bytes: u64,
}

impl ObjectContext {
	pub fn new(direction: Direction, identity: ObjectIdentity, logical_id: LogicalId) -> Self {
		Self {
			logical_id,
			identity,
			session_id: None,
			connection_id: None,
			direction,
			stream_id: None,
			stream_offset_start: None,
			payload_bytes: 0,
		}
	}

	/// Attach a process-local trace session identifier.
	pub fn with_session_id(mut self, session_id: u64) -> Self {
		self.session_id = Some(session_id);
		self
	}

	/// Attach a process-local transport connection identifier.
	pub fn with_connection_id(mut self, connection_id: u64) -> Self {
		self.connection_id = Some(connection_id);
		self
	}

	/// Attach the transport stream identifier.
	pub fn with_stream_id(mut self, stream_id: u64) -> Self {
		self.stream_id = Some(stream_id);
		self
	}

	/// Attach the inclusive stream byte offset where this object starts.
	pub fn with_stream_offset_start(mut self, offset_start: u64) -> Self {
		self.stream_offset_start = Some(offset_start);
		self
	}

	/// Attach a payload size known before tracing starts.
	pub fn with_payload_bytes(mut self, payload_bytes: u64) -> Self {
		self.payload_bytes = payload_bytes;
		self
	}
}

/// A moq-transport object trace, or a zero-work disabled token.
#[must_use = "object traces must be explicitly finished when processing completes"]
pub struct ObjectTrace(Option<ObjectTraceState>);

struct ObjectTraceState {
	handle: Handle,
	trace_id: u64,
	payload_bytes: u64,
	stream_offset_end: Option<u64>,
}

/// A scoped object phase whose completion consumes the token.
#[must_use = "dropping an object phase records an abandoned phase"]
pub struct ObjectPhaseTrace<'a> {
	object: &'a mut ObjectTrace,
	span_id: u64,
	phase: ObjectPhase,
	finished: bool,
}

impl ObjectTrace {
	/// Return a disabled object trace token.
	pub fn disabled() -> Self {
		Self(None)
	}

	/// Update the object payload size once it is known.
	pub fn set_payload_bytes(&mut self, payload_bytes: u64) {
		if let Some(state) = &mut self.0 {
			state.payload_bytes = payload_bytes;
		}
	}

	/// Update the exclusive stream byte offset reached by this object.
	pub fn set_stream_offset_end(&mut self, stream_offset_end: u64) {
		if let Some(state) = &mut self.0 {
			state.stream_offset_end = Some(stream_offset_end);
		}
	}

	/// Start a measured object lifecycle phase.
	pub fn phase(&mut self, phase: ObjectPhase) -> ObjectPhaseTrace<'_> {
		let span_id = self
			.0
			.as_ref()
			.map(|_| crate::NEXT_SPAN_ID.fetch_add(1, Ordering::Relaxed))
			.unwrap_or_default();
		self.emit_phase(span_id, phase, PhaseEdge::Start, None);
		ObjectPhaseTrace {
			object: self,
			span_id,
			phase,
			finished: false,
		}
	}

	fn emit_phase(&self, span_id: u64, phase: ObjectPhase, edge: PhaseEdge, outcome: Option<ObjectOutcome>) {
		let Some(state) = &self.0 else {
			return;
		};
		if let Some(backend) = &state.handle.inner {
			backend.object_phase(now_ns(), state.trace_id, span_id, phase, edge, outcome);
		}
	}

	/// Finish the object interval with the latest metadata and result.
	pub fn finish(mut self, outcome: ObjectOutcome) {
		let Some(state) = self.0.take() else {
			return;
		};
		state.emit_end(outcome);
	}
}

impl ObjectTraceState {
	fn emit_end(self, outcome: ObjectOutcome) {
		if let Some(backend) = &self.handle.inner {
			backend.object_end(
				now_ns(),
				self.trace_id,
				self.stream_offset_end,
				self.payload_bytes,
				outcome,
			);
		}
	}
}

impl Drop for ObjectTrace {
	fn drop(&mut self) {
		if let Some(state) = self.0.take() {
			state.emit_end(ObjectOutcome::Abandoned);
		}
	}
}

impl ObjectPhaseTrace<'_> {
	/// Update the object payload size once it is known.
	pub fn set_payload_bytes(&mut self, payload_bytes: u64) {
		self.object.set_payload_bytes(payload_bytes);
	}

	/// Update the exclusive stream byte offset reached by this object.
	pub fn set_stream_offset_end(&mut self, stream_offset_end: u64) {
		self.object.set_stream_offset_end(stream_offset_end);
	}

	/// Finish the phase with an explicit result.
	pub fn finish(mut self, outcome: ObjectOutcome) {
		self.object
			.emit_phase(self.span_id, self.phase, PhaseEdge::Done, Some(outcome));
		self.finished = true;
	}
}

impl Drop for ObjectPhaseTrace<'_> {
	fn drop(&mut self) {
		if !self.finished {
			self.object.emit_phase(
				self.span_id,
				self.phase,
				PhaseEdge::Done,
				Some(ObjectOutcome::Abandoned),
			);
		}
	}
}

impl Handle {
	/// Start a moq-transport object trace.
	pub fn object(&self, context: ObjectContext) -> ObjectTrace {
		let Some(inner) = self.inner.as_ref() else {
			return ObjectTrace::disabled();
		};
		if !inner.object_enabled() {
			return ObjectTrace::disabled();
		}
		let mut context = context;
		context.session_id = context.session_id.or(self.session_id);
		context.connection_id = context.connection_id.or(self.connection_id);
		let trace_id = crate::NEXT_TRACE_ID.fetch_add(1, Ordering::Relaxed);
		inner.object_start(now_ns(), trace_id, &context);
		ObjectTrace(Some(ObjectTraceState {
			handle: self.clone(),
			trace_id,
			payload_bytes: context.payload_bytes,
			stream_offset_end: None,
		}))
	}
}
