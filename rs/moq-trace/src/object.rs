use std::hash::BuildHasher;
use std::sync::atomic::Ordering;

use serde::{Deserialize, Serialize};

use crate::{Direction, Event, Handle, PhaseEdge, now_ns};

/// MoQ protocol family represented by an object event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
	MoqTransport,
}

/// Metadata emitted once when a MoQ object lifecycle starts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectEvent {
	/// Monotonic timestamp in nanoseconds from the local process clock.
	pub timestamp_ns: u64,
	/// Process-unique lifecycle identifier used by child and completion records.
	pub trace_id: u64,
	/// Process-unique identity shared by ingress and every outbound copy.
	pub logical_id: LogicalId,
	/// Process-local MoQ session ID when available.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub session_id: Option<u64>,
	/// Process-local transport connection ID when available.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub connection_id: Option<u64>,
	/// Whether this object is entering or leaving the relay.
	pub direction: Direction,
	/// MoQ protocol family for this object.
	pub protocol: Protocol,
	/// moq-transport track alias or request ID on this session.
	pub track_alias: u64,
	/// moq-transport group ID.
	pub group_id: u64,
	/// moq-transport object ID.
	pub object_id: u64,
	/// QUIC stream ID when the backend exposes it.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stream_id: Option<u64>,
	/// Inclusive stream byte offset where this object starts, when known.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stream_offset_start: Option<u64>,
	/// Sampling rate active for this event.
	pub sample_rate: u64,
}

/// Identity shared by ingress and every outbound copy of one logical object.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Final metadata emitted when a MoQ object lifecycle completes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectEndEvent {
	/// Monotonic completion timestamp in nanoseconds.
	pub timestamp_ns: u64,
	/// Object lifecycle identifier from [`ObjectEvent::trace_id`].
	pub trace_id: u64,
	/// Exclusive stream byte offset where this object ends, when known.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stream_offset_end: Option<u64>,
	/// Object payload size in bytes.
	pub payload_bytes: u64,
}

/// A measured step in the moq-transport object lifecycle.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
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

/// Result of an object lifecycle phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
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
	track_alias: u64,
	group_id: u64,
	object_id: u64,
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
	logical_id: LogicalId,
	session_id: Option<u64>,
	connection_id: Option<u64>,
	direction: Direction,
	track_alias: u64,
	group_id: u64,
	object_id: u64,
	stream_id: Option<u64>,
	stream_offset_start: Option<u64>,
	payload_bytes: u64,
}

impl ObjectContext {
	pub fn new(direction: Direction, identity: ObjectIdentity, logical_id: LogicalId) -> Self {
		Self {
			logical_id,
			session_id: None,
			connection_id: None,
			direction,
			track_alias: identity.track_alias,
			group_id: identity.group_id,
			object_id: identity.object_id,
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

/// A sampled moq-transport object trace, or a zero-work disabled token.
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
		self.emit_phase(phase, PhaseEdge::Start, None);
		ObjectPhaseTrace {
			object: self,
			phase,
			finished: false,
		}
	}

	fn emit_phase(&self, phase: ObjectPhase, edge: PhaseEdge, outcome: Option<ObjectOutcome>) {
		let Some(state) = &self.0 else {
			return;
		};
		state.handle.emit(Event::MoqObjectPhase(ObjectPhaseEvent {
			timestamp_ns: now_ns(),
			trace_id: state.trace_id,
			phase,
			edge,
			outcome,
		}));
	}

	/// Finish the object interval with the latest metadata.
	pub fn finish(mut self) {
		let Some(state) = self.0.take() else {
			return;
		};
		state.handle.emit(Event::MoqObjectEnd(ObjectEndEvent {
			timestamp_ns: now_ns(),
			trace_id: state.trace_id,
			stream_offset_end: state.stream_offset_end,
			payload_bytes: state.payload_bytes,
		}));
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
		self.object.emit_phase(self.phase, PhaseEdge::Done, Some(outcome));
		self.finished = true;
	}
}

impl Drop for ObjectPhaseTrace<'_> {
	fn drop(&mut self) {
		if !self.finished {
			self.object
				.emit_phase(self.phase, PhaseEdge::Done, Some(ObjectOutcome::Abandoned));
		}
	}
}

/// moq-transport object phase fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectPhaseEvent {
	/// Monotonic boundary timestamp in nanoseconds.
	pub timestamp_ns: u64,
	/// Parent object lifecycle identifier.
	pub trace_id: u64,
	/// Object lifecycle phase being measured.
	pub phase: ObjectPhase,
	/// Whether this boundary starts or completes the phase.
	pub edge: PhaseEdge,
	/// Completion result, present only when `edge` is `done`.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub outcome: Option<ObjectOutcome>,
}

impl Handle {
	fn object_sample_rate(&self, logical_id: LogicalId) -> Option<u64> {
		let inner = self.inner.as_ref()?;
		let sample = inner.config.object_sample;
		if inner.object_hasher.hash_one(logical_id) % sample == sample - 1 {
			Some(sample)
		} else {
			None
		}
	}

	/// Start a moq-transport object trace after applying object sampling.
	pub fn object(&self, context: ObjectContext) -> ObjectTrace {
		if !crate::backend::object_enabled() {
			return ObjectTrace::disabled();
		}
		let logical_id = context.logical_id;
		let Some(sample_rate) = self.object_sample_rate(logical_id) else {
			return ObjectTrace::disabled();
		};
		let trace_id = crate::NEXT_TRACE_ID.fetch_add(1, Ordering::Relaxed);
		let object = ObjectEvent {
			timestamp_ns: now_ns(),
			trace_id,
			logical_id,
			session_id: context.session_id.or(self.session_id),
			connection_id: context.connection_id.or(self.connection_id),
			direction: context.direction,
			protocol: Protocol::MoqTransport,
			track_alias: context.track_alias,
			group_id: context.group_id,
			object_id: context.object_id,
			stream_id: context.stream_id,
			stream_offset_start: context.stream_offset_start,
			sample_rate,
		};
		self.emit(Event::MoqObjectStart(object));
		ObjectTrace(Some(ObjectTraceState {
			handle: self.clone(),
			trace_id,
			payload_bytes: context.payload_bytes,
			stream_offset_end: None,
		}))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::Config;

	#[test]
	fn logical_identity_samples_ingress_and_copies_together() {
		let handle = Handle::new(Config {
			object_sample: 2,
			..Config::default()
		});
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
		let starts = handle
			.events()
			.into_iter()
			.filter_map(|event| match event {
				Event::MoqObjectStart(event) => Some(event),
				_ => None,
			})
			.collect::<Vec<_>>();
		assert_eq!(starts.len(), 3);
		assert!(starts.iter().all(|event| event.logical_id == sampled));
	}
}
