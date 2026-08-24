//! Feature-gated tracing adapter for MoQ object instrumentation.

#[cfg(feature = "trace")]
pub(crate) use moq_trace::*;

#[cfg(not(feature = "trace"))]
#[allow(dead_code)]
mod disabled {
	/// Whether an object is entering or leaving the relay.
	#[derive(Clone, Copy)]
	pub enum Direction {
		/// Object is entering the relay.
		Rx,
		/// Object is leaving the relay.
		Tx,
	}

	/// A measured step in the object lifecycle.
	#[derive(Clone, Copy)]
	pub enum ObjectPhase {
		/// Parse an inbound object header.
		HeaderParse,
		/// Create an inbound object in the relay model.
		Create,
		/// Read an inbound object payload.
		PayloadRead,
		/// Commit an inbound frame to the relay model.
		FrameCommit,
		/// Clone an outbound object.
		Clone,
		/// Encode an outbound object header.
		HeaderEncode,
		/// Write an outbound object payload.
		PayloadWrite,
	}

	/// Result of an object lifecycle phase.
	#[derive(Clone, Copy)]
	pub enum ObjectOutcome {
		/// Processing completed successfully.
		Success,
		/// Processing completed with an error.
		Failed,
		/// The phase ended without an explicit outcome.
		Abandoned,
	}

	/// No-op object identity used when the `trace` feature is disabled.
	#[derive(Clone, Copy)]
	pub struct ObjectIdentity;

	impl ObjectIdentity {
		/// Create a no-op identity from object coordinates.
		pub fn new(_track_alias: u64, _group_id: u64, _object_id: u64) -> Self {
			Self
		}
	}

	/// No-op logical object identity used when tracing is disabled.
	#[derive(Clone, Copy)]
	pub struct LogicalId;

	impl LogicalId {
		/// Create a no-op identity from a group instance and frame ordinal.
		pub fn new(_group: u64, _frame: u64) -> Self {
			Self
		}
	}

	/// No-op metadata used when the `trace` feature is disabled.
	#[derive(Clone)]
	pub struct ObjectContext;

	impl ObjectContext {
		/// Create no-op metadata for one object.
		pub fn new(_direction: Direction, _identity: ObjectIdentity, _logical_id: LogicalId) -> Self {
			Self
		}

		/// Ignore an optional session identifier.
		pub fn with_session_id(self, _session_id: u64) -> Self {
			self
		}

		/// Ignore an optional transport connection identifier.
		pub fn with_connection_id(self, _connection_id: u64) -> Self {
			self
		}

		/// Ignore an optional stream identifier.
		pub fn with_stream_id(self, _stream_id: u64) -> Self {
			self
		}

		/// Ignore an optional starting stream byte offset.
		pub fn with_stream_offset_start(self, _offset_start: u64) -> Self {
			self
		}

		/// Ignore a payload size known before tracing starts.
		pub fn with_payload_bytes(self, _payload_bytes: u64) -> Self {
			self
		}
	}

	/// No-op object trace used when the `trace` feature is disabled.
	#[must_use = "object traces must be explicitly finished when processing completes"]
	pub struct ObjectTrace;

	/// No-op scoped object phase used when tracing is disabled.
	#[must_use = "object phases must be explicitly finished when processing completes"]
	pub struct ObjectPhaseTrace<'a> {
		_object: &'a mut ObjectTrace,
	}

	impl ObjectTrace {
		/// Return a disabled object trace token.
		pub fn disabled() -> Self {
			Self
		}

		/// Ignore a payload size update.
		pub fn set_payload_bytes(&mut self, _payload_bytes: u64) {}

		/// Ignore a stream offset update.
		pub fn set_stream_offset_end(&mut self, _stream_offset_end: u64) {}

		/// Return a no-op scoped object phase.
		pub fn phase(&mut self, _phase: ObjectPhase) -> ObjectPhaseTrace<'_> {
			ObjectPhaseTrace { _object: self }
		}

		/// Finish the no-op object interval.
		pub fn finish(self) {}
	}

	impl ObjectPhaseTrace<'_> {
		/// Ignore a payload size update.
		pub fn set_payload_bytes(&mut self, _payload_bytes: u64) {}

		/// Ignore a stream offset update.
		pub fn set_stream_offset_end(&mut self, _stream_offset_end: u64) {}

		/// Finish the no-op object phase.
		pub fn finish(self, _outcome: ObjectOutcome) {}
	}

	/// No-op trace handle used when the `trace` feature is disabled.
	#[derive(Clone, Default)]
	pub struct Handle;

	impl Handle {
		/// Return a disabled trace handle.
		pub fn disabled() -> Self {
			Self
		}

		/// Ignore a process-local session identifier.
		pub fn with_session_id(self, _session_id: u64) -> Self {
			self
		}

		/// Ignore a process-local transport connection identifier.
		pub fn with_connection_id(self, _connection_id: u64) -> Self {
			self
		}

		/// Ignore allocation of a process-local session identifier.
		pub fn with_new_session_id(self) -> Self {
			self
		}

		/// Return a no-op trace token for one object.
		pub fn object(&self, _context: ObjectContext) -> ObjectTrace {
			ObjectTrace
		}
	}

	pub fn global() -> Handle {
		Handle
	}
}

#[cfg(not(feature = "trace"))]
pub(crate) use disabled::*;
