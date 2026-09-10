use crate::{Direction, Handle, now_ns};

/// Result of one UDP socket operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SocketOutcome {
	/// The socket operation completed successfully.
	Success,
	/// The socket was not ready and registered a wakeup.
	Pending,
	/// The socket reported that the operation would block.
	WouldBlock,
	/// The socket reported a connection reset.
	ConnectionReset,
	/// The socket operation failed with another error.
	Error,
	/// The trace token was dropped before an outcome was recorded.
	Abandoned,
}

/// Batch measurements returned by one UDP socket operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SocketStats {
	/// Number of buffers processed by the operation.
	pub buffers: usize,
	/// Number of UDP datagrams represented by those buffers.
	pub datagrams: usize,
	/// Total number of bytes represented by those buffers.
	pub bytes: usize,
}

impl SocketStats {
	/// Create socket batch measurements.
	pub fn new(buffers: usize, datagrams: usize, bytes: usize) -> Self {
		Self {
			buffers,
			datagrams,
			bytes,
		}
	}
}

/// A UDP socket operation whose completion consumes the token.
pub struct SocketTrace {
	handle: Handle,
	trace_id: u64,
	finished: bool,
}

impl Handle {
	/// Start a UDP socket operation.
	pub fn socket(&self, direction: Direction, connection_id: Option<u64>) -> Option<SocketTrace> {
		let inner = self.inner.as_ref()?;
		if !inner.socket_enabled() {
			return None;
		}
		let trace_id = crate::NEXT_TRACE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		inner.socket_start(now_ns(), trace_id, direction, connection_id);
		Some(SocketTrace {
			handle: self.clone(),
			trace_id,
			finished: false,
		})
	}
}

impl SocketTrace {
	/// Finish the socket operation with its result and batch measurements.
	pub fn finish(mut self, outcome: SocketOutcome, stats: SocketStats) {
		self.emit_end(outcome, stats);
		self.finished = true;
	}

	fn emit_end(&self, outcome: SocketOutcome, stats: SocketStats) {
		if let Some(backend) = &self.handle.inner {
			backend.socket_end(now_ns(), self.trace_id, outcome, stats);
		}
	}
}

impl Drop for SocketTrace {
	fn drop(&mut self) {
		if !self.finished {
			self.emit_end(SocketOutcome::Abandoned, SocketStats::default());
		}
	}
}
