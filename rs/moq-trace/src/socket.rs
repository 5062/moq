use serde::{Deserialize, Serialize};

use crate::{Direction, Event, Handle, now_ns};

/// Result of one UDP socket operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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

/// Fields recorded when a UDP socket operation starts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SocketEvent {
	/// Monotonic timestamp in nanoseconds from the local process clock.
	pub at_ns: u64,
	/// Process-unique identifier shared by this operation's records.
	pub trace_id: u64,
	/// Quinn stable connection ID when the operation belongs to one connection.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub connection_id: Option<u64>,
	/// Whether the operation receives or transmits datagrams.
	pub direction: Direction,
	/// Sampling rate active for this operation.
	pub sample_rate: u64,
}

/// Fields recorded when a UDP socket operation ends.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SocketEndEvent {
	/// Identity and timing fields shared with the start record.
	#[serde(flatten)]
	pub socket: SocketEvent,
	/// Result of the socket operation.
	pub outcome: SocketOutcome,
	/// Batch measurements observed by the operation.
	#[serde(flatten)]
	pub stats: SocketStats,
}

/// A sampled UDP socket operation whose completion consumes the token.
pub struct SocketTrace {
	handle: Handle,
	event: SocketEvent,
	finished: bool,
}

impl Handle {
	/// Start a sampled UDP socket operation.
	pub fn socket(&self, direction: Direction, connection_id: Option<u64>) -> Option<SocketTrace> {
		let inner = self.inner.as_ref()?;
		let sample_rate = inner.config.socket_sample;
		let seen = inner.socket_seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		if seen % sample_rate != sample_rate - 1 {
			return None;
		}

		let event = SocketEvent {
			at_ns: now_ns(),
			trace_id: inner.next_trace_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
			connection_id,
			direction,
			sample_rate,
		};
		inner.emit(Event::SocketStart(event.clone()));
		Some(SocketTrace {
			handle: self.clone(),
			event,
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
		let mut socket = self.event.clone();
		socket.at_ns = now_ns();
		self.handle
			.emit(Event::SocketEnd(SocketEndEvent { socket, outcome, stats }));
	}
}

impl Drop for SocketTrace {
	fn drop(&mut self) {
		if !self.finished {
			self.emit_end(SocketOutcome::Abandoned, SocketStats::default());
		}
	}
}
