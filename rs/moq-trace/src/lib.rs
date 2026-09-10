//! Relay tracing for MoQ objects and QUIC packets.

#[cfg(all(feature = "lttng", not(target_os = "linux")))]
compile_error!("the lttng feature is supported only on Linux");

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

mod backend;

mod object;
pub use object::{LogicalId, ObjectContext, ObjectIdentity, ObjectOutcome, ObjectPhase, ObjectPhaseTrace, ObjectTrace};

mod packet;
use packet::PhaseEdge;
pub use packet::{PacketContext, PacketOutcome, PacketPhase, PacketPhaseTrace, PacketTrace, StreamFrame};

mod socket;
pub use socket::{SocketOutcome, SocketStats, SocketTrace};

/// Trace event direction at the relay boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Direction {
	/// Event was observed while receiving from the peer.
	Rx,
	/// Event was observed while transmitting to the peer.
	Tx,
}

impl Direction {
	/// Return the stable lowercase representation used by normalized traces.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Rx => "rx",
			Self::Tx => "tx",
		}
	}
}

impl std::fmt::Display for Direction {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str(self.as_str())
	}
}

/// QUIC packet number space.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PacketSpace {
	/// QUIC Initial packet space.
	Initial,
	/// QUIC Handshake packet space.
	Handshake,
	/// QUIC 0-RTT packet space.
	ZeroRtt,
	/// QUIC 1-RTT application data packet space.
	Data,
}

/// A cheap cloneable handle used by instrumentation sites to emit trace events.
#[derive(Clone, Default)]
pub struct Handle {
	inner: Option<Arc<backend::Backend>>,
	session_id: Option<u64>,
	connection_id: Option<u64>,
}

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_TRACE_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

impl Handle {
	#[cfg(test)]
	fn new() -> Self {
		Self {
			inner: Some(Arc::new(backend::Backend::new())),
			session_id: None,
			connection_id: None,
		}
	}

	/// Create a handle that never emits events.
	pub fn disabled() -> Self {
		Self::default()
	}

	/// Return a clone that stamps object events with this process-local session ID.
	pub fn with_session_id(mut self, session_id: u64) -> Self {
		self.session_id = Some(session_id);
		self
	}

	/// Return a clone that stamps object events with this transport connection ID.
	pub fn with_connection_id(mut self, connection_id: u64) -> Self {
		self.connection_id = Some(connection_id);
		self
	}

	/// Return a clone that stamps object events with the next process-local session ID.
	pub fn with_new_session_id(mut self) -> Self {
		if self.inner.is_some() {
			self.session_id = Some(NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed));
		}
		self
	}

	#[cfg(test)]
	fn events(&self) -> Vec<backend::Event> {
		self.inner.as_ref().map(|inner| inner.events()).unwrap_or_default()
	}
}

static GLOBAL: OnceLock<Arc<backend::Backend>> = OnceLock::new();

/// Return the process-global trace handle used by MoQ, QUIC, and socket hooks.
pub fn global() -> Handle {
	if !backend::available() {
		return Handle::disabled();
	}
	let inner = Some(GLOBAL.get_or_init(|| Arc::new(backend::Backend::new())).clone());
	Handle {
		inner,
		session_id: None,
		connection_id: None,
	}
}

/// Return a process-relative monotonic timestamp in nanoseconds for boundaries recorded after they occur.
pub fn now_ns() -> u64 {
	static START: OnceLock<std::time::Instant> = OnceLock::new();
	START
		.get_or_init(std::time::Instant::now)
		.elapsed()
		.as_nanos()
		.try_into()
		.unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
