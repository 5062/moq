//! Relay tracing for MoQ objects and QUIC packets.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use serde::{Deserialize, Serialize};

#[cfg_attr(test, allow(dead_code))]
mod backend;

mod object;
pub use object::{
	LogicalId, ObjectContext, ObjectEndEvent, ObjectEvent, ObjectIdentity, ObjectOutcome, ObjectPhase,
	ObjectPhaseEvent, ObjectPhaseTrace, ObjectTrace, Protocol,
};

mod packet;
pub use packet::{
	PacketContext, PacketEndEvent, PacketEvent, PacketOutcome, PacketPhase, PacketPhaseEvent, PacketPhaseTrace,
	PacketTrace, PhaseEdge, StreamFrame, StreamFrameEvent,
};

mod socket;
pub use socket::{SocketEndEvent, SocketEvent, SocketOutcome, SocketStats, SocketTrace};

/// Errors returned when installing process-global tracing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
	/// LTTng-UST tracing is only available on Linux.
	#[error("LTTng-UST tracing is only available on Linux")]
	UnsupportedPlatform,
	/// Process-global tracing was already installed.
	#[error("process-global tracing already installed")]
	GlobalAlreadyInstalled,
}

/// Trace event direction at the relay boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// One typed trace event used by instrumentation and offline analysis.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
	/// First byte of a moq-transport object was observed.
	#[serde(rename = "moq_object_start")]
	MoqObjectStart(ObjectEvent),
	/// Final byte of a moq-transport object was observed.
	#[serde(rename = "moq_object_end")]
	MoqObjectEnd(ObjectEndEvent),
	/// moq-transport object processing phase boundary.
	#[serde(rename = "moq_object_phase")]
	MoqObjectPhase(ObjectPhaseEvent),
	/// QUIC packet processing started.
	#[serde(rename = "quic_packet_start")]
	PacketStart(PacketEvent),
	/// QUIC packet processing completed.
	#[serde(rename = "quic_packet_end")]
	PacketEnd(PacketEndEvent),
	/// QUIC packet processing phase boundary.
	#[serde(rename = "quic_packet_phase")]
	PacketPhase(PacketPhaseEvent),
	/// QUIC STREAM frame mapped to its parent packet.
	#[serde(rename = "quic_stream_frame")]
	StreamFrame(StreamFrameEvent),
	/// UDP socket operation started.
	#[serde(rename = "udp_socket_start")]
	SocketStart(SocketEvent),
	/// UDP socket operation completed.
	#[serde(rename = "udp_socket_end")]
	SocketEnd(SocketEndEvent),
}

impl Event {
	/// Process-unique trace identifier for scoped socket and packet records.
	pub fn trace_id(&self) -> Option<u64> {
		match self {
			Self::MoqObjectStart(event) => Some(event.trace_id),
			Self::MoqObjectEnd(event) => Some(event.trace_id),
			Self::MoqObjectPhase(event) => Some(event.trace_id),
			Self::SocketStart(event) => Some(event.trace_id),
			Self::PacketStart(event) => Some(event.trace_id),
			Self::PacketEnd(event) => Some(event.trace_id),
			Self::PacketPhase(event) => Some(event.trace_id),
			Self::StreamFrame(event) => Some(event.trace_id),
			Self::SocketEnd(event) => Some(event.trace_id),
		}
	}
}

/// A cheap cloneable handle used by instrumentation sites to emit trace events.
#[derive(Clone, Default)]
pub struct Handle {
	inner: Option<Arc<Inner>>,
	session_id: Option<u64>,
	connection_id: Option<u64>,
}

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_TRACE_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

struct Inner {
	#[cfg(test)]
	events: std::sync::Mutex<Vec<Event>>,
}

impl Handle {
	#[cfg(test)]
	fn new() -> Self {
		Self {
			inner: Some(Arc::new(Inner::new())),
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

	pub(crate) fn emit(&self, event: Event) -> bool {
		let Some(inner) = &self.inner else {
			return false;
		};
		inner.emit(event)
	}

	#[cfg(test)]
	fn events(&self) -> Vec<Event> {
		self.inner
			.as_ref()
			.map(|inner| inner.events.lock().unwrap().clone())
			.unwrap_or_default()
	}
}

impl Inner {
	fn new() -> Self {
		Self {
			#[cfg(test)]
			events: std::sync::Mutex::new(Vec::new()),
		}
	}

	fn emit(&self, event: Event) -> bool {
		#[cfg(test)]
		let emitted = {
			self.events.lock().unwrap().push(event);
			true
		};
		#[cfg(not(test))]
		let emitted = backend::emit(&event);
		emitted
	}
}

static GLOBAL: OnceLock<Arc<Inner>> = OnceLock::new();

/// Install process-global tracing for MoQ, QUIC, and socket instrumentation.
#[cfg(target_os = "linux")]
pub fn install() -> Result<(), Error> {
	backend::initialize();
	GLOBAL
		.set(Arc::new(Inner::new()))
		.map_err(|_| Error::GlobalAlreadyInstalled)
}

/// Install process-global tracing for MoQ, QUIC, and socket instrumentation.
#[cfg(not(target_os = "linux"))]
pub fn install() -> Result<(), Error> {
	Err(Error::UnsupportedPlatform)
}

/// Return the process-global trace handle used by MoQ, QUIC, and socket hooks.
pub fn global() -> Handle {
	let inner = GLOBAL.get().cloned();
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
