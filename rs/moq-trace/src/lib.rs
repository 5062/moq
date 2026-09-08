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

/// Tracing configuration shared by MoQ and QUIC instrumentation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
	/// Emit all events for every n-th MoQ object ID. Values below 1 are treated as 1.
	pub object_sample: u64,
	/// Emit all events for every n-th QUIC packet number. Values below 1 are treated as 1.
	pub packet_sample: u64,
	/// Emit every n-th UDP socket operation. Values below 1 are treated as 1.
	pub socket_sample: u64,
}

impl Config {
	fn normalized(mut self) -> Self {
		self.object_sample = self.object_sample.max(1);
		self.packet_sample = self.packet_sample.max(1);
		self.socket_sample = self.socket_sample.max(1);
		self
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			object_sample: 1,
			packet_sample: 1,
			socket_sample: 1,
		}
	}
}

/// Errors returned when installing process-global tracing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
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

/// One normalized JSON trace record used by offline analysis.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
	/// Trace format and clock metadata. This must be the first record.
	TraceHeader(TraceHeaderEvent),
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

/// Current trace format revision. Readers reject every other revision.
pub const TRACE_REVISION: u32 = backend::TRACE_REVISION;

/// Metadata that begins every trace file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraceHeaderEvent {
	/// Exact trace schema revision.
	pub revision: u32,
	/// Timestamp clock and unit used by every event.
	pub clock: TraceClock,
}

/// Timestamp clock used by a trace file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceClock {
	/// Process-local monotonic nanoseconds.
	MonotonicNs,
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
			_ => None,
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

struct Inner {
	config: Config,
	packet_seen: AtomicU64,
	socket_seen: AtomicU64,
	next_trace_id: AtomicU64,
	object_hasher: std::collections::hash_map::RandomState,
	#[cfg(test)]
	events: std::sync::Mutex<Vec<Event>>,
}

impl Handle {
	/// Create an enabled handle with the supplied sampling configuration.
	///
	/// The process must be traced by an active LTTng-UST session for events to
	/// be recorded. Creating the handle itself does not start or stop a session.
	pub fn new(config: Config) -> Self {
		let config = config.normalized();
		let handle = Self {
			inner: Some(Arc::new(Inner::new(config))),
			session_id: None,
			connection_id: None,
		};
		backend::initialize();
		handle
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
	fn new(config: Config) -> Self {
		Self {
			config,
			packet_seen: AtomicU64::new(0),
			socket_seen: AtomicU64::new(0),
			next_trace_id: AtomicU64::new(1),
			object_hasher: std::collections::hash_map::RandomState::new(),
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
pub fn install(config: Config) -> Result<(), Error> {
	backend::initialize();
	GLOBAL
		.set(Arc::new(Inner::new(config.normalized())))
		.map_err(|_| Error::GlobalAlreadyInstalled)
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

/// Return a monotonic timestamp in nanoseconds.
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
