//! Raw JSONL tracing for MoQ relay object and QUIC packet latency.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::JoinHandle;

use serde::{Deserialize, Serialize};

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
	/// File path for newline-delimited JSON events. `None` keeps tracing in no-op mode.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
	/// Emit all events for every Nth MoQ object ID. Values below 1 are treated as 1.
	pub object_sample: u64,
	/// Emit all events for every Nth QUIC packet number. Values below 1 are treated as 1.
	pub packet_sample: u64,
	/// Emit every Nth UDP socket operation. Values below 1 are treated as 1.
	pub socket_sample: u64,
	/// Maximum queued events before new events are dropped.
	pub queue_capacity: usize,
}

impl Config {
	/// Return a disabled config with default sampling and queue sizing.
	pub fn disabled() -> Self {
		Self::default()
	}

	/// True when events should be written to disk.
	pub fn is_enabled(&self) -> bool {
		self.path.is_some()
	}

	fn normalized(mut self) -> Self {
		self.object_sample = self.object_sample.max(1);
		self.packet_sample = self.packet_sample.max(1);
		self.socket_sample = self.socket_sample.max(1);
		self.queue_capacity = self.queue_capacity.max(1);
		self
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			path: None,
			object_sample: 1,
			packet_sample: 1,
			socket_sample: 1,
			queue_capacity: 4096,
		}
	}
}

/// Errors returned when creating a trace writer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
	/// The trace output file could not be created.
	#[error("failed to create trace output")]
	Create(#[source] std::io::Error),
	/// The trace writer thread could not be spawned.
	#[error("failed to spawn trace writer")]
	Spawn(#[source] std::io::Error),
	/// A process-global trace destination was already installed.
	#[error("process-global trace destination already installed")]
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
	/// Return the serialized direction name.
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

/// One JSONL trace record.
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
pub const TRACE_REVISION: u32 = 1;

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

enum WriterCommand {
	Event(Event),
	Flush(std::sync::mpsc::SyncSender<bool>),
}

struct Inner {
	config: Config,
	sender: Option<std::sync::mpsc::SyncSender<WriterCommand>>,
	writer: Option<JoinHandle<()>>,
	packet_seen: AtomicU64,
	socket_seen: AtomicU64,
	next_trace_id: AtomicU64,
	object_hasher: std::collections::hash_map::RandomState,
	emitted: AtomicU64,
	dropped: AtomicU64,
	writer_failed: Arc<AtomicBool>,
}

impl Handle {
	/// Create a handle from config, spawning a writer thread when `path` is set.
	pub fn new(config: Config) -> Result<Self, Error> {
		let config = config.normalized();
		let Some(path) = config.path.clone() else {
			return Ok(Self::disabled());
		};

		let file = File::create(path).map_err(Error::Create)?;
		let (sender, receiver) = std::sync::mpsc::sync_channel(config.queue_capacity);
		let writer_failed = Arc::new(AtomicBool::new(false));
		let writer_status = writer_failed.clone();
		let writer = std::thread::Builder::new()
			.name("moq-trace-writer".into())
			.spawn(move || write_events(file, receiver, writer_status))
			.map_err(Error::Spawn)?;
		Ok(Self {
			inner: Some(Arc::new(Inner::new(config, Some(sender), Some(writer), writer_failed))),
			session_id: None,
			connection_id: None,
		})
	}

	/// Return a disabled handle.
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

	/// Emit an event without applying object or packet sampling.
	pub fn emit(&self, event: Event) -> bool {
		let Some(inner) = &self.inner else {
			return false;
		};
		inner.emit(event)
	}

	/// Flush all events accepted before this call to the output file.
	pub fn flush(&self) -> bool {
		let Some(inner) = &self.inner else {
			return true;
		};
		let Some(sender) = &inner.sender else {
			return false;
		};
		let (complete, receiver) = std::sync::mpsc::sync_channel(0);
		if sender.send(WriterCommand::Flush(complete)).is_err() {
			return false;
		}
		receiver.recv().unwrap_or(false)
	}

	/// Number of events accepted by this handle.
	pub fn emitted(&self) -> u64 {
		self.inner
			.as_ref()
			.map(|inner| inner.emitted.load(Ordering::Relaxed))
			.unwrap_or_default()
	}

	/// Number of events dropped because the queue was full or closed.
	pub fn dropped(&self) -> u64 {
		self.inner
			.as_ref()
			.map(|inner| inner.dropped.load(Ordering::Relaxed))
			.unwrap_or_default()
	}

	/// Whether the background writer encountered a terminal error.
	pub fn writer_failed(&self) -> bool {
		self.inner
			.as_ref()
			.map(|inner| inner.writer_failed.load(Ordering::Relaxed))
			.unwrap_or_default()
	}
}

impl Inner {
	fn new(
		config: Config,
		sender: Option<std::sync::mpsc::SyncSender<WriterCommand>>,
		writer: Option<JoinHandle<()>>,
		writer_failed: Arc<AtomicBool>,
	) -> Self {
		Self {
			config,
			sender,
			writer,
			packet_seen: AtomicU64::new(0),
			socket_seen: AtomicU64::new(0),
			next_trace_id: AtomicU64::new(1),
			object_hasher: std::collections::hash_map::RandomState::new(),
			emitted: AtomicU64::new(0),
			dropped: AtomicU64::new(0),
			writer_failed,
		}
	}

	fn emit(&self, event: Event) -> bool {
		let Some(sender) = self.sender.as_ref() else {
			return false;
		};

		match sender.try_send(WriterCommand::Event(event)) {
			Ok(()) => {
				self.emitted.fetch_add(1, Ordering::Relaxed);
				true
			}
			Err(std::sync::mpsc::TrySendError::Full(_)) => {
				self.dropped.fetch_add(1, Ordering::Relaxed);
				false
			}
			Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
				self.dropped.fetch_add(1, Ordering::Relaxed);
				self.writer_failed.store(true, Ordering::Relaxed);
				false
			}
		}
	}
}

impl Drop for Inner {
	fn drop(&mut self) {
		self.sender.take();
		if let Some(writer) = self.writer.take() {
			let _ = writer.join();
		}
	}
}

fn write_events<W: Write>(writer: W, receiver: std::sync::mpsc::Receiver<WriterCommand>, failed: Arc<AtomicBool>) {
	let mut writer = BufWriter::new(writer);
	let header = Event::TraceHeader(TraceHeaderEvent {
		revision: TRACE_REVISION,
		clock: TraceClock::MonotonicNs,
	});
	if serde_json::to_writer(&mut writer, &header)
		.map_err(std::io::Error::other)
		.and_then(|()| writer.write_all(b"\n"))
		.is_err()
	{
		failed.store(true, Ordering::Relaxed);
		return;
	}
	for command in receiver {
		let result = match command {
			WriterCommand::Event(event) => serde_json::to_writer(&mut writer, &event)
				.map_err(std::io::Error::other)
				.and_then(|()| writer.write_all(b"\n")),
			WriterCommand::Flush(complete) => {
				let result = writer.flush();
				let _ = complete.send(result.is_ok());
				result
			}
		};
		if result.is_err() {
			failed.store(true, Ordering::Relaxed);
			break;
		}
	}
	if writer.flush().is_err() {
		failed.store(true, Ordering::Relaxed);
	}
}

static GLOBAL: OnceLock<Weak<Inner>> = OnceLock::new();

/// Install the process-global trace destination used by all instrumentation layers.
///
/// The registry does not extend the handle lifetime, so the caller must retain the handle.
pub fn install_global(handle: &Handle) -> Result<(), Error> {
	let inner = handle.inner.as_ref().map(Arc::downgrade).unwrap_or_default();
	GLOBAL.set(inner).map_err(|_| Error::GlobalAlreadyInstalled)
}

/// Return the process-global trace handle used by MoQ, QUIC, and socket hooks.
pub fn global() -> Handle {
	let inner = GLOBAL.get().and_then(Weak::upgrade);
	Handle {
		inner,
		session_id: None,
		connection_id: None,
	}
}

/// Return a monotonic timestamp in nanoseconds for trace events.
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
