//! Raw JSONL tracing for MoQ relay object and QUIC packet latency.

use std::fs::File;
use std::hash::BuildHasher;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::thread::JoinHandle;

use serde::{Deserialize, Serialize};

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

/// MoQ protocol family represented by an object event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
	/// IETF moq-transport object stream.
	MoqTransport,
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
	/// Return the serialized phase name.
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
	/// Create an identity from its track alias, group ID, and object ID.
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
	/// Create metadata for one moq-transport object.
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
		let logical_id = context.logical_id;
		let Some(sample_rate) = self.object_sample_rate(logical_id) else {
			return ObjectTrace::disabled();
		};
		let trace_id = self
			.inner
			.as_ref()
			.unwrap()
			.next_trace_id
			.fetch_add(1, Ordering::Relaxed);
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
		self.emit(Event::MoqObjectStart(object.clone()));
		ObjectTrace(Some(ObjectTraceState {
			handle: self.clone(),
			trace_id,
			payload_bytes: context.payload_bytes,
			stream_offset_end: None,
		}))
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
