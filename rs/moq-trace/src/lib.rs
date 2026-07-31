//! Raw JSONL tracing for MoQ relay object and QUIC packet latency.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock, Weak};
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
}

/// Trace event direction at the relay boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
	/// Event was observed while receiving from the peer.
	Rx,
	/// Event was observed while transmitting to the peer.
	Tx,
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

/// MoQ object trace fields common to object start and end events.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectEvent {
	/// Monotonic timestamp in nanoseconds from the local process clock.
	pub timestamp_ns: u64,
	/// Process-local MoQ session ID when available.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub session_id: Option<u64>,
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
	/// Exclusive stream byte offset where this object ends, when known.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stream_offset_end: Option<u64>,
	/// Object payload size in bytes.
	pub payload_bytes: u64,
	/// Sampling rate active for this event.
	pub sample_rate: u64,
}

/// A measured step in the moq-transport object lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ObjectPhase {
	/// Parse an inbound object header.
	HeaderParse,
	/// Create an inbound object in the relay model.
	Create,
	/// Read an inbound object payload.
	PayloadRead,
	/// Clone or select an outbound object from the relay model.
	Clone,
	/// Encode an outbound object header.
	HeaderEncode,
	/// Write an outbound object payload.
	PayloadWrite,
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
	session_id: Option<u64>,
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
	pub fn new(direction: Direction, identity: ObjectIdentity) -> Self {
		Self {
			session_id: None,
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
	object: ObjectEvent,
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
			let object = &mut state.object;
			object.payload_bytes = payload_bytes;
		}
	}

	/// Update the exclusive stream byte offset reached by this object.
	pub fn set_stream_offset_end(&mut self, stream_offset_end: u64) {
		if let Some(state) = &mut self.0 {
			state.object.stream_offset_end = Some(stream_offset_end);
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
		let mut object = state.object.clone();
		object.timestamp_ns = now_ns();
		state.handle.emit(Event::MoqObjectPhase(ObjectPhaseEvent {
			phase,
			edge,
			outcome,
			object,
		}));
	}

	/// Finish the object interval with the latest metadata.
	pub fn finish(mut self) {
		let Some(mut state) = self.0.take() else {
			return;
		};
		state.object.timestamp_ns = now_ns();
		state.handle.emit(Event::MoqObjectEnd(state.object));
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
pub struct ObjectPhaseEvent {
	/// Object lifecycle phase being measured.
	pub phase: ObjectPhase,
	/// Whether this boundary starts or completes the phase.
	pub edge: PhaseEdge,
	/// Completion result, present only when `edge` is `done`.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub outcome: Option<ObjectOutcome>,
	/// Object metadata associated with the phase boundary.
	#[serde(flatten)]
	pub object: ObjectEvent,
}

/// One JSONL trace record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
	/// First byte of a moq-transport object was observed.
	#[serde(rename = "moq_object_start")]
	MoqObjectStart(ObjectEvent),
	/// Final byte of a moq-transport object was observed.
	#[serde(rename = "moq_object_end")]
	MoqObjectEnd(ObjectEvent),
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
			Self::SocketStart(event) => Some(event.trace_id),
			Self::PacketStart(event) => Some(event.trace_id),
			Self::PacketEnd(event) => Some(event.packet.trace_id),
			Self::PacketPhase(event) => Some(event.packet.trace_id),
			Self::StreamFrame(event) => Some(event.packet.trace_id),
			Self::SocketEnd(event) => Some(event.socket.trace_id),
			_ => None,
		}
	}

	fn object_id(&self) -> Option<u64> {
		match self {
			Self::MoqObjectStart(event) | Self::MoqObjectEnd(event) => Some(event.object_id),
			Self::MoqObjectPhase(event) => Some(event.object.object_id),
			_ => None,
		}
	}

	fn set_sample_rate(&mut self, sample_rate: u64) {
		match self {
			Self::MoqObjectStart(event) | Self::MoqObjectEnd(event) => event.sample_rate = sample_rate,
			Self::MoqObjectPhase(event) => event.object.sample_rate = sample_rate,
			_ => {}
		}
	}
}

/// Emit the canonical moq-transport object interval start event.
pub fn object_interval_start(handle: &Handle, object: &ObjectEvent) -> bool {
	let Some(sample_rate) = handle.object_sample_rate(object.object_id) else {
		return false;
	};
	let mut object = object.clone();
	object.timestamp_ns = now_ns();
	object.sample_rate = sample_rate;
	handle.emit(Event::MoqObjectStart(object))
}

/// Emit the canonical moq-transport object interval end event.
pub fn object_interval_end(handle: &Handle, object: &ObjectEvent) -> bool {
	let Some(sample_rate) = handle.object_sample_rate(object.object_id) else {
		return false;
	};
	let mut object = object.clone();
	object.timestamp_ns = now_ns();
	object.sample_rate = sample_rate;
	handle.emit(Event::MoqObjectEnd(object))
}

/// A cheap cloneable handle used by instrumentation sites to emit trace events.
#[derive(Clone, Default)]
pub struct Handle {
	inner: Option<Arc<Inner>>,
	session_id: Option<u64>,
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

	/// Return a clone that stamps object events with the next process-local session ID.
	pub fn with_new_session_id(mut self) -> Self {
		if self.inner.is_some() {
			self.session_id = Some(NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed));
		}
		self
	}

	fn object_sample_rate(&self, object_id: u64) -> Option<u64> {
		let inner = self.inner.as_ref()?;
		let sample = inner.config.object_sample;
		if object_id % sample == sample - 1 {
			Some(sample)
		} else {
			None
		}
	}

	/// Start a moq-transport object trace after applying object sampling.
	pub fn object(&self, context: ObjectContext) -> ObjectTrace {
		let Some(sample_rate) = self.object_sample_rate(context.object_id) else {
			return ObjectTrace::disabled();
		};
		let object = ObjectEvent {
			timestamp_ns: now_ns(),
			session_id: context.session_id.or(self.session_id),
			direction: context.direction,
			protocol: Protocol::MoqTransport,
			track_alias: context.track_alias,
			group_id: context.group_id,
			object_id: context.object_id,
			stream_id: context.stream_id,
			stream_offset_start: context.stream_offset_start,
			stream_offset_end: None,
			payload_bytes: context.payload_bytes,
			sample_rate,
		};
		self.emit(Event::MoqObjectStart(object.clone()));
		ObjectTrace(Some(ObjectTraceState {
			handle: self.clone(),
			object,
		}))
	}

	/// Emit an event without applying object or packet sampling.
	pub fn emit(&self, event: Event) -> bool {
		let Some(inner) = &self.inner else {
			return false;
		};
		inner.emit(event)
	}

	/// Emit a MoQ object event after sampling by object ID.
	pub fn emit_object(&self, mut event: Event) -> bool {
		let Some(inner) = &self.inner else {
			return false;
		};
		let Some(object_id) = event.object_id() else {
			return false;
		};
		let sample = inner.config.object_sample;
		if object_id % sample != sample - 1 {
			return false;
		}
		event.set_sample_rate(sample);
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

static GLOBAL: OnceLock<RwLock<Weak<Inner>>> = OnceLock::new();

/// Replace the process-global trace handle used by vendored QUIC hooks.
///
/// The registry does not extend the handle lifetime, so the caller must retain a clone.
pub fn set_global(handle: Handle) {
	let inner = handle.inner.as_ref().map(Arc::downgrade).unwrap_or_default();
	*GLOBAL
		.get_or_init(|| RwLock::new(Weak::new()))
		.write()
		.expect("trace global poisoned") = inner;
}

/// Return the process-global trace handle used by vendored QUIC hooks.
pub fn global() -> Handle {
	let inner = GLOBAL
		.get_or_init(|| RwLock::new(Weak::new()))
		.read()
		.expect("trace global poisoned")
		.upgrade();
	Handle {
		inner,
		session_id: None,
	}
}

/// Clear the process-global trace handle.
pub fn clear_global() {
	set_global(Handle::disabled());
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
mod tests {
	use std::io;

	use super::*;

	fn object_event() -> Event {
		Event::MoqObjectEnd(ObjectEvent {
			timestamp_ns: 42,
			session_id: Some(7),
			direction: Direction::Tx,
			protocol: Protocol::MoqTransport,
			track_alias: 11,
			group_id: 12,
			object_id: 13,
			stream_id: Some(16),
			stream_offset_start: Some(100),
			stream_offset_end: Some(144),
			payload_bytes: 44,
			sample_rate: 1,
		})
	}

	#[test]
	fn config_rejects_unknown_fields() {
		let error = serde_json::from_str::<Config>(r#"{"unexpected":true}"#).unwrap_err();

		assert!(error.to_string().contains("unknown field `unexpected`"));
	}

	#[test]
	fn config_uses_default_when_fields_are_missing() {
		let config = serde_json::from_str::<Config>("{}").unwrap();
		let default = Config::default();

		assert_eq!(config.path, default.path);
		assert_eq!(config.object_sample, default.object_sample);
		assert_eq!(config.packet_sample, default.packet_sample);
		assert_eq!(config.socket_sample, default.socket_sample);
		assert_eq!(config.queue_capacity, default.queue_capacity);
	}

	#[test]
	fn flush_makes_events_visible_while_clones_are_alive() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::default()
		})
		.unwrap();
		let clone = handle.clone();

		assert!(handle.emit(object_event()));
		assert!(handle.flush());
		assert_eq!(std::fs::read_to_string(path).unwrap().lines().count(), 1);
		drop(clone);
	}

	#[test]
	fn serializes_jsonl_event_type() {
		let json = serde_json::to_string(&object_event()).unwrap();
		assert!(json.contains(r#""type":"moq_object_end""#));
		assert!(json.contains(r#""session_id":7"#));
		assert!(json.contains(r#""protocol":"moq_transport""#));
	}

	#[test]
	fn serializes_object_phase_event() {
		let event = Event::MoqObjectPhase(ObjectPhaseEvent {
			phase: ObjectPhase::Create,
			edge: PhaseEdge::Done,
			outcome: Some(ObjectOutcome::Success),
			object: ObjectEvent {
				timestamp_ns: 42,
				session_id: Some(7),
				direction: Direction::Rx,
				protocol: Protocol::MoqTransport,
				track_alias: 11,
				group_id: 12,
				object_id: 13,
				stream_id: Some(16),
				stream_offset_start: Some(100),
				stream_offset_end: Some(144),
				payload_bytes: 44,
				sample_rate: 1,
			},
		});

		let json = serde_json::to_string(&event).unwrap();
		assert!(json.contains(r#""type":"moq_object_phase""#));
		assert!(json.contains(r#""phase":"create""#));
		assert!(json.contains(r#""edge":"done""#));
		assert!(json.contains(r#""outcome":"success""#));
		assert!(!json.contains("frame"));
		assert!(!json.contains(r#""point""#));
	}

	#[test]
	fn object_interval_helpers_stamp_and_emit_events() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::disabled()
		})
		.unwrap();
		let object = ObjectEvent {
			timestamp_ns: u64::MAX,
			session_id: Some(7),
			direction: Direction::Tx,
			protocol: Protocol::MoqTransport,
			track_alias: 11,
			group_id: 12,
			object_id: 13,
			stream_id: Some(16),
			stream_offset_start: Some(100),
			stream_offset_end: Some(144),
			payload_bytes: 44,
			sample_rate: 0,
		};

		object_interval_start(&handle, &object);
		object_interval_end(&handle, &object);
		drop(handle);

		let contents = std::fs::read_to_string(path).unwrap();
		assert!(contents.contains(r#""type":"moq_object_start""#));
		assert!(contents.contains(r#""type":"moq_object_end""#));
		assert!(!contents.contains(&u64::MAX.to_string()));
	}

	#[test]
	fn object_phase_guard_records_latest_metadata_and_abandonment() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::disabled()
		})
		.unwrap();
		let mut object = handle.object(ObjectContext::new(Direction::Rx, ObjectIdentity::new(11, 12, 13)));

		{
			let mut phase = object.phase(ObjectPhase::HeaderParse);
			phase.set_payload_bytes(44);
			phase.set_stream_offset_end(144);
		}
		object.finish();
		drop(handle);

		let events: Vec<Event> = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect();
		assert!(matches!(
			&events[2],
			Event::MoqObjectPhase(ObjectPhaseEvent {
				phase: ObjectPhase::HeaderParse,
				edge: PhaseEdge::Done,
				outcome: Some(ObjectOutcome::Abandoned),
				object: ObjectEvent {
					payload_bytes: 44,
					stream_offset_end: Some(144),
					..
				},
			})
		));
	}

	#[test]
	fn object_trace_updates_metadata_and_emits_interval() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::disabled()
		})
		.unwrap();
		let context = ObjectContext::new(Direction::Tx, ObjectIdentity::new(11, 12, 13))
			.with_session_id(7)
			.with_stream_id(16)
			.with_stream_offset_start(100)
			.with_payload_bytes(44);

		let mut object = handle.object(context);
		let mut phase = object.phase(ObjectPhase::HeaderEncode);
		phase.set_payload_bytes(44);
		phase.set_stream_offset_end(144);
		phase.finish(ObjectOutcome::Success);
		object.finish();
		drop(handle);

		let contents = std::fs::read_to_string(path).unwrap();
		let events = contents
			.lines()
			.map(|line| serde_json::from_str::<Event>(line).unwrap())
			.collect::<Vec<_>>();
		assert_eq!(events.len(), 4);
		assert!(matches!(
			events[0],
			Event::MoqObjectStart(ref object) if object.payload_bytes == 44
		));
		assert!(matches!(
			events[1],
			Event::MoqObjectPhase(ObjectPhaseEvent {
				phase: ObjectPhase::HeaderEncode,
				edge: PhaseEdge::Start,
				outcome: None,
				..
			})
		));
		assert!(matches!(
			events[2],
			Event::MoqObjectPhase(ObjectPhaseEvent {
				phase: ObjectPhase::HeaderEncode,
				edge: PhaseEdge::Done,
				outcome: Some(ObjectOutcome::Success),
				ref object,
			}) if object.payload_bytes == 44 && object.stream_offset_end == Some(144)
		));
		assert!(matches!(
			events[3],
			Event::MoqObjectEnd(ref object)
				if object.session_id == Some(7)
					&& object.stream_id == Some(16)
					&& object.stream_offset_start == Some(100)
					&& object.stream_offset_end == Some(144)
		));
	}

	#[test]
	fn session_scoped_handle_stamps_object_events() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::disabled()
		})
		.unwrap()
		.with_session_id(7);

		handle
			.object(ObjectContext::new(Direction::Tx, ObjectIdentity::new(11, 12, 13)))
			.finish();
		drop(handle);

		let events = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str::<Event>(line).unwrap())
			.collect::<Vec<_>>();
		assert!(events.iter().all(|event| match event {
			Event::MoqObjectStart(object) | Event::MoqObjectEnd(object) => object.session_id == Some(7),
			_ => false,
		}));
	}

	#[test]
	fn sequential_session_ids_start_at_one() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::disabled()
		})
		.unwrap();
		let first = handle.clone().with_new_session_id();
		let second = handle.with_new_session_id();

		first
			.object(ObjectContext::new(Direction::Tx, ObjectIdentity::new(11, 12, 13)))
			.finish();
		second
			.object(ObjectContext::new(Direction::Tx, ObjectIdentity::new(21, 22, 23)))
			.finish();
		drop(first);
		drop(second);

		let session_ids = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str::<Event>(line).unwrap())
			.filter_map(|event| match event {
				Event::MoqObjectStart(object) => object.session_id,
				_ => None,
			})
			.collect::<Vec<_>>();
		assert_eq!(session_ids, vec![1, 2]);
	}

	#[test]
	fn object_trace_samples_once_before_emitting_phases() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			object_sample: 3,
			..Config::disabled()
		})
		.unwrap();

		for object_id in 0..3 {
			let mut object = handle.object(ObjectContext::new(
				Direction::Rx,
				ObjectIdentity::new(11, 12, object_id),
			));
			object.phase(ObjectPhase::HeaderParse).finish(ObjectOutcome::Success);
			object.finish();
		}
		drop(handle);

		let contents = std::fs::read_to_string(path).unwrap();
		let events = contents
			.lines()
			.map(|line| serde_json::from_str::<Event>(line).unwrap())
			.collect::<Vec<_>>();
		assert_eq!(events.len(), 4);
		assert!(events.iter().all(|event| match event {
			Event::MoqObjectStart(object) | Event::MoqObjectEnd(object) => {
				object.object_id == 2 && object.sample_rate == 3
			}
			Event::MoqObjectPhase(event) => event.object.object_id == 2 && event.object.sample_rate == 3,
			_ => false,
		}));
	}

	#[test]
	fn disabled_object_trace_is_noop() {
		let handle = Handle::new(Config::disabled()).unwrap();
		let mut object = handle.object(ObjectContext::new(Direction::Rx, ObjectIdentity::new(11, 12, 13)));

		let mut phase = object.phase(ObjectPhase::HeaderParse);
		phase.set_payload_bytes(44);
		phase.set_stream_offset_end(144);
		phase.finish(ObjectOutcome::Success);
		object.finish();

		assert_eq!(handle.emitted(), 0);
	}
	#[test]
	fn samples_all_events_for_every_nth_object() {
		let dir = tempfile::tempdir().unwrap();
		let handle = Handle::new(Config {
			path: Some(dir.path().join("trace.jsonl")),
			object_sample: 3,
			..Config::disabled()
		})
		.unwrap();

		let mut skipped = object_event();
		if let Event::MoqObjectEnd(object) = &mut skipped {
			object.object_id = 0;
		}

		assert!(!handle.emit_object(skipped.clone()));
		assert!(!handle.emit_object(skipped.clone()));
		assert!(!handle.emit_object(skipped));

		let mut sampled = object_event();
		if let Event::MoqObjectEnd(object) = &mut sampled {
			object.object_id = 2;
		}

		assert!(handle.emit_object(sampled.clone()));
		assert!(handle.emit_object(sampled.clone()));
		assert!(handle.emit_object(sampled));
		assert_eq!(handle.emitted(), 3);
	}

	#[test]
	fn config_without_path_is_disabled() {
		let handle = Handle::new(Config::disabled()).unwrap();

		assert!(!handle.emit(object_event()));
		assert_eq!(handle.emitted(), 0);
		assert_eq!(handle.dropped(), 0);
	}

	#[test]
	fn global_handle_does_not_keep_writer_alive() {
		clear_global();
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::disabled()
		})
		.unwrap();
		set_global(handle.clone());

		assert!(global().emit(object_event()));
		drop(handle);

		assert!(!global().emit(object_event()));
		let contents = std::fs::read_to_string(path).unwrap();
		assert!(contents.contains(r#""type":"moq_object_end""#));
	}

	struct FailingWriter;

	impl Write for FailingWriter {
		fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
			Err(io::Error::other("expected test failure"))
		}

		fn flush(&mut self) -> io::Result<()> {
			Ok(())
		}
	}

	#[test]
	fn records_writer_failure() {
		let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let (sender, receiver) = std::sync::mpsc::sync_channel(1);
		sender.send(WriterCommand::Event(object_event())).unwrap();
		drop(sender);

		write_events(FailingWriter, receiver, failed.clone());
		let handle = Handle {
			inner: Some(Arc::new(Inner::new(Config::default(), None, None, failed))),
			session_id: None,
		};
		assert!(handle.writer_failed());
	}

	#[test]
	fn drops_when_queue_is_full() {
		let (sender, receiver) = std::sync::mpsc::sync_channel(1);
		let handle = Handle {
			inner: Some(Arc::new(Inner::new(
				Config::default(),
				Some(sender),
				None,
				Arc::new(AtomicBool::new(false)),
			))),
			session_id: None,
		};

		assert!(handle.emit(object_event()));
		assert!(!handle.emit(object_event()));
		assert_eq!(handle.dropped(), 1);
		drop(handle);
		drop(receiver);
	}

	#[test]
	fn writer_flushes_on_drop() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		{
			let handle = Handle::new(Config {
				path: Some(path.clone()),
				..Config::disabled()
			})
			.unwrap();
			assert!(handle.emit(object_event()));
		}

		let contents = std::fs::read_to_string(&path).unwrap();
		assert!(contents.contains(r#""type":"moq_object_end""#));
	}

	#[test]
	fn socket_trace_keeps_start_and_end_together() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			socket_sample: 2,
			..Config::default()
		})
		.unwrap();

		assert!(handle.socket(Direction::Rx, None).is_none());
		handle
			.socket(Direction::Rx, None)
			.expect("second socket operation should be sampled")
			.finish(SocketOutcome::Success, SocketStats::new(2, 5, 6144));
		drop(handle);

		let events: Vec<Event> = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect();
		assert_eq!(events.len(), 2);
		assert_eq!(events[0].trace_id(), events[1].trace_id());
	}

	#[test]
	fn dropped_socket_trace_records_abandoned() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::default()
		})
		.unwrap();

		drop(handle.socket(Direction::Tx, Some(7)).unwrap());
		drop(handle);

		let events: Vec<Event> = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect();
		assert!(matches!(
			events.as_slice(),
			[
				Event::SocketStart(_),
				Event::SocketEnd(SocketEndEvent {
					outcome: SocketOutcome::Abandoned,
					..
				})
			]
		));
	}

	#[test]
	fn serialized_events_can_be_read_back() {
		let json = serde_json::to_string(&object_event()).unwrap();
		let event: Event = serde_json::from_str(&json).unwrap();

		assert_eq!(event, object_event());
	}

	#[test]
	fn dropped_packet_trace_records_abandoned() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::default()
		})
		.unwrap();

		drop(handle.packet(PacketContext::new(Direction::Rx, 7)));
		drop(handle);

		let events: Vec<Event> = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect();
		assert!(matches!(
			events.as_slice(),
			[
				Event::PacketStart(_),
				Event::PacketEnd(PacketEndEvent {
					outcome: PacketOutcome::Abandoned,
					..
				})
			]
		));
	}

	#[test]
	fn dropped_packet_phase_records_abandoned() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::default()
		})
		.unwrap();
		let packet = handle.packet(PacketContext::new(Direction::Rx, 7));

		drop(packet.phase(PacketPhase::HeaderParse));
		packet.finish(PacketOutcome::Success);
		drop(handle);

		let events: Vec<Event> = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect();
		assert!(matches!(
			&events[2],
			Event::PacketPhase(PacketPhaseEvent {
				edge: PhaseEdge::Done,
				outcome: Some(PacketOutcome::Abandoned),
				..
			})
		));
	}

	#[test]
	fn disabled_packet_trace_is_noop() {
		let handle = Handle::new(Config::disabled()).unwrap();
		let mut packet = handle.packet(PacketContext::new(Direction::Rx, 7));

		packet.phase(PacketPhase::HeaderParse).finish(PacketOutcome::Success);
		packet.set_number(91);
		packet.set_space(PacketSpace::Data);
		packet.set_byte_len(1200);
		packet.stream_frame(StreamFrame::new(16, 0, 10), PacketOutcome::Success);
		packet.finish(PacketOutcome::Success);

		assert_eq!(handle.emitted(), 0);
	}

	#[test]
	fn unsampled_packet_trace_is_noop() {
		let dir = tempfile::tempdir().unwrap();
		let handle = Handle::new(Config {
			path: Some(dir.path().join("trace.jsonl")),
			packet_sample: 2,
			..Config::default()
		})
		.unwrap();
		let packet = handle.packet(PacketContext::new(Direction::Tx, 7).with_number(0));

		packet.phase(PacketPhase::FrameEncode).finish(PacketOutcome::Success);
		packet.finish(PacketOutcome::Success);

		assert_eq!(handle.emitted(), 0);
	}

	#[test]
	fn packet_trace_enriches_context_after_header_decode() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::default()
		})
		.unwrap();
		let mut packet = handle.packet(PacketContext::new(Direction::Rx, 7).with_byte_len(1200));

		packet.phase(PacketPhase::HeaderParse).finish(PacketOutcome::Success);
		packet.set_space(PacketSpace::Data);
		packet.set_number(91);
		packet
			.phase(PacketPhase::HeaderUnprotect)
			.finish(PacketOutcome::Success);
		packet.stream_frame(StreamFrame::new(16, 120, 520), PacketOutcome::Success);
		packet.finish(PacketOutcome::Success);
		drop(handle);

		let events: Vec<Event> = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect();
		assert!(matches!(
			events.last(),
			Some(Event::PacketEnd(PacketEndEvent {
				packet: PacketEvent {
					packet_number: Some(91),
					packet_space: Some(PacketSpace::Data),
					..
				},
				outcome: PacketOutcome::Success,
			}))
		));
		assert!(events.iter().all(|event| event.trace_id() == Some(1)));
	}

	#[test]
	fn packet_with_multiple_stream_frames_has_one_interval() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::default()
		})
		.unwrap();
		let packet = handle.packet(
			PacketContext::new(Direction::Tx, 7)
				.with_number(2)
				.with_space(PacketSpace::Data),
		);

		packet.stream_frame(StreamFrame::new(4, 0, 10), PacketOutcome::Success);
		packet.stream_frame(StreamFrame::new(8, 20, 40), PacketOutcome::Success);
		packet.finish(PacketOutcome::Success);
		drop(handle);

		let events: Vec<Event> = std::fs::read_to_string(path)
			.unwrap()
			.lines()
			.map(|line| serde_json::from_str(line).unwrap())
			.collect();
		assert_eq!(
			events
				.iter()
				.filter(|event| matches!(event, Event::PacketStart(_)))
				.count(),
			1
		);
		assert_eq!(
			events
				.iter()
				.filter(|event| matches!(event, Event::PacketEnd(_)))
				.count(),
			1
		);
		assert_eq!(
			events
				.iter()
				.filter(|event| matches!(event, Event::StreamFrame(_)))
				.count(),
			2
		);
	}
}
