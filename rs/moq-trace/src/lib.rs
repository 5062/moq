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
	/// Quinn stable connection ID when available.
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

/// A named moq-transport object trace point.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum ObjectTracePoint {
	/// Inbound object header parsing started.
	RxObjectHeaderParseStart,
	/// Inbound object header parsing completed.
	RxObjectHeaderParsed,
	/// Inbound object model lookup started.
	RxLookupStart,
	/// Inbound object model lookup completed.
	RxLookupDone,
	/// Inbound object creation started.
	RxObjectCreateStart,
	/// Inbound object creation completed.
	RxObjectCreated,
	/// Inbound object payload read started.
	RxPayloadReadStart,
	/// Inbound object payload read completed.
	RxPayloadReadDone,
	/// Outbound object clone or selection started.
	TxObjectCloneStart,
	/// Outbound object clone or selection completed.
	TxObjectCloned,
	/// Outbound object header encoding started.
	TxObjectHeaderEncodeStart,
	/// Outbound object header encoding completed.
	TxObjectHeaderEncoded,
	/// Outbound object payload write started.
	TxPayloadWriteStart,
	/// Outbound object payload write completed.
	TxPayloadWriteDone,
}

/// moq-transport object trace point fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectPhaseEvent {
	/// Object trace point observed by the instrumentation hook.
	pub point: ObjectTracePoint,
	/// Object metadata associated with the trace point.
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

/// Emit a moq-transport object phase event.
pub fn object_phase(handle: &Handle, point: ObjectTracePoint, object: &ObjectEvent) -> bool {
	let Some(sample_rate) = handle.object_sample_rate(object.object_id) else {
		return false;
	};
	let mut object = object.clone();
	object.timestamp_ns = now_ns();
	object.sample_rate = sample_rate;
	handle.emit(Event::MoqObjectPhase(ObjectPhaseEvent { point, object }))
}

/// A cheap cloneable handle used by instrumentation sites to emit trace events.
#[derive(Clone, Default)]
pub struct Handle {
	inner: Option<Arc<Inner>>,
}

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
		})
	}

	/// Return a disabled handle.
	pub fn disabled() -> Self {
		Self::default()
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
	Handle { inner }
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
			point: ObjectTracePoint::RxObjectCreated,
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
		assert!(json.contains(r#""point":"rx_object_created""#));
		assert!(!json.contains("frame"));
		assert!(!json.contains(r#""phase""#));
		assert!(!json.contains(r#""edge""#));
	}

	#[test]
	fn object_helpers_stamp_and_emit_events() {
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
		object_phase(&handle, ObjectTracePoint::TxObjectHeaderEncoded, &object);
		object_interval_end(&handle, &object);
		drop(handle);

		let contents = std::fs::read_to_string(path).unwrap();
		assert!(contents.contains(r#""type":"moq_object_start""#));
		assert!(contents.contains(r#""type":"moq_object_phase""#));
		assert!(contents.contains(r#""point":"tx_object_header_encoded""#));
		assert!(contents.contains(r#""type":"moq_object_end""#));
		assert!(!contents.contains(&u64::MAX.to_string()));
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
	fn packet_trace_enriches_context_after_header_decode() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path.clone()),
			..Config::default()
		})
		.unwrap();
		let mut packet = handle
			.packet(PacketContext::new(7, Direction::Rx).with_byte_len(1200))
			.unwrap();

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
		let packet = handle
			.packet(
				PacketContext::new(7, Direction::Tx)
					.with_number(2)
					.with_space(PacketSpace::Data),
			)
			.unwrap();

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
