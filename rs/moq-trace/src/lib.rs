//! Raw JSONL tracing for MoQ relay object and QUIC packet latency.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;

use serde::{Deserialize, Serialize};

/// Tracing configuration shared by MoQ and QUIC instrumentation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
	/// File path for newline-delimited JSON events. `None` keeps tracing in no-op mode.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
	/// Emit every Nth MoQ object event. Values below 1 are treated as 1.
	#[serde(default = "default_sample")]
	pub object_sample: u64,
	/// Emit every Nth QUIC packet event. Values below 1 are treated as 1.
	#[serde(default = "default_sample")]
	pub packet_sample: u64,
	/// Maximum queued events before new events are dropped.
	#[serde(default = "default_queue_capacity")]
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
			queue_capacity: default_queue_capacity(),
		}
	}
}

fn default_sample() -> u64 {
	1
}

fn default_queue_capacity() -> usize {
	4096
}

/// Errors returned when creating a trace writer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
	/// The trace output file could not be created.
	#[error("failed to create trace output")]
	Create(#[source] std::io::Error),
}

/// Trace event direction at the relay boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
	/// Event was observed while reading from the peer.
	Inbound,
	/// Event was observed while writing to the peer.
	Outbound,
}

/// MoQ protocol family represented by an object event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
	pub at_ns: u128,
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

/// A named QUIC packet trace point.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum PacketTracePoint {
	/// Entry to an inbound UDP socket read.
	RxSocketIoStart,
	/// Completion of an inbound UDP socket read.
	RxSocketIoDone,
	/// Entry to inbound QUIC packet header parsing.
	RxPacketHeaderParseStart,
	/// Inbound QUIC packet header parsing completed.
	RxPacketHeaderParsed,
	/// Entry to inbound QUIC packet decryption.
	RxPacketDecryptStart,
	/// Inbound QUIC packet decryption completed.
	RxPacketDecrypted,
	/// Entry to inbound QUIC STREAM frame processing.
	RxStreamFrameProcessStart,
	/// Inbound QUIC STREAM frame processing completed.
	RxStreamFrameProcessed,
	/// Entry to outbound QUIC packet encoding.
	TxPacketEncodeStart,
	/// Outbound QUIC packet encoding completed.
	TxPacketEncoded,
	/// Entry to outbound QUIC packet encryption.
	TxPacketEncryptStart,
	/// Outbound QUIC packet encryption completed.
	TxPacketEncrypted,
	/// Entry to an outbound UDP socket write.
	TxSocketIoStart,
	/// Completion of an outbound UDP socket write.
	TxSocketIoDone,
}

/// QUIC packet trace fields common to packet start and end events.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PacketEvent {
	/// Monotonic timestamp in nanoseconds from the local process clock.
	pub at_ns: u128,
	/// Quinn stable connection ID when available.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub session_id: Option<u64>,
	/// Whether this packet is entering or leaving the relay.
	pub direction: Direction,
	/// QUIC packet number, when known at this hook.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub packet_number: Option<u64>,
	/// QUIC packet number space, when the trace point is tied to one.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub packet_space: Option<PacketSpace>,
	/// UDP datagram length in bytes, when known at this hook.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub udp_len: Option<usize>,
	/// QUIC stream ID for a STREAM frame carried by this packet, when present.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stream_id: Option<u64>,
	/// Inclusive STREAM frame offset, when present.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stream_offset_start: Option<u64>,
	/// Exclusive STREAM frame offset, when present.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub stream_offset_end: Option<u64>,
	/// Sampling rate active for this event.
	pub sample_rate: u64,
}

/// QUIC packet trace point fields.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PacketPhaseEvent {
	/// Packet trace point observed by the instrumentation hook.
	pub point: PacketTracePoint,
	/// Packet metadata associated with the trace point.
	#[serde(flatten)]
	pub packet: PacketEvent,
}

/// One JSONL trace record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
	PacketEnd(PacketEvent),
	/// QUIC packet processing phase boundary.
	#[serde(rename = "quic_packet_phase")]
	PacketPhase(PacketPhaseEvent),
}

impl Event {
	fn set_sample_rate(&mut self, sample_rate: u64) {
		match self {
			Self::MoqObjectStart(event) | Self::MoqObjectEnd(event) => event.sample_rate = sample_rate,
			Self::MoqObjectPhase(event) => event.object.sample_rate = sample_rate,
			Self::PacketStart(event) | Self::PacketEnd(event) => event.sample_rate = sample_rate,
			Self::PacketPhase(event) => event.packet.sample_rate = sample_rate,
		}
	}
}

/// Emit the canonical moq-transport object interval start event.
pub fn object_interval_start(handle: &Handle, object: &ObjectEvent) -> bool {
	let mut object = object.clone();
	object.at_ns = now_ns();
	handle.emit_object(Event::MoqObjectStart(object))
}

/// Emit the canonical moq-transport object interval end event.
pub fn object_interval_end(handle: &Handle, object: &ObjectEvent) -> bool {
	let mut object = object.clone();
	object.at_ns = now_ns();
	handle.emit_object(Event::MoqObjectEnd(object))
}

/// Emit a moq-transport object phase event.
pub fn object_phase(handle: &Handle, point: ObjectTracePoint, object: &ObjectEvent) -> bool {
	let mut object = object.clone();
	object.at_ns = now_ns();
	handle.emit_object(Event::MoqObjectPhase(ObjectPhaseEvent { point, object }))
}

/// A cheap cloneable handle used by instrumentation sites to emit trace events.
#[derive(Clone, Default)]
pub struct Handle {
	inner: Option<Arc<Inner>>,
}

struct Inner {
	config: Config,
	sender: Mutex<Option<std::sync::mpsc::SyncSender<Event>>>,
	writer: Mutex<Option<JoinHandle<()>>>,
	object_seen: AtomicU64,
	packet_seen: AtomicU64,
	emitted: AtomicU64,
	dropped: AtomicU64,
}

impl Handle {
	/// Create a handle from config, spawning a writer thread when `path` is set.
	pub fn new(config: Config) -> Result<Self, Error> {
		let config = config.normalized();
		let Some(path) = config.path.clone() else {
			return Ok(Self {
				inner: Some(Arc::new(Inner::new(config, None, None))),
			});
		};

		let file = File::create(path).map_err(Error::Create)?;
		let (sender, receiver) = std::sync::mpsc::sync_channel(config.queue_capacity);
		let writer = std::thread::spawn(move || write_events(file, receiver));
		Ok(Self {
			inner: Some(Arc::new(Inner::new(config, Some(sender), Some(writer)))),
		})
	}

	/// Return a disabled handle.
	pub fn disabled() -> Self {
		Self::default()
	}

	/// Emit an event without applying object or packet sampling.
	pub fn emit(&self, event: Event) -> bool {
		let Some(inner) = &self.inner else {
			return false;
		};
		inner.emit(event)
	}

	/// Emit a MoQ object event after applying object sampling.
	pub fn emit_object(&self, mut event: Event) -> bool {
		let Some(inner) = &self.inner else {
			return false;
		};
		let sample = inner.config.object_sample;
		let seen = inner.object_seen.fetch_add(1, Ordering::Relaxed) + 1;
		if seen % sample != 0 {
			return false;
		}
		event.set_sample_rate(sample);
		inner.emit(event)
	}

	/// Emit a QUIC packet event after applying packet sampling.
	pub fn emit_packet(&self, mut event: Event) -> bool {
		let Some(inner) = &self.inner else {
			return false;
		};
		let sample = inner.config.packet_sample;
		let seen = inner.packet_seen.fetch_add(1, Ordering::Relaxed) + 1;
		if seen % sample != 0 {
			return false;
		}
		event.set_sample_rate(sample);
		inner.emit(event)
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
}

impl Inner {
	fn new(config: Config, sender: Option<std::sync::mpsc::SyncSender<Event>>, writer: Option<JoinHandle<()>>) -> Self {
		Self {
			config,
			sender: Mutex::new(sender),
			writer: Mutex::new(writer),
			object_seen: AtomicU64::new(0),
			packet_seen: AtomicU64::new(0),
			emitted: AtomicU64::new(0),
			dropped: AtomicU64::new(0),
		}
	}

	fn emit(&self, event: Event) -> bool {
		let Some(sender) = self.sender.lock().expect("trace sender poisoned").as_ref().cloned() else {
			self.emitted.fetch_add(1, Ordering::Relaxed);
			return true;
		};

		match sender.try_send(event) {
			Ok(()) => {
				self.emitted.fetch_add(1, Ordering::Relaxed);
				true
			}
			Err(_) => {
				self.dropped.fetch_add(1, Ordering::Relaxed);
				false
			}
		}
	}
}

impl Drop for Inner {
	fn drop(&mut self) {
		self.sender.lock().expect("trace sender poisoned").take();
		if let Some(writer) = self.writer.lock().expect("trace writer poisoned").take() {
			let _ = writer.join();
		}
	}
}

fn write_events(file: File, receiver: std::sync::mpsc::Receiver<Event>) {
	let mut file = BufWriter::new(file);
	for event in receiver {
		if serde_json::to_writer(&mut file, &event).is_err() {
			break;
		}
		if file.write_all(b"\n").is_err() {
			break;
		}
	}
	let _ = file.flush();
}

static GLOBAL: OnceLock<Mutex<Handle>> = OnceLock::new();

/// Replace the process-global trace handle used by vendored QUIC hooks.
pub fn set_global(handle: Handle) {
	*GLOBAL
		.get_or_init(|| Mutex::new(Handle::disabled()))
		.lock()
		.expect("trace global poisoned") = handle;
}

/// Return the process-global trace handle used by vendored QUIC hooks.
pub fn global() -> Handle {
	GLOBAL
		.get_or_init(|| Mutex::new(Handle::disabled()))
		.lock()
		.expect("trace global poisoned")
		.clone()
}

/// Clear the process-global trace handle.
pub fn clear_global() {
	set_global(Handle::disabled());
}

/// Return a monotonic timestamp in nanoseconds for trace events.
pub fn now_ns() -> u128 {
	static START: OnceLock<std::time::Instant> = OnceLock::new();
	START.get_or_init(std::time::Instant::now).elapsed().as_nanos()
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	fn object_event() -> Event {
		Event::MoqObjectEnd(ObjectEvent {
			at_ns: 42,
			session_id: Some(7),
			direction: Direction::Outbound,
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
				at_ns: 42,
				session_id: Some(7),
				direction: Direction::Inbound,
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
	fn serializes_packet_phase_event() {
		let event = Event::PacketPhase(PacketPhaseEvent {
			point: PacketTracePoint::RxPacketDecrypted,
			packet: PacketEvent {
				at_ns: 42,
				session_id: Some(7),
				direction: Direction::Inbound,
				packet_number: Some(2),
				packet_space: Some(PacketSpace::Data),
				udp_len: Some(1200),
				stream_id: None,
				stream_offset_start: None,
				stream_offset_end: None,
				sample_rate: 1,
			},
		});

		let json = serde_json::to_string(&event).unwrap();
		assert!(json.contains(r#""type":"quic_packet_phase""#));
		assert!(json.contains(r#""point":"rx_packet_decrypted""#));
		assert!(!json.contains(r#""phase""#));
		assert!(!json.contains(r#""edge""#));
	}

	#[test]
	fn omits_packet_space_when_trace_point_is_socket_scoped() {
		let event = Event::PacketPhase(PacketPhaseEvent {
			point: PacketTracePoint::RxSocketIoDone,
			packet: PacketEvent {
				at_ns: 42,
				session_id: Some(7),
				direction: Direction::Inbound,
				packet_number: None,
				packet_space: None,
				udp_len: Some(1200),
				stream_id: None,
				stream_offset_start: None,
				stream_offset_end: None,
				sample_rate: 1,
			},
		});

		let json = serde_json::to_string(&event).unwrap();
		assert!(json.contains(r#""point":"rx_socket_io_done""#));
		assert!(!json.contains(r#""packet_space""#));
	}

	#[test]
	fn omits_udp_len_when_trace_point_has_not_measured_it_yet() {
		let event = Event::PacketPhase(PacketPhaseEvent {
			point: PacketTracePoint::TxPacketEncodeStart,
			packet: PacketEvent {
				at_ns: 42,
				session_id: Some(7),
				direction: Direction::Outbound,
				packet_number: Some(2),
				packet_space: Some(PacketSpace::Data),
				udp_len: None,
				stream_id: None,
				stream_offset_start: None,
				stream_offset_end: None,
				sample_rate: 1,
			},
		});

		let json = serde_json::to_string(&event).unwrap();
		assert!(json.contains(r#""point":"tx_packet_encode_start""#));
		assert!(!json.contains(r#""udp_len""#));
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
			at_ns: u128::MAX,
			session_id: Some(7),
			direction: Direction::Outbound,
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
		assert!(!contents.contains(&u128::MAX.to_string()));
	}

	#[test]
	fn samples_every_n_events_and_records_rate() {
		let handle = Handle::new(Config {
			object_sample: 3,
			..Config::disabled()
		})
		.unwrap();

		let event = object_event();
		assert!(!handle.emit_object(event.clone()));
		assert!(!handle.emit_object(event.clone()));
		assert!(handle.emit_object(event));
		assert_eq!(handle.emitted(), 1);
	}

	#[test]
	fn drops_when_queue_is_full() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("trace.jsonl");
		let handle = Handle::new(Config {
			path: Some(path),
			queue_capacity: 1,
			..Config::disabled()
		})
		.unwrap();

		let mut saw_drop = false;
		for _ in 0..1000 {
			handle.emit(Event::PacketEnd(PacketEvent {
				at_ns: 1,
				session_id: Some(1),
				direction: Direction::Outbound,
				packet_number: Some(2),
				packet_space: Some(PacketSpace::Data),
				udp_len: Some(1200),
				stream_id: Some(0),
				stream_offset_start: Some(0),
				stream_offset_end: Some(10),
				sample_rate: 1,
			}));
			if handle.dropped() > 0 {
				saw_drop = true;
				break;
			}
		}
		assert!(saw_drop);
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

		let deadline = std::time::Instant::now() + Duration::from_secs(2);
		let contents = loop {
			let contents = std::fs::read_to_string(&path).unwrap_or_default();
			if contents.contains(r#""type":"moq_object_end""#) || std::time::Instant::now() >= deadline {
				break contents;
			}
			std::thread::sleep(Duration::from_millis(10));
		};
		assert!(contents.contains(r#""type":"moq_object_end""#));
	}
}
