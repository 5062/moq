//! Trace validation, correlation, and artifact generation.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use moq_trace::{Direction, Event, ObjectEvent, ObjectOutcome, PacketEvent, PacketOutcome, PhaseEdge};
use serde::Serialize;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Options {
	pub object_size: u64,
	pub subscribers: usize,
	pub warmup_seconds: f64,
	pub cooldown_seconds: f64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum Dir {
	Rx,
	Tx,
}

impl From<Direction> for Dir {
	fn from(value: Direction) -> Self {
		match value {
			Direction::Rx => Self::Rx,
			Direction::Tx => Self::Tx,
		}
	}
}

impl std::fmt::Display for Dir {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str(match self {
			Self::Rx => "rx",
			Self::Tx => "tx",
		})
	}
}

#[derive(Clone, Copy, Debug)]
struct TimeRange {
	start: u64,
	end: u64,
}

fn time_range(start: u64, end: u64, label: &str) -> Result<TimeRange> {
	if end < start {
		bail!("{label} completes before it starts");
	}
	Ok(TimeRange { start, end })
}

#[derive(Default)]
struct PhaseScope {
	starts: Vec<u64>,
	dones: Vec<(u64, bool)>,
	packet_events: Vec<PacketEvent>,
}

impl PhaseScope {
	fn add(&mut self, timestamp: u64, edge: PhaseEdge, success: bool) {
		match edge {
			PhaseEdge::Start => self.starts.push(timestamp),
			PhaseEdge::Done => self.dones.push((timestamp, success)),
		}
	}

	fn add_packet(&mut self, event: PacketEvent, edge: PhaseEdge, success: bool) {
		self.add(event.timestamp_ns, edge, success);
		self.packet_events.push(event);
	}

	fn finish(mut self, label: &str) -> Result<Vec<TimeRange>> {
		self.starts.sort_unstable();
		self.dones.sort_unstable_by_key(|item| item.0);
		if self.starts.len() != self.dones.len() {
			bail!(
				"{label} has {} starts and {} completions",
				self.starts.len(),
				self.dones.len()
			);
		}
		self.starts
			.into_iter()
			.zip(self.dones)
			.map(|(start, (end, success))| {
				if !success {
					bail!("{label} did not complete successfully");
				}
				time_range(start, end, label)
			})
			.collect()
	}
}

#[derive(Clone, Copy, Debug)]
struct Packet {
	trace_id: u64,
	connection_id: u64,
	direction: Dir,
	start: u64,
	end: u64,
}

#[derive(Clone, Debug)]
struct PacketPhase {
	phase: String,
	occurrence: usize,
	range: TimeRange,
}

#[derive(Clone, Copy, Debug)]
struct StreamFrame {
	packet: Packet,
	stream_id: u64,
	start: u64,
	end: u64,
}

#[derive(Default)]
struct PacketScope {
	starts: Vec<PacketEvent>,
	ends: Vec<(PacketEvent, PacketOutcome)>,
}

#[derive(Serialize)]
struct PacketSample {
	metric: String,
	direction: Dir,
	connection_id: u64,
	trace_id: u64,
	occurrence: usize,
	elapsed_ms: f64,
	latency_us: f64,
}

struct PacketIndex {
	by_id: BTreeMap<u64, Packet>,
	frames: Vec<StreamFrame>,
	phases: BTreeMap<u64, Vec<PacketPhase>>,
	samples: Vec<PacketSample>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ObjectKey {
	group_id: u64,
	object_id: u64,
}

impl std::fmt::Display for ObjectKey {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(formatter, "({}, {})", self.group_id, self.object_id)
	}
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ObjectRange {
	direction: Dir,
	session_id: u64,
	connection_id: u64,
	stream_id: u64,
	start: u64,
	end: u64,
}

#[derive(Clone, Debug)]
struct PhaseInterval {
	phase: String,
	occurrence: usize,
	range: TimeRange,
}

#[derive(Clone, Debug)]
struct ObjectSession {
	direction: Dir,
	session_id: u64,
	lifecycles: Vec<TimeRange>,
	phases: Vec<PhaseInterval>,
	ranges: Vec<ObjectRange>,
}

#[derive(Clone, Debug)]
struct Boundaries {
	rx_starts: Vec<u64>,
	rx_ends: Vec<u64>,
	tx_starts: Vec<u64>,
	tx_ends: Vec<u64>,
}

#[derive(Clone, Debug)]
struct IndexedObject {
	sessions: Vec<ObjectSession>,
	boundaries: Boundaries,
	rx_payload_bytes: BTreeSet<u64>,
}

#[derive(Default)]
struct ObjectScope {
	starts: Vec<u64>,
	ends: Vec<ObjectEvent>,
	phases: BTreeMap<String, PhaseScope>,
}

#[derive(Default)]
struct ObjectBuilder {
	scopes: BTreeMap<(Dir, u64), ObjectScope>,
	boundaries: Option<Boundaries>,
	rx_payload_bytes: BTreeSet<u64>,
}

impl ObjectBuilder {
	fn add_lifecycle(&mut self, event: ObjectEvent, start: bool) -> Result<()> {
		let direction = Dir::from(event.direction);
		let session = event
			.session_id
			.ok_or_else(|| anyhow!("object event is missing session_id"))?;
		let scope = self.scopes.entry((direction, session)).or_default();
		let boundaries = self.boundaries.get_or_insert_with(|| Boundaries {
			rx_starts: Vec::new(),
			rx_ends: Vec::new(),
			tx_starts: Vec::new(),
			tx_ends: Vec::new(),
		});
		if start {
			scope.starts.push(event.timestamp_ns);
			match direction {
				Dir::Rx => boundaries.rx_starts.push(event.timestamp_ns),
				Dir::Tx => boundaries.tx_starts.push(event.timestamp_ns),
			}
		} else {
			match direction {
				Dir::Rx => {
					boundaries.rx_ends.push(event.timestamp_ns);
					self.rx_payload_bytes.insert(event.payload_bytes);
				}
				Dir::Tx => boundaries.tx_ends.push(event.timestamp_ns),
			}
			scope.ends.push(event);
		}
		Ok(())
	}

	fn add_phase(&mut self, event: moq_trace::ObjectPhaseEvent) -> Result<()> {
		let direction = Dir::from(event.object.direction);
		let session = event
			.object
			.session_id
			.ok_or_else(|| anyhow!("object phase is missing session_id"))?;
		let phase = enum_name(&event.phase)?;
		validate_object_phase(direction, &phase)?;
		self.scopes
			.entry((direction, session))
			.or_default()
			.phases
			.entry(phase)
			.or_default()
			.add(
				event.object.timestamp_ns,
				event.edge,
				event.outcome == Some(ObjectOutcome::Success),
			);
		Ok(())
	}

	fn finish(self, key: ObjectKey) -> Result<IndexedObject> {
		let mut sessions = Vec::new();
		for ((direction, session_id), mut scope) in self.scopes {
			scope.starts.sort_unstable();
			scope.ends.sort_unstable_by_key(|event| event.timestamp_ns);
			let label = format!("{direction} session {session_id} object");
			if scope.starts.len() != scope.ends.len() {
				bail!(
					"{label} has {} starts and {} completions",
					scope.starts.len(),
					scope.ends.len()
				);
			}
			let lifecycles = scope
				.starts
				.iter()
				.zip(&scope.ends)
				.map(|(&start, end)| time_range(start, end.timestamp_ns, &label))
				.collect::<Result<_>>()?;
			let mut phases = Vec::new();
			for (phase, phase_scope) in scope.phases {
				for (occurrence, range) in phase_scope.finish(&format!("{label} {phase}"))?.into_iter().enumerate() {
					phases.push(PhaseInterval {
						phase: phase.clone(),
						occurrence,
						range,
					});
				}
			}
			let ranges = scope.ends.iter().map(object_range).collect::<Result<_>>()?;
			sessions.push(ObjectSession {
				direction,
				session_id,
				lifecycles,
				phases,
				ranges,
			});
		}
		let mut boundaries = self
			.boundaries
			.ok_or_else(|| anyhow!("object {key} has no lifecycle events"))?;
		boundaries.rx_starts.sort_unstable();
		boundaries.rx_ends.sort_unstable();
		boundaries.tx_starts.sort_unstable();
		boundaries.tx_ends.sort_unstable();
		Ok(IndexedObject {
			sessions,
			boundaries,
			rx_payload_bytes: self.rx_payload_bytes,
		})
	}
}

fn object_range(event: &ObjectEvent) -> Result<ObjectRange> {
	Ok(ObjectRange {
		direction: event.direction.into(),
		session_id: event
			.session_id
			.ok_or_else(|| anyhow!("completed object is missing session_id"))?,
		connection_id: event
			.connection_id
			.ok_or_else(|| anyhow!("completed object is missing connection_id"))?,
		stream_id: event
			.stream_id
			.ok_or_else(|| anyhow!("completed object is missing stream_id"))?,
		start: event
			.stream_offset_start
			.ok_or_else(|| anyhow!("completed object is missing stream_offset_start"))?,
		end: event
			.stream_offset_end
			.ok_or_else(|| anyhow!("completed object is missing stream_offset_end"))?,
	})
}

fn enum_name(value: &impl Serialize) -> Result<String> {
	serde_json::to_value(value)?
		.as_str()
		.map(ToOwned::to_owned)
		.ok_or_else(|| anyhow!("trace enum did not serialize as text"))
}

fn validate_object_phase(direction: Dir, phase: &str) -> Result<()> {
	let valid = match direction {
		Dir::Rx => ["header_parse", "create", "payload_read", "frame_commit"].contains(&phase),
		Dir::Tx => ["clone", "header_encode", "payload_write"].contains(&phase),
	};
	if !valid {
		bail!("object has invalid {direction} phase {phase}");
	}
	Ok(())
}

struct TraceIndex {
	packets: PacketIndex,
	objects: BTreeMap<ObjectKey, IndexedObject>,
	coverage: CoverageIndex,
}

fn read_trace(path: &Path) -> Result<TraceIndex> {
	let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
	let mut packet_scopes: BTreeMap<u64, PacketScope> = BTreeMap::new();
	let mut phase_scopes: BTreeMap<(u64, String), PhaseScope> = BTreeMap::new();
	let mut stream_events = Vec::new();
	let mut objects: BTreeMap<ObjectKey, ObjectBuilder> = BTreeMap::new();
	for (line_number, line) in BufReader::new(file).lines().enumerate() {
		let line = line.with_context(|| format!("failed to read trace line {}", line_number + 1))?;
		let event: Event =
			serde_json::from_str(&line).with_context(|| format!("invalid trace line {}", line_number + 1))?;
		match event {
			Event::PacketStart(event) => packet_scopes.entry(event.trace_id).or_default().starts.push(event),
			Event::PacketEnd(event) => packet_scopes
				.entry(event.packet.trace_id)
				.or_default()
				.ends
				.push((event.packet, event.outcome)),
			Event::PacketPhase(event) => {
				let phase = enum_name(&event.phase)?;
				phase_scopes
					.entry((event.packet.trace_id, phase))
					.or_default()
					.add_packet(event.packet, event.edge, event.outcome == Some(PacketOutcome::Success));
			}
			Event::StreamFrame(event) => stream_events.push(event),
			Event::MoqObjectStart(event) => {
				let key = ObjectKey {
					group_id: event.group_id,
					object_id: event.object_id,
				};
				objects.entry(key).or_default().add_lifecycle(event, true)?;
			}
			Event::MoqObjectEnd(event) => {
				let key = ObjectKey {
					group_id: event.group_id,
					object_id: event.object_id,
				};
				objects.entry(key).or_default().add_lifecycle(event, false)?;
			}
			Event::MoqObjectPhase(event) => {
				let key = ObjectKey {
					group_id: event.object.group_id,
					object_id: event.object.object_id,
				};
				objects.entry(key).or_default().add_phase(event)?;
			}
			Event::SocketStart(_) | Event::SocketEnd(_) => {}
			_ => {}
		}
	}
	let packets = finish_packets(packet_scopes, phase_scopes, stream_events)?;
	let objects = objects
		.into_iter()
		.map(|(key, builder)| Ok((key, builder.finish(key)?)))
		.collect::<Result<_>>()?;
	let coverage = CoverageIndex::new(&packets);
	Ok(TraceIndex {
		packets,
		objects,
		coverage,
	})
}

fn validate_packet_event(event: &PacketEvent, packet: Packet, label: &str) -> Result<()> {
	if event.sample_rate != 1 {
		bail!("QUIC object correlation requires packet_sample = 1");
	}
	if Dir::from(event.direction) != packet.direction || event.connection_id != packet.connection_id {
		bail!("{label} changes packet identity");
	}
	Ok(())
}

fn packet_phase_valid(direction: Dir, phase: &str) -> bool {
	match direction {
		Dir::Rx => [
			"header_parse",
			"routing",
			"scheduling",
			"header_unprotect",
			"payload_decrypt",
			"frame_process",
		]
		.contains(&phase),
		Dir::Tx => ["frame_encode", "packet_encrypt"].contains(&phase),
	}
}

fn finish_packets(
	packet_scopes: BTreeMap<u64, PacketScope>,
	mut phase_scopes: BTreeMap<(u64, String), PhaseScope>,
	stream_events: Vec<moq_trace::StreamFrameEvent>,
) -> Result<PacketIndex> {
	let mut by_id = BTreeMap::new();
	let mut unsuccessful = BTreeSet::new();
	for (trace_id, scope) in packet_scopes {
		if scope.starts.len() != 1 || scope.ends.len() != 1 {
			bail!(
				"packet {trace_id} has {} starts and {} completions",
				scope.starts.len(),
				scope.ends.len()
			);
		}
		let start = &scope.starts[0];
		let (end, outcome) = &scope.ends[0];
		if start.sample_rate != 1 || end.sample_rate != 1 {
			bail!("QUIC object correlation requires packet_sample = 1");
		}
		if start.direction != end.direction || start.connection_id != end.connection_id {
			bail!("packet {trace_id} changes identity");
		}
		let range = time_range(start.timestamp_ns, end.timestamp_ns, &format!("packet {trace_id}"))?;
		if *outcome != PacketOutcome::Success {
			unsuccessful.insert(trace_id);
			continue;
		}
		by_id.insert(
			trace_id,
			Packet {
				trace_id,
				connection_id: start.connection_id,
				direction: start.direction.into(),
				start: range.start,
				end: range.end,
			},
		);
	}
	let mut frames = Vec::new();
	for event in stream_events {
		if event.outcome != PacketOutcome::Success {
			continue;
		}
		let trace_id = event.packet.trace_id;
		let Some(&packet) = by_id.get(&trace_id) else {
			if unsuccessful.contains(&trace_id) {
				bail!("successful STREAM frame references unsuccessful packet {trace_id}");
			}
			bail!("STREAM frame references unknown packet {trace_id}");
		};
		validate_packet_event(&event.packet, packet, &format!("packet {trace_id} STREAM frame"))?;
		if event.offset_end < event.offset_start {
			bail!("packet {trace_id} STREAM frame has a negative byte range");
		}
		if event.offset_end > event.offset_start {
			frames.push(StreamFrame {
				packet,
				stream_id: event.stream_id,
				start: event.offset_start,
				end: event.offset_end,
			});
		}
	}
	let first_packet = by_id.values().map(|packet| packet.start).min().unwrap_or(0);
	let mut phases = BTreeMap::new();
	let mut samples = Vec::new();
	for packet in by_id.values().copied() {
		samples.push(PacketSample {
			metric: format!("{}_packet_span", packet.direction),
			direction: packet.direction,
			connection_id: packet.connection_id,
			trace_id: packet.trace_id,
			occurrence: 0,
			elapsed_ms: ms(packet.start - first_packet),
			latency_us: us(packet.end - packet.start),
		});
		let keys = phase_scopes
			.keys()
			.filter(|(trace_id, _)| *trace_id == packet.trace_id)
			.cloned()
			.collect::<Vec<_>>();
		let mut processing_start = None;
		for key in keys {
			let phase = key.1.clone();
			if !packet_phase_valid(packet.direction, &phase) {
				bail!(
					"packet {} has invalid {} phase {phase}",
					packet.trace_id,
					packet.direction
				);
			}
			let scope = phase_scopes.remove(&key).unwrap();
			for event in &scope.packet_events {
				validate_packet_event(event, packet, &format!("packet {} {phase}", packet.trace_id))?;
			}
			let ranges = scope.finish(&format!("packet {} {phase}", packet.trace_id))?;
			if packet.direction == Dir::Rx && phase == "scheduling" {
				if ranges.len() != 1 {
					bail!("packet {} scheduling has {} occurrences", packet.trace_id, ranges.len());
				}
				processing_start = Some(ranges[0].end);
			}
			for (occurrence, range) in ranges.into_iter().enumerate() {
				phases
					.entry(packet.trace_id)
					.or_insert_with(Vec::new)
					.push(PacketPhase {
						phase: phase.clone(),
						occurrence,
						range,
					});
				samples.push(PacketSample {
					metric: format!("{}_{phase}", packet.direction),
					direction: packet.direction,
					connection_id: packet.connection_id,
					trace_id: packet.trace_id,
					occurrence,
					elapsed_ms: ms(range.start - first_packet),
					latency_us: us(range.end - range.start),
				});
			}
		}
		if let Some(start) = processing_start {
			let range = time_range(start, packet.end, &format!("packet {} RX processing", packet.trace_id))?;
			samples.push(PacketSample {
				metric: "rx_packet_processing_span".into(),
				direction: packet.direction,
				connection_id: packet.connection_id,
				trace_id: packet.trace_id,
				occurrence: 0,
				elapsed_ms: ms(range.start - first_packet),
				latency_us: us(range.end - range.start),
			});
		}
	}
	for ((trace_id, phase), _) in phase_scopes {
		if !unsuccessful.contains(&trace_id) {
			bail!("phase {phase} references unknown packet {trace_id}");
		}
	}
	Ok(PacketIndex {
		by_id,
		frames,
		phases,
		samples,
	})
}

fn ms(ns: u64) -> f64 {
	ns as f64 / 1_000_000.0
}
fn us(ns: u64) -> f64 {
	ns as f64 / 1_000.0
}

#[derive(Clone, Debug)]
struct Coverage {
	first_start: u64,
	first_end: u64,
	complete_end: u64,
	packet_ids: Vec<u64>,
}

struct CoverageIndex {
	frames: HashMap<(Dir, u64, u64), Vec<StreamFrame>>,
	cache: HashMap<ObjectRange, Coverage>,
}

impl CoverageIndex {
	fn new(packets: &PacketIndex) -> Self {
		let mut frames: HashMap<_, Vec<_>> = HashMap::new();
		for &frame in &packets.frames {
			frames
				.entry((frame.packet.direction, frame.packet.connection_id, frame.stream_id))
				.or_default()
				.push(frame);
		}
		for values in frames.values_mut() {
			values.sort_unstable_by_key(|frame| (frame.packet.end, frame.packet.trace_id, frame.start, frame.end));
		}
		Self {
			frames,
			cache: HashMap::new(),
		}
	}

	fn first_complete(&mut self, object: ObjectRange) -> Result<Coverage> {
		if let Some(coverage) = self.cache.get(&object) {
			return Ok(coverage.clone());
		}
		if object.end <= object.start {
			bail!("{} object has an empty or negative byte range", object.direction);
		}
		let mut candidates = self
			.frames
			.get(&(object.direction, object.connection_id, object.stream_id))
			.into_iter()
			.flatten()
			.filter_map(|frame| {
				let start = object.start.max(frame.start);
				let end = object.end.min(frame.end);
				(start < end).then_some((frame.packet.end, start, end, *frame))
			})
			.collect::<Vec<_>>();
		candidates.sort_unstable_by_key(|item| (item.0, item.3.packet.trace_id, item.1, item.2));
		let mut intervals: Vec<(u64, u64)> = Vec::new();
		let mut selected = BTreeMap::new();
		let mut index = 0;
		while index < candidates.len() {
			let completion = candidates[index].0;
			while index < candidates.len() && candidates[index].0 == completion {
				let (_, start, end, frame) = candidates[index];
				merge_interval(&mut intervals, (start, end));
				selected.insert(frame.packet.trace_id, frame.packet);
				index += 1;
			}
			if intervals == [(object.start, object.end)] {
				let mut packets = selected.values().copied().collect::<Vec<_>>();
				packets.sort_unstable_by_key(|packet| (packet.end, packet.trace_id));
				let coverage = Coverage {
					first_start: packets.iter().map(|packet| packet.start).min().unwrap(),
					first_end: packets.iter().map(|packet| packet.end).min().unwrap(),
					complete_end: packets.iter().map(|packet| packet.end).max().unwrap(),
					packet_ids: packets.iter().map(|packet| packet.trace_id).collect(),
				};
				self.cache.insert(object, coverage.clone());
				return Ok(coverage);
			}
		}
		bail!(
			"incomplete packet coverage for {} connection {} stream {} range [{}, {})",
			object.direction,
			object.connection_id,
			object.stream_id,
			object.start,
			object.end
		)
	}
}

fn merge_interval(intervals: &mut Vec<(u64, u64)>, new: (u64, u64)) {
	intervals.push(new);
	intervals.sort_unstable();
	let mut merged: Vec<(u64, u64)> = Vec::new();
	for (start, end) in intervals.drain(..) {
		if let Some(last) = merged.last_mut()
			&& start <= last.1
		{
			last.1 = last.1.max(end);
		} else {
			merged.push((start, end));
		}
	}
	*intervals = merged;
}

#[derive(Clone, Debug, Serialize)]
struct Sample {
	group_id: u64,
	object_id: u64,
	metric: String,
	copy_ordinal: usize,
	elapsed_ms: f64,
	latency_us: f64,
}

#[derive(Clone, Debug, Serialize)]
struct TimelineSelection {
	statistic: String,
	target_us: f64,
	group_id: u64,
	object_id: u64,
	actual_us: f64,
}
#[derive(Clone, Debug, Serialize)]
struct TimelineInterval {
	direction: Dir,
	session_id: u64,
	phase: String,
	occurrence: usize,
	start_us: f64,
	end_us: f64,
}
#[derive(Clone, Copy, Debug, Serialize)]
struct TimelineCopy {
	session_id: u64,
	subscriber_ordinal: usize,
	full_span_us: f64,
}
#[derive(Clone, Debug, Serialize)]
struct ObjectTimeline {
	selection: TimelineSelection,
	intervals: Vec<TimelineInterval>,
	first_copy: TimelineCopy,
	last_copy: TimelineCopy,
	slowest_copy: TimelineCopy,
}

#[derive(Clone, Debug, Serialize)]
struct Statistics {
	count: usize,
	mean: f64,
	p50: f64,
	p95: f64,
	p99: f64,
	max: f64,
}

#[derive(Serialize)]
struct Artifact {
	statistics: BTreeMap<String, Statistics>,
	quic_object_statistics: BTreeMap<String, Statistics>,
	packet_statistics: BTreeMap<String, Statistics>,
	packet_count: usize,
	group_count: usize,
	timelines: Vec<ObjectTimeline>,
}

pub(crate) fn run(input: &Path, output: &Path, options: Options) -> Result<()> {
	if options.subscribers == 0
		|| options.object_size == 0
		|| options.warmup_seconds < 0.0
		|| options.cooldown_seconds < 0.0
	{
		bail!("analysis options must be positive");
	}
	let mut trace = read_trace(input)?;
	let (keys, first_rx, group_count) = steady_state(&trace, options)?;
	let samples = object_samples(&trace, &keys, first_rx);
	let quic_samples = quic_samples(&mut trace, &keys, first_rx, options.subscribers)?;
	let timelines = build_timelines(&mut trace, &keys, &samples)?;
	let required = ["rx_routing", "rx_scheduling"];
	let packet_statistics = summarize_packets(&trace.packets.samples)?;
	for metric in required {
		if !packet_statistics.contains_key(metric) {
			bail!("Quinn trace is missing packet metric: {metric}");
		}
	}
	let artifact = Artifact {
		statistics: summarize(&samples)?,
		quic_object_statistics: summarize(&quic_samples)?,
		packet_statistics,
		packet_count: trace.packets.by_id.len(),
		group_count,
		timelines,
	};
	std::fs::create_dir_all(output)?;
	write_csv(&output.join("objects.csv"), &samples)?;
	write_csv(&output.join("quic_objects.csv"), &quic_samples)?;
	write_csv(&output.join("quic_packets.csv"), &trace.packets.samples)?;
	serde_json::to_writer_pretty(BufWriter::new(File::create(output.join("analysis.json"))?), &artifact)?;
	Ok(())
}

fn write_csv(path: &Path, rows: &[impl Serialize]) -> Result<()> {
	let mut writer = csv::Writer::from_path(path)?;
	for row in rows {
		writer.serialize(row)?;
	}
	writer.flush()?;
	Ok(())
}

fn steady_state(trace: &TraceIndex, options: Options) -> Result<(Vec<ObjectKey>, u64, usize)> {
	let matching = trace
		.objects
		.iter()
		.filter(|(_, object)| object.rx_payload_bytes.contains(&options.object_size))
		.collect::<Vec<_>>();
	if matching.is_empty() {
		bail!("trace has no completed {}-byte inbound objects", options.object_size);
	}
	for (key, object) in &matching {
		if object.boundaries.rx_starts.len() != 1 {
			bail!("{key} has {} rx_starts", object.boundaries.rx_starts.len());
		}
	}
	let first = matching.iter().map(|(_, o)| o.boundaries.rx_starts[0]).min().unwrap();
	let last = matching.iter().map(|(_, o)| o.boundaries.rx_starts[0]).max().unwrap();
	let start = first.saturating_add((options.warmup_seconds * 1e9) as u64);
	let end = last.saturating_sub((options.cooldown_seconds * 1e9) as u64);
	let keys = matching
		.into_iter()
		.filter(|(_, o)| (start..=end).contains(&o.boundaries.rx_starts[0]))
		.map(|(k, _)| *k)
		.collect::<Vec<_>>();
	if keys.is_empty() {
		bail!("steady-state window contains no complete objects");
	}
	for key in &keys {
		let b = &trace.objects[key].boundaries;
		if b.rx_ends.len() != 1 {
			bail!("{key} has {} rx_ends", b.rx_ends.len());
		}
		if b.tx_starts.len() != options.subscribers || b.tx_ends.len() != options.subscribers {
			bail!("{key} does not have {} complete outbound copies", options.subscribers);
		}
	}
	let groups = keys.iter().map(|k| k.group_id).collect::<BTreeSet<_>>();
	let min = *groups.first().unwrap();
	let max = *groups.last().unwrap();
	if groups.len() as u64 != max - min + 1 {
		bail!("steady-state groups are not contiguous");
	}
	Ok((keys, first, groups.len()))
}

fn object_samples(trace: &TraceIndex, keys: &[ObjectKey], first_rx: u64) -> Vec<Sample> {
	let mut rows = Vec::new();
	for key in keys {
		let b = &trace.objects[key].boundaries;
		let rx = b.rx_starts[0];
		for (copy, &end) in b.tx_ends.iter().enumerate() {
			rows.push(Sample {
				group_id: key.group_id,
				object_id: key.object_id,
				metric: "full_span".into(),
				copy_ordinal: copy,
				elapsed_ms: ms(rx - first_rx),
				latency_us: us(end - rx),
			});
		}
	}
	rows
}

fn quic_samples(trace: &mut TraceIndex, keys: &[ObjectKey], first_rx: u64, subscribers: usize) -> Result<Vec<Sample>> {
	let mut rows = Vec::new();
	for key in keys {
		let ranges = trace.objects[key]
			.sessions
			.iter()
			.flat_map(|s| s.ranges.iter().copied())
			.collect::<Vec<_>>();
		let rx = ranges
			.iter()
			.filter(|r| r.direction == Dir::Rx)
			.copied()
			.collect::<Vec<_>>();
		let mut tx = ranges
			.iter()
			.filter(|r| r.direction == Dir::Tx)
			.copied()
			.collect::<Vec<_>>();
		tx.sort_unstable_by_key(|r| r.session_id);
		if rx.len() != 1 || tx.len() != subscribers {
			bail!("{key} has invalid completed transport range counts");
		}
		let inbound = trace.coverage.first_complete(rx[0])?;
		for (copy, range) in tx.into_iter().enumerate() {
			let outbound = trace.coverage.first_complete(range)?;
			for (metric, end, start) in [
				("quic_forward_start", outbound.first_end, inbound.first_start),
				("quic_tail_gap", outbound.complete_end, inbound.complete_end),
				("quic_full_span", outbound.complete_end, inbound.first_start),
			] {
				if end < start {
					bail!("{metric} is negative for object {key} copy {copy}");
				}
				rows.push(Sample {
					group_id: key.group_id,
					object_id: key.object_id,
					metric: metric.into(),
					copy_ordinal: copy,
					elapsed_ms: ms(inbound.first_start - first_rx),
					latency_us: us(end - start),
				});
			}
		}
	}
	Ok(rows)
}

fn summarize(rows: &[Sample]) -> Result<BTreeMap<String, Statistics>> {
	summarize_values(rows.iter().map(|r| (&r.metric, r.latency_us)))
}
fn summarize_packets(rows: &[PacketSample]) -> Result<BTreeMap<String, Statistics>> {
	summarize_values(rows.iter().map(|r| (&r.metric, r.latency_us)))
}
fn summarize_values<'a>(rows: impl Iterator<Item = (&'a String, f64)>) -> Result<BTreeMap<String, Statistics>> {
	let mut grouped: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	for (metric, value) in rows {
		grouped.entry(metric.clone()).or_default().push(value);
	}
	if grouped.is_empty() {
		bail!("cannot summarize empty samples");
	}
	Ok(grouped
		.into_iter()
		.map(|(metric, mut values)| {
			values.sort_by(f64::total_cmp);
			let count = values.len();
			let mean = values.iter().sum::<f64>() / count as f64;
			let stats = Statistics {
				count,
				mean,
				p50: quantile(&values, 0.5),
				p95: quantile(&values, 0.95),
				p99: quantile(&values, 0.99),
				max: *values.last().unwrap(),
			};
			(metric, stats)
		})
		.collect())
}
fn quantile(values: &[f64], q: f64) -> f64 {
	let pos = (values.len() - 1) as f64 * q;
	let low = pos.floor() as usize;
	let high = pos.ceil() as usize;
	values[low] + (values[high] - values[low]) * (pos - low as f64)
}

fn build_timelines(trace: &mut TraceIndex, keys: &[ObjectKey], samples: &[Sample]) -> Result<Vec<ObjectTimeline>> {
	let mut slowest = BTreeMap::new();
	for row in samples {
		slowest
			.entry(ObjectKey {
				group_id: row.group_id,
				object_id: row.object_id,
			})
			.and_modify(|v: &mut f64| *v = v.max(row.latency_us))
			.or_insert(row.latency_us);
	}
	let values = slowest.values().copied().collect::<Vec<_>>();
	let mean = values.iter().sum::<f64>() / values.len() as f64;
	let mut sorted = values.clone();
	sorted.sort_by(f64::total_cmp);
	let targets = [
		("mean", mean),
		("median", quantile(&sorted, 0.5)),
		("p99", quantile(&sorted, 0.99)),
	];
	let mut timelines = Vec::new();
	for (statistic, target) in targets {
		let (&key, &actual) = slowest
			.iter()
			.min_by(|a, b| (a.1 - target).abs().total_cmp(&(b.1 - target).abs()).then(a.0.cmp(b.0)))
			.unwrap();
		timelines.push(extract_timeline(
			trace,
			key,
			TimelineSelection {
				statistic: statistic.into(),
				target_us: target,
				group_id: key.group_id,
				object_id: key.object_id,
				actual_us: actual,
			},
		)?);
	}
	let _ = keys;
	Ok(timelines)
}

fn extract_timeline(trace: &mut TraceIndex, key: ObjectKey, selection: TimelineSelection) -> Result<ObjectTimeline> {
	#[derive(Clone)]
	struct Raw {
		direction: Dir,
		session: u64,
		phase: String,
		occurrence: usize,
		start: u64,
		end: u64,
	}
	let object = trace.objects[&key].clone();
	let mut raw = Vec::new();
	for session in &object.sessions {
		for (i, r) in session.lifecycles.iter().enumerate() {
			raw.push(Raw {
				direction: session.direction,
				session: session.session_id,
				phase: "object".into(),
				occurrence: i,
				start: r.start,
				end: r.end,
			});
		}
		for p in &session.phases {
			raw.push(Raw {
				direction: session.direction,
				session: session.session_id,
				phase: p.phase.clone(),
				occurrence: p.occurrence,
				start: p.range.start,
				end: p.range.end,
			});
		}
		for range in &session.ranges {
			let coverage = trace.coverage.first_complete(*range)?;
			for (i, id) in coverage.packet_ids.iter().enumerate() {
				let p = trace.packets.by_id[id];
				raw.push(Raw {
					direction: range.direction,
					session: range.session_id,
					phase: "quic_packet".into(),
					occurrence: i,
					start: p.start,
					end: p.end,
				});
				for phase in trace.packets.phases.get(id).into_iter().flatten() {
					raw.push(Raw {
						direction: range.direction,
						session: range.session_id,
						phase: format!("quic_{}", phase.phase),
						occurrence: phase.occurrence,
						start: phase.range.start,
						end: phase.range.end,
					});
				}
			}
		}
	}
	let rx_object = raw
		.iter()
		.filter(|r| r.direction == Dir::Rx && r.phase == "object")
		.collect::<Vec<_>>();
	if rx_object.len() != 1 {
		bail!("selected object {key} has {} completed RX lifecycles", rx_object.len());
	}
	let rx_start = rx_object[0].start;
	let origin = raw
		.iter()
		.filter(|r| r.direction == Dir::Rx && r.phase == "quic_packet")
		.map(|r| r.start)
		.min()
		.ok_or_else(|| anyhow!("selected object {key} has no covering RX packets"))?;
	let mut intervals = raw
		.iter()
		.map(|r| TimelineInterval {
			direction: r.direction,
			session_id: r.session,
			phase: r.phase.clone(),
			occurrence: r.occurrence,
			start_us: (r.start as f64 - origin as f64) / 1000.0,
			end_us: (r.end as f64 - origin as f64) / 1000.0,
		})
		.collect::<Vec<_>>();
	intervals.sort_by_key(|i| (i.direction, i.session_id, i.occurrence));
	let mut copies = raw
		.iter()
		.filter(|r| r.direction == Dir::Tx && r.phase == "object")
		.collect::<Vec<_>>();
	copies.sort_by_key(|r| r.session);
	let copies = copies
		.into_iter()
		.enumerate()
		.map(|(i, r)| TimelineCopy {
			session_id: r.session,
			subscriber_ordinal: i + 1,
			full_span_us: us(r.end - rx_start),
		})
		.collect::<Vec<_>>();
	if copies.is_empty() {
		bail!("selected object {key} has no completed TX lifecycles");
	}
	let slowest = *copies
		.iter()
		.max_by(|a, b| {
			a.full_span_us
				.total_cmp(&b.full_span_us)
				.then_with(|| b.session_id.cmp(&a.session_id))
		})
		.unwrap();
	Ok(ObjectTimeline {
		selection,
		intervals,
		first_copy: copies[0],
		last_copy: *copies.last().unwrap(),
		slowest_copy: slowest,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use moq_trace::{PacketPhase as TracePacketPhase, StreamFrameEvent};

	fn event(trace_id: u64, timestamp_ns: u64) -> PacketEvent {
		PacketEvent {
			timestamp_ns,
			trace_id,
			connection_id: 7,
			direction: Direction::Rx,
			packet_number: None,
			packet_space: None,
			byte_len: None,
			sample_rate: 1,
		}
	}

	#[test]
	fn dropped_packets_are_excluded() {
		let scopes = BTreeMap::from([
			(
				1,
				PacketScope {
					starts: vec![event(1, 10)],
					ends: vec![(event(1, 13), PacketOutcome::Dropped)],
				},
			),
			(
				2,
				PacketScope {
					starts: vec![event(2, 20)],
					ends: vec![(event(2, 25), PacketOutcome::Success)],
				},
			),
		]);
		let index = finish_packets(scopes, BTreeMap::new(), Vec::new()).unwrap();
		assert_eq!(index.by_id.keys().copied().collect::<Vec<_>>(), [2]);
	}

	#[test]
	fn successful_stream_frame_cannot_reference_dropped_packet() {
		let scopes = BTreeMap::from([(
			1,
			PacketScope {
				starts: vec![event(1, 10)],
				ends: vec![(event(1, 13), PacketOutcome::Dropped)],
			},
		)]);
		let frames = vec![StreamFrameEvent {
			packet: event(1, 12),
			stream_id: 4,
			offset_start: 0,
			offset_end: 10,
			outcome: PacketOutcome::Success,
		}];
		let error = match finish_packets(scopes, BTreeMap::new(), frames) {
			Ok(_) => panic!("successful frame unexpectedly referenced a dropped packet"),
			Err(error) => error,
		};
		assert!(
			error
				.to_string()
				.contains("successful STREAM frame references unsuccessful packet 1")
		);
	}

	#[test]
	fn rx_processing_starts_when_scheduling_finishes() {
		let scopes = BTreeMap::from([(
			1,
			PacketScope {
				starts: vec![event(1, 1_000)],
				ends: vec![(event(1, 12_000), PacketOutcome::Success)],
			},
		)]);
		let mut phase = PhaseScope::default();
		phase.add_packet(event(1, 3_000), PhaseEdge::Start, false);
		phase.add_packet(event(1, 7_000), PhaseEdge::Done, true);
		let phases = BTreeMap::from([((1, enum_name(&TracePacketPhase::Scheduling).unwrap()), phase)]);
		let index = finish_packets(scopes, phases, Vec::new()).unwrap();
		let sample = index
			.samples
			.iter()
			.find(|sample| sample.metric == "rx_packet_processing_span")
			.unwrap();
		assert_eq!(sample.latency_us, 5.0);
	}
}
