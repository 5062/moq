use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use moq_trace::{
	Direction, Event, ObjectEndEvent, ObjectEvent, ObjectOutcome, ObjectPhase, PacketEndEvent, PacketEvent,
	PacketOutcome, PacketPhase, PhaseEdge, SocketOutcome, TRACE_REVISION, TraceClock,
};

use super::model::{Interval, LogicalObject, ObjectLifecycle, PacketLifecycle, PhaseInterval, StreamFrame, Trace};

#[derive(Default)]
struct ObjectBuilder {
	starts: BTreeMap<u64, ObjectEvent>,
	ends: BTreeMap<u64, ObjectEndEvent>,
	phase_starts: BTreeMap<(u64, ObjectPhase), Vec<u64>>,
	phases: BTreeMap<u64, Vec<(ObjectPhase, Interval)>>,
}

#[derive(Default)]
struct PacketBuilder {
	starts: BTreeMap<u64, PacketEvent>,
	ends: BTreeMap<u64, PacketEndEvent>,
	phase_starts: BTreeMap<(u64, PacketPhase), Vec<u64>>,
	phases: BTreeMap<u64, Vec<(PacketPhase, Interval)>>,
}

pub(super) fn read(path: &Path) -> Result<Trace> {
	let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
	let mut lines = BufReader::new(file).lines().enumerate();
	let Some((_, header)) = lines.next() else {
		bail!("trace is empty");
	};
	let header: Event = serde_json::from_str(&header?).context("invalid trace header")?;
	match header {
		Event::TraceHeader(header) if header.revision == TRACE_REVISION && header.clock == TraceClock::MonotonicNs => {}
		Event::TraceHeader(header) => bail!("unsupported trace revision or clock: {header:?}"),
		_ => bail!("the first trace record must be trace_header"),
	}

	let mut objects = ObjectBuilder::default();
	let mut packets = PacketBuilder::default();
	let mut frames = Vec::new();
	let mut sockets = BTreeSet::new();
	for (line, value) in lines {
		let value = value.with_context(|| format!("failed to read trace line {}", line + 1))?;
		let event: Event =
			serde_json::from_str(&value).with_context(|| format!("invalid trace event on line {}", line + 1))?;
		match event {
			Event::TraceHeader(_) => bail!("trace_header may only appear as the first record"),
			Event::MoqObjectStart(event) => insert_unique(&mut objects.starts, event.trace_id, event, "object start")?,
			Event::MoqObjectEnd(event) => insert_unique(&mut objects.ends, event.trace_id, event, "object end")?,
			Event::MoqObjectPhase(event) => {
				if event.edge == PhaseEdge::Done && event.outcome != Some(ObjectOutcome::Success) {
					bail!(
						"object trace {} phase {:?} did not succeed",
						event.trace_id,
						event.phase
					);
				}
				add_phase(
					&mut objects.phase_starts,
					&mut objects.phases,
					event.trace_id,
					event.phase,
					event.timestamp_ns,
					event.edge,
				)?;
			}
			Event::PacketStart(event) => insert_unique(&mut packets.starts, event.trace_id, event, "packet start")?,
			Event::PacketEnd(event) => insert_unique(&mut packets.ends, event.trace_id, event, "packet end")?,
			Event::PacketPhase(event) => {
				if event.edge == PhaseEdge::Done && event.outcome != Some(PacketOutcome::Success) {
					continue;
				}
				add_phase(
					&mut packets.phase_starts,
					&mut packets.phases,
					event.trace_id,
					event.phase,
					event.timestamp_ns,
					event.edge,
				)?;
			}
			Event::StreamFrame(event) if event.outcome == PacketOutcome::Success => frames.push(StreamFrame {
				trace_id: event.trace_id,
				timestamp_ns: event.timestamp_ns,
				stream_id: event.stream_id,
				offset_start: event.offset_start,
				offset_end: event.offset_end,
			}),
			Event::StreamFrame(_) => {}
			Event::SocketStart(event) => {
				if !sockets.insert(event.trace_id) {
					bail!("duplicate socket start for trace {}", event.trace_id);
				}
			}
			Event::SocketEnd(event) => {
				if !sockets.remove(&event.trace_id) {
					bail!("socket end {} has no start", event.trace_id);
				}
				if event.outcome == SocketOutcome::Abandoned {
					bail!("socket trace {} was abandoned", event.trace_id);
				}
			}
			_ => bail!("trace contains an event unsupported by revision {TRACE_REVISION}"),
		}
	}
	if !sockets.is_empty() {
		bail!("trace has {} incomplete socket operations", sockets.len());
	}
	finish(objects, packets, frames)
}

fn insert_unique<T>(map: &mut BTreeMap<u64, T>, trace_id: u64, value: T, label: &str) -> Result<()> {
	if map.insert(trace_id, value).is_some() {
		bail!("duplicate {label} for trace {trace_id}");
	}
	Ok(())
}

fn add_phase<P: Copy + Ord + std::fmt::Debug>(
	starts: &mut BTreeMap<(u64, P), Vec<u64>>,
	completed: &mut BTreeMap<u64, Vec<(P, Interval)>>,
	trace_id: u64,
	phase: P,
	timestamp: u64,
	edge: PhaseEdge,
) -> Result<()> {
	match edge {
		PhaseEdge::Start => starts.entry((trace_id, phase)).or_default().push(timestamp),
		PhaseEdge::Done => {
			let start = starts
				.get_mut(&(trace_id, phase))
				.and_then(Vec::pop)
				.ok_or_else(|| anyhow!("trace {trace_id} phase {phase:?} completes without a start"))?;
			if timestamp < start {
				bail!("trace {trace_id} phase {phase:?} completes before it starts");
			}
			completed
				.entry(trace_id)
				.or_default()
				.push((phase, Interval { start, end: timestamp }));
		}
	}
	Ok(())
}

fn finish(objects: ObjectBuilder, packets: PacketBuilder, frames: Vec<StreamFrame>) -> Result<Trace> {
	if objects.phase_starts.values().any(|starts| !starts.is_empty()) {
		bail!("trace contains incomplete object phases");
	}
	if packets.phase_starts.values().any(|starts| !starts.is_empty()) {
		bail!("trace contains incomplete packet phases");
	}
	let mut logical = BTreeMap::<moq_trace::LogicalId, (Option<ObjectLifecycle>, Vec<ObjectLifecycle>)>::new();
	for (trace_id, start) in objects.starts {
		let end = objects
			.ends
			.get(&trace_id)
			.cloned()
			.ok_or_else(|| anyhow!("object trace {trace_id} has no end"))?;
		if end.timestamp_ns < start.timestamp_ns {
			bail!("object trace {trace_id} completes before it starts");
		}
		validate_object_phases(
			start.direction,
			objects.phases.get(&trace_id).map(Vec::as_slice).unwrap_or_default(),
		)?;
		let lifecycle = ObjectLifecycle {
			phases: occurrences(objects.phases.get(&trace_id).cloned().unwrap_or_default()),
			start: start.clone(),
			end,
		};
		let entry = logical.entry(start.logical_id).or_default();
		match start.direction {
			Direction::Rx => {
				if entry.0.replace(lifecycle).is_some() {
					bail!("logical object {} has multiple ingress lifecycles", start.logical_id);
				}
			}
			Direction::Tx => entry.1.push(lifecycle),
		}
	}
	if objects.ends.len()
		!= logical
			.values()
			.map(|(rx, tx)| usize::from(rx.is_some()) + tx.len())
			.sum::<usize>()
	{
		bail!("trace contains object ends without starts");
	}
	let objects = logical
		.into_iter()
		.map(|(id, (rx, mut tx))| {
			tx.sort_by_key(|lifecycle| lifecycle.start.session_id);
			Ok((
				id,
				LogicalObject {
					rx: rx.ok_or_else(|| anyhow!("logical object {id} has no ingress lifecycle"))?,
					tx,
				},
			))
		})
		.collect::<Result<_>>()?;

	let mut complete_packets = BTreeMap::new();
	for (trace_id, start) in packets.starts {
		let end = packets
			.ends
			.get(&trace_id)
			.cloned()
			.ok_or_else(|| anyhow!("packet trace {trace_id} has no end"))?;
		if end.timestamp_ns < start.timestamp_ns {
			bail!("packet trace {trace_id} completes before it starts");
		}
		complete_packets.insert(
			trace_id,
			PacketLifecycle {
				start,
				end,
				phases: occurrences(packets.phases.get(&trace_id).cloned().unwrap_or_default()),
			},
		);
	}
	if packets.ends.len() != complete_packets.len() {
		bail!("trace contains packet ends without starts");
	}
	Ok(Trace {
		objects,
		packets: complete_packets,
		frames,
	})
}

fn occurrences<P: Copy + Ord>(mut values: Vec<(P, Interval)>) -> Vec<PhaseInterval<P>> {
	values.sort_by_key(|(phase, interval)| (*phase, interval.start));
	let mut counts = BTreeMap::new();
	values
		.into_iter()
		.map(|(phase, interval)| {
			let occurrence = counts.entry(phase).or_insert(0);
			let value = PhaseInterval {
				phase,
				occurrence: *occurrence,
				interval,
			};
			*occurrence += 1;
			value
		})
		.collect()
}

fn validate_object_phases(direction: Direction, phases: &[(ObjectPhase, Interval)]) -> Result<()> {
	for (phase, _) in phases {
		let valid = matches!(
			(direction, phase),
			(
				Direction::Rx,
				ObjectPhase::HeaderParse | ObjectPhase::Create | ObjectPhase::PayloadRead | ObjectPhase::FrameCommit
			) | (
				Direction::Tx,
				ObjectPhase::Clone | ObjectPhase::HeaderEncode | ObjectPhase::PayloadWrite
			)
		);
		if !valid {
			bail!("invalid {direction} object phase {phase:?}");
		}
	}
	Ok(())
}
