use std::ops::Range;

use anyhow::{Result, anyhow, bail};
use moq_trace::PacketOutcome;
use rangemap::RangeSet;

use super::model::{ObjectLifecycle, Trace};

#[derive(Clone, Debug)]
pub(super) struct Coverage {
	pub first_start: u64,
	pub first_end: u64,
	pub complete_end: u64,
	pub packet_ids: Vec<u64>,
}

pub(super) fn resolve(trace: &Trace, object: &ObjectLifecycle) -> Result<Coverage> {
	let connection_id = object
		.start
		.connection_id
		.ok_or_else(|| anyhow!("object trace {} is missing connection_id", object.start.trace_id))?;
	let stream_id = object
		.start
		.stream_id
		.ok_or_else(|| anyhow!("object trace {} is missing stream_id", object.start.trace_id))?;
	let start = object
		.start
		.stream_offset_start
		.ok_or_else(|| anyhow!("object trace {} is missing stream_offset_start", object.start.trace_id))?;
	let end = object
		.end
		.stream_offset_end
		.ok_or_else(|| anyhow!("object trace {} is missing stream_offset_end", object.start.trace_id))?;
	if end <= start {
		bail!("object trace {} has an empty transport range", object.start.trace_id);
	}
	let target = start..end;
	let mut candidates = trace
		.frames
		.iter()
		.filter_map(|frame| {
			let packet = trace.packets.get(&frame.trace_id)?;
			(packet.start.connection_id == connection_id
				&& packet.start.direction == object.start.direction
				&& packet.start.sample_rate == 1
				&& packet.end.outcome == PacketOutcome::Success
				&& frame.stream_id == stream_id
				&& overlaps(&(frame.offset_start..frame.offset_end), &target))
			.then_some((frame, packet))
		})
		.collect::<Vec<_>>();
	candidates.sort_by_key(|(frame, packet)| (frame.timestamp_ns, packet.end.timestamp_ns, frame.trace_id));

	let mut covered = RangeSet::new();
	let mut packet_ids = Vec::new();
	let mut first_start = None;
	let mut first_end = None;
	let mut complete_end = None;
	for (frame, packet) in candidates {
		let range = frame.offset_start.max(start)..frame.offset_end.min(end);
		if range.is_empty() {
			continue;
		}
		covered.insert(range);
		if !packet_ids.contains(&frame.trace_id) {
			packet_ids.push(frame.trace_id);
		}
		first_start.get_or_insert(packet.start.timestamp_ns);
		first_end.get_or_insert(packet.end.timestamp_ns);
		if covered.gaps(&target).next().is_none() {
			complete_end = Some(packet.end.timestamp_ns);
			break;
		}
	}
	Ok(Coverage {
		first_start: first_start
			.ok_or_else(|| anyhow!("object trace {} has no covering packets", object.start.trace_id))?,
		first_end: first_end.unwrap(),
		complete_end: complete_end.ok_or_else(|| {
			anyhow!(
				"object trace {} does not have complete packet coverage",
				object.start.trace_id
			)
		})?,
		packet_ids,
	})
}

fn overlaps(left: &Range<u64>, right: &Range<u64>) -> bool {
	left.start < right.end && right.start < left.end
}
