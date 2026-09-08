use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, anyhow, bail};
use moq_trace::PacketOutcome;

use super::Options;
use super::coverage;
use super::model::{
	LogicalObject, Metric, ObjectLifecycle, ObjectTimeline, PacketSample, Report, Sample, Statistics, TimelineCopy,
	TimelineInterval, TimelineSelection, Trace,
};

pub(super) fn analyze(trace: &Trace, options: Options) -> Result<Report> {
	if trace.packets.values().any(|packet| packet.start.sample_rate != 1) {
		bail!("QUIC object correlation requires packet_sample = 1");
	}
	let (objects, origin, group_count) = steady_state(trace, options)?;
	let object_samples = object_samples(&objects, origin);
	let quic_object_samples = quic_samples(trace, &objects, origin)?;
	let packet_samples = packet_samples(trace, origin);
	let packet_statistics = summarize_packet(&packet_samples)?;
	for required in ["rx_routing", "rx_scheduling"] {
		if !packet_statistics.contains_key(required) {
			bail!("Quinn trace is missing packet metric: {required}");
		}
	}
	Ok(Report {
		statistics: summarize(&object_samples)?,
		quic_object_statistics: summarize(&quic_object_samples)?,
		packet_statistics,
		packet_count: trace.packets.len(),
		group_count,
		timelines: timelines(trace, &objects)?,
		object_samples,
		quic_object_samples,
		packet_samples,
	})
}

fn steady_state(trace: &Trace, options: Options) -> Result<(Vec<&LogicalObject>, u64, usize)> {
	let mut matching = trace
		.objects
		.values()
		.filter(|object| object.rx.end.payload_bytes == options.object_size.get())
		.collect::<Vec<_>>();
	if matching.is_empty() {
		bail!("trace has no completed {}-byte inbound objects", options.object_size);
	}
	matching.sort_by_key(|object| object.rx.start.timestamp_ns);
	let origin = matching[0].rx.start.timestamp_ns;
	let last = matching.last().unwrap().rx.start.timestamp_ns;
	let warmup = u64::try_from(options.warmup.as_nanos()).unwrap_or(u64::MAX);
	let cooldown = u64::try_from(options.cooldown.as_nanos()).unwrap_or(u64::MAX);
	let start = origin.saturating_add(warmup);
	let end = last.saturating_sub(cooldown);
	let selected = matching
		.into_iter()
		.filter(|object| (start..=end).contains(&object.rx.start.timestamp_ns))
		.collect::<Vec<_>>();
	if selected.is_empty() {
		bail!("steady-state window contains no complete objects");
	}
	for object in &selected {
		if object.tx.len() != options.subscribers.get() {
			bail!(
				"logical object {} has {} outbound copies, expected {}",
				object.rx.start.logical_id,
				object.tx.len(),
				options.subscribers
			);
		}
	}
	let groups = selected
		.iter()
		.map(|object| object.rx.start.group_id)
		.collect::<BTreeSet<_>>();
	let min = *groups.first().unwrap();
	let max = *groups.last().unwrap();
	if groups.len() as u64 != max - min + 1 {
		bail!("steady-state groups are not contiguous");
	}
	Ok((selected, origin, groups.len()))
}

fn object_samples(objects: &[&LogicalObject], origin: u64) -> Vec<Sample> {
	objects
		.iter()
		.flat_map(|object| {
			object.tx.iter().enumerate().map(move |(copy, tx)| Sample {
				group_id: object.rx.start.group_id,
				object_id: object.rx.start.object_id,
				metric: Metric::FullSpan,
				copy_ordinal: copy,
				elapsed_ms: ms(object.rx.start.timestamp_ns.saturating_sub(origin)),
				latency_us: us(tx.end.timestamp_ns.saturating_sub(object.rx.start.timestamp_ns)),
			})
		})
		.collect()
}

fn quic_samples(trace: &Trace, objects: &[&LogicalObject], origin: u64) -> Result<Vec<Sample>> {
	let mut rows = Vec::new();
	for object in objects {
		let inbound = coverage::resolve(trace, &object.rx)?;
		for (copy, tx) in object.tx.iter().enumerate() {
			let outbound = coverage::resolve(trace, tx)?;
			for (metric, start, end) in [
				(Metric::QuicForwardStart, inbound.first_start, outbound.first_end),
				(Metric::QuicTailGap, inbound.complete_end, outbound.complete_end),
				(Metric::QuicFullSpan, inbound.first_start, outbound.complete_end),
			] {
				if end < start {
					bail!(
						"{} is negative for logical object {} copy {}",
						metric.as_str(),
						object.rx.start.logical_id,
						copy
					);
				}
				rows.push(Sample {
					group_id: object.rx.start.group_id,
					object_id: object.rx.start.object_id,
					metric,
					copy_ordinal: copy,
					elapsed_ms: ms(inbound.first_start.saturating_sub(origin)),
					latency_us: us(end - start),
				});
			}
		}
	}
	Ok(rows)
}

fn packet_samples(trace: &Trace, origin: u64) -> Vec<PacketSample> {
	let mut rows = Vec::new();
	for packet in trace.packets.values() {
		if packet.end.outcome != PacketOutcome::Success {
			continue;
		}
		rows.push(PacketSample {
			metric: format!("{}_packet_span", packet.start.direction),
			direction: packet.start.direction,
			connection_id: packet.start.connection_id,
			trace_id: packet.start.trace_id,
			occurrence: 0,
			elapsed_ms: ms(packet.start.timestamp_ns.saturating_sub(origin)),
			latency_us: us(packet.end.timestamp_ns - packet.start.timestamp_ns),
		});
		let mut processing_start = None;
		for phase in &packet.phases {
			if packet.start.direction == moq_trace::Direction::Rx && phase.phase == moq_trace::PacketPhase::Scheduling {
				processing_start = Some(phase.interval.end);
			}
			rows.push(PacketSample {
				metric: format!("{}_{}", packet.start.direction, phase.phase.as_str()),
				direction: packet.start.direction,
				connection_id: packet.start.connection_id,
				trace_id: packet.start.trace_id,
				occurrence: phase.occurrence,
				elapsed_ms: ms(phase.interval.start.saturating_sub(origin)),
				latency_us: us(phase.interval.end - phase.interval.start),
			});
		}
		if let Some(start) = processing_start
			&& packet.end.timestamp_ns >= start
		{
			rows.push(PacketSample {
				metric: "rx_packet_processing_span".into(),
				direction: packet.start.direction,
				connection_id: packet.start.connection_id,
				trace_id: packet.start.trace_id,
				occurrence: 0,
				elapsed_ms: ms(start.saturating_sub(origin)),
				latency_us: us(packet.end.timestamp_ns - start),
			});
		}
	}
	rows
}

fn summarize(rows: &[Sample]) -> Result<BTreeMap<String, Statistics>> {
	summarize_values(rows.iter().map(|row| (row.metric.as_str(), row.latency_us)))
}

fn summarize_packet(rows: &[PacketSample]) -> Result<BTreeMap<String, Statistics>> {
	summarize_values(rows.iter().map(|row| (row.metric.as_str(), row.latency_us)))
}

fn summarize_values<'a>(rows: impl Iterator<Item = (&'a str, f64)>) -> Result<BTreeMap<String, Statistics>> {
	let mut grouped: BTreeMap<String, Vec<f64>> = BTreeMap::new();
	for (metric, value) in rows {
		grouped.entry(metric.into()).or_default().push(value);
	}
	if grouped.is_empty() {
		bail!("cannot summarize empty samples");
	}
	Ok(grouped
		.into_iter()
		.map(|(metric, mut values)| {
			values.sort_by(f64::total_cmp);
			let count = values.len();
			(
				metric,
				Statistics {
					count,
					mean: values.iter().sum::<f64>() / count as f64,
					p50: quantile(&values, 0.5),
					p95: quantile(&values, 0.95),
					p99: quantile(&values, 0.99),
					max: *values.last().unwrap(),
				},
			)
		})
		.collect())
}

fn quantile(values: &[f64], quantile: f64) -> f64 {
	let position = (values.len() - 1) as f64 * quantile;
	let low = position.floor() as usize;
	let high = position.ceil() as usize;
	values[low] + (values[high] - values[low]) * (position - low as f64)
}

fn timelines(trace: &Trace, objects: &[&LogicalObject]) -> Result<Vec<ObjectTimeline>> {
	let mut slowest = objects
		.iter()
		.map(|object| {
			let value = object
				.tx
				.iter()
				.map(|tx| us(tx.end.timestamp_ns - object.rx.start.timestamp_ns))
				.max_by(f64::total_cmp)
				.unwrap();
			(*object, value)
		})
		.collect::<Vec<_>>();
	let mut values = slowest.iter().map(|(_, value)| *value).collect::<Vec<_>>();
	values.sort_by(f64::total_cmp);
	let targets = [
		("mean", values.iter().sum::<f64>() / values.len() as f64),
		("median", quantile(&values, 0.5)),
		("p99", quantile(&values, 0.99)),
	];
	targets
		.into_iter()
		.map(|(statistic, target)| {
			let (object, actual) = slowest
				.iter_mut()
				.min_by(|left, right| (left.1 - target).abs().total_cmp(&(right.1 - target).abs()))
				.map(|(object, value)| (*object, *value))
				.unwrap();
			build_timeline(trace, object, statistic, target, actual)
		})
		.collect()
}

fn build_timeline(
	trace: &Trace,
	object: &LogicalObject,
	statistic: &str,
	target: f64,
	actual: f64,
) -> Result<ObjectTimeline> {
	let origin = object.rx.start.timestamp_ns;
	let mut intervals = lifecycle_intervals(trace, &object.rx, origin)?;
	let mut copies = Vec::new();
	for (index, tx) in object.tx.iter().enumerate() {
		intervals.extend(lifecycle_intervals(trace, tx, origin)?);
		copies.push(TimelineCopy {
			session_id: tx
				.start
				.session_id
				.ok_or_else(|| anyhow!("TX object is missing session_id"))?,
			subscriber_ordinal: index + 1,
			full_span_us: us(tx.end.timestamp_ns - origin),
		});
	}

	let slowest_copy = *copies
		.iter()
		.max_by(|left, right| left.full_span_us.total_cmp(&right.full_span_us))
		.unwrap();

	Ok(ObjectTimeline {
		selection: TimelineSelection {
			statistic: statistic.into(),
			target_us: target,
			group_id: object.rx.start.group_id,
			object_id: object.rx.start.object_id,
			actual_us: actual,
		},
		intervals,
		first_copy: copies[0],
		last_copy: *copies.last().unwrap(),
		slowest_copy,
	})
}

fn lifecycle_intervals(trace: &Trace, lifecycle: &ObjectLifecycle, origin: u64) -> Result<Vec<TimelineInterval>> {
	let session_id = lifecycle
		.start
		.session_id
		.ok_or_else(|| anyhow!("object trace {} is missing session_id", lifecycle.start.trace_id))?;
	let mut values = vec![TimelineInterval {
		direction: lifecycle.start.direction,
		session_id,
		phase: "object".into(),
		occurrence: 0,
		start_us: signed_us(lifecycle.start.timestamp_ns, origin),
		end_us: signed_us(lifecycle.end.timestamp_ns, origin),
	}];
	values.extend(lifecycle.phases.iter().map(|phase| TimelineInterval {
		direction: lifecycle.start.direction,
		session_id,
		phase: phase.phase.as_str().into(),
		occurrence: phase.occurrence,
		start_us: signed_us(phase.interval.start, origin),
		end_us: signed_us(phase.interval.end, origin),
	}));
	let coverage = coverage::resolve(trace, lifecycle)?;
	for (occurrence, trace_id) in coverage.packet_ids.into_iter().enumerate() {
		let packet = &trace.packets[&trace_id];
		values.push(TimelineInterval {
			direction: lifecycle.start.direction,
			session_id,
			phase: "quic_packet".into(),
			occurrence,
			start_us: signed_us(packet.start.timestamp_ns, origin),
			end_us: signed_us(packet.end.timestamp_ns, origin),
		});
		values.extend(packet.phases.iter().map(|phase| TimelineInterval {
			direction: lifecycle.start.direction,
			session_id,
			phase: format!("quic_{}", phase.phase.as_str()),
			occurrence: phase.occurrence,
			start_us: signed_us(phase.interval.start, origin),
			end_us: signed_us(phase.interval.end, origin),
		}));
	}
	Ok(values)
}

fn ms(nanoseconds: u64) -> f64 {
	nanoseconds as f64 / 1_000_000.0
}

fn us(nanoseconds: u64) -> f64 {
	nanoseconds as f64 / 1_000.0
}

fn signed_us(timestamp: u64, origin: u64) -> f64 {
	(timestamp as f64 - origin as f64) / 1_000.0
}
