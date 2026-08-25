use std::collections::BTreeMap;

use moq_trace::{Direction, ObjectEvent, ObjectPhase, PacketEvent, PacketPhase};
use serde::Serialize;

#[derive(Clone, Copy, Debug)]
pub(super) struct Interval {
	pub start: u64,
	pub end: u64,
}

#[derive(Clone, Debug)]
pub(super) struct PhaseInterval<P> {
	pub phase: P,
	pub occurrence: usize,
	pub interval: Interval,
}

#[derive(Clone, Debug)]
pub(super) struct ObjectLifecycle {
	pub start: ObjectEvent,
	pub end: moq_trace::ObjectEndEvent,
	pub phases: Vec<PhaseInterval<ObjectPhase>>,
}

#[derive(Clone, Debug)]
pub(super) struct LogicalObject {
	pub rx: ObjectLifecycle,
	pub tx: Vec<ObjectLifecycle>,
}

#[derive(Clone, Debug)]
pub(super) struct PacketLifecycle {
	pub start: PacketEvent,
	pub end: moq_trace::PacketEndEvent,
	pub phases: Vec<PhaseInterval<PacketPhase>>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct StreamFrame {
	pub trace_id: u64,
	pub timestamp_ns: u64,
	pub stream_id: u64,
	pub offset_start: u64,
	pub offset_end: u64,
}

pub(super) struct Trace {
	pub objects: BTreeMap<moq_trace::LogicalId, LogicalObject>,
	pub packets: BTreeMap<u64, PacketLifecycle>,
	pub frames: Vec<StreamFrame>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Sample {
	pub group_id: u64,
	pub object_id: u64,
	pub metric: Metric,
	pub copy_ordinal: usize,
	pub elapsed_ms: f64,
	pub latency_us: f64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PacketSample {
	pub metric: String,
	pub direction: Direction,
	pub connection_id: u64,
	pub trace_id: u64,
	pub occurrence: usize,
	pub elapsed_ms: f64,
	pub latency_us: f64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Metric {
	FullSpan,
	QuicForwardStart,
	QuicTailGap,
	QuicFullSpan,
}

impl Metric {
	pub fn as_str(self) -> &'static str {
		match self {
			Self::FullSpan => "full_span",
			Self::QuicForwardStart => "quic_forward_start",
			Self::QuicTailGap => "quic_tail_gap",
			Self::QuicFullSpan => "quic_full_span",
		}
	}
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct Statistics {
	pub count: usize,
	pub mean: f64,
	pub p50: f64,
	pub p95: f64,
	pub p99: f64,
	pub max: f64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TimelineSelection {
	pub statistic: String,
	pub target_us: f64,
	pub group_id: u64,
	pub object_id: u64,
	pub actual_us: f64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TimelineInterval {
	pub direction: Direction,
	pub session_id: u64,
	pub phase: String,
	pub occurrence: usize,
	pub start_us: f64,
	pub end_us: f64,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub(crate) struct TimelineCopy {
	pub session_id: u64,
	pub subscriber_ordinal: usize,
	pub full_span_us: f64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ObjectTimeline {
	pub selection: TimelineSelection,
	pub intervals: Vec<TimelineInterval>,
	pub first_copy: TimelineCopy,
	pub last_copy: TimelineCopy,
	pub slowest_copy: TimelineCopy,
}

#[derive(Debug, Serialize)]
pub(crate) struct Report {
	pub statistics: BTreeMap<String, Statistics>,
	pub quic_object_statistics: BTreeMap<String, Statistics>,
	pub packet_statistics: BTreeMap<String, Statistics>,
	pub packet_count: usize,
	pub group_count: usize,
	pub timelines: Vec<ObjectTimeline>,
	#[serde(skip)]
	pub object_samples: Vec<Sample>,
	#[serde(skip)]
	pub quic_object_samples: Vec<Sample>,
	#[serde(skip)]
	pub packet_samples: Vec<PacketSample>,
}
