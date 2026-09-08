use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::time::Duration;

use anyhow::Result;

mod artifact;
mod coverage;
mod ingest;
mod metrics;
mod model;

pub(crate) use model::{Metric, Report, Sample, Statistics};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Options {
	pub object_size: NonZeroU64,
	pub subscribers: NonZeroUsize,
	pub warmup: Duration,
	pub cooldown: Duration,
}

pub(crate) fn run(input: &Path, output: &Path, options: Options) -> Result<Report> {
	let trace = ingest::read(input)?;
	let report = metrics::analyze(&trace, options)?;
	artifact::publish(output, &report)?;
	Ok(report)
}
