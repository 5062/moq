use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::time::Duration;

use anyhow::Result;

mod artifact;
mod coverage;
mod ctf;
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

#[derive(Clone, Copy, Debug)]
pub(crate) struct Source<'a> {
	pub ctf: &'a Path,
	pub python: &'a Path,
	pub expected_pid: Option<u32>,
}

pub(crate) fn run(source: Source<'_>, output: &Path, options: Options) -> Result<Report> {
	let trace = ingest::read(source.ctf, source.python, source.expected_pid)?;
	let report = metrics::analyze(&trace, options)?;
	artifact::publish(output, &report)?;
	Ok(report)
}
