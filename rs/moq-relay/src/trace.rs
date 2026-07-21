//! Relay-side raw trace configuration.

use std::path::PathBuf;

use clap::Args;
use serde::{Deserialize, Serialize};

/// Configuration for raw JSONL relay tracing.
///
/// Tracing is disabled unless [`Self::path`] is set. Object events come from
/// moq-transport session code and packet events come from the local quinn patch.
#[derive(Args, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
#[group(id = "trace-config")]
pub struct TraceConfig {
	/// JSONL file path to write raw trace events to.
	#[arg(long = "trace-path", env = "MOQ_TRACE_PATH")]
	pub path: Option<PathBuf>,

	/// Emit every Nth moq-transport object event. Defaults to 1.
	#[arg(long = "trace-object-sample", env = "MOQ_TRACE_OBJECT_SAMPLE")]
	pub object_sample: Option<u64>,

	/// Emit every Nth QUIC packet event. Defaults to 1.
	#[arg(long = "trace-packet-sample", env = "MOQ_TRACE_PACKET_SAMPLE")]
	pub packet_sample: Option<u64>,

	/// Maximum queued trace events before new events are dropped. Defaults to 4096.
	#[arg(long = "trace-queue-capacity", env = "MOQ_TRACE_QUEUE_CAPACITY")]
	pub queue_capacity: Option<usize>,
}

impl TraceConfig {
	/// Build the raw trace handle for this relay.
	pub fn build(&self) -> anyhow::Result<moq_trace::Handle> {
		let Some(path) = self.path.clone() else {
			let handle = moq_trace::Handle::disabled();
			moq_trace::set_global(handle.clone());
			return Ok(handle);
		};

		let config = moq_trace::Config {
			path: Some(path.clone()),
			object_sample: self.object_sample.unwrap_or(1).max(1),
			packet_sample: self.packet_sample.unwrap_or(1).max(1),
			queue_capacity: self.queue_capacity.unwrap_or(4096).max(1),
		};
		let handle = moq_trace::Handle::new(config)?;
		moq_trace::set_global(handle.clone());
		tracing::info!(path = %path.display(), object_sample = self.object_sample.unwrap_or(1).max(1), packet_sample = self.packet_sample.unwrap_or(1).max(1), "raw trace enabled");
		Ok(handle)
	}
}
