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
	#[cfg(feature = "trace")]
	pub fn build(&self) -> anyhow::Result<moq_net::trace::Handle> {
		let Some(path) = self.path.clone() else {
			let handle = moq_net::trace::Handle::disabled();
			moq_trace::set_global(handle.clone());
			return Ok(handle);
		};

		let config = moq_trace::Config {
			path: Some(path.clone()),
			object_sample: self.object_sample.unwrap_or(1).max(1),
			packet_sample: self.packet_sample.unwrap_or(1).max(1),
			queue_capacity: self.queue_capacity.unwrap_or(4096).max(1),
		};
		let handle = moq_net::trace::Handle::new(config)?;
		moq_trace::set_global(handle.clone());
		tracing::info!(path = %path.display(), object_sample = self.object_sample.unwrap_or(1).max(1), packet_sample = self.packet_sample.unwrap_or(1).max(1), "raw trace enabled");
		Ok(handle)
	}

	/// Build a disabled trace handle when the relay is compiled without tracing.
	#[cfg(not(feature = "trace"))]
	pub fn build(&self) -> anyhow::Result<moq_net::trace::Handle> {
		anyhow::ensure!(
			self.path.is_none()
				&& self.object_sample.is_none()
				&& self.packet_sample.is_none()
				&& self.queue_capacity.is_none(),
			"relay tracing requires building moq-relay with --features trace"
		);
		Ok(moq_net::trace::Handle::disabled())
	}
}

#[cfg(test)]
mod tests {
	#[cfg(not(feature = "trace"))]
	use super::*;

	#[cfg(not(feature = "trace"))]
	#[test]
	fn configured_trace_requires_feature() {
		let config = TraceConfig {
			path: Some(PathBuf::from("/tmp/moq-trace.jsonl")),
			..Default::default()
		};

		let Err(err) = config.build() else {
			panic!("trace config should require the trace feature");
		};
		assert!(err.to_string().contains("--features trace"));
	}
}
