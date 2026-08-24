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

	/// Emit all events for every Nth moq-transport object ID. Defaults to 1.
	#[arg(long = "trace-object-sample", env = "MOQ_TRACE_OBJECT_SAMPLE")]
	pub object_sample: Option<u64>,

	/// Emit all events for every Nth QUIC packet number. Defaults to 1.
	#[arg(long = "trace-packet-sample", env = "MOQ_TRACE_PACKET_SAMPLE")]
	pub packet_sample: Option<u64>,

	/// Emit every Nth UDP socket operation. Defaults to 1.
	#[arg(long = "trace-socket-sample", env = "MOQ_TRACE_SOCKET_SAMPLE")]
	pub socket_sample: Option<u64>,

	/// Maximum queued trace events before new events are dropped. Defaults to 4096.
	#[arg(long = "trace-queue-capacity", env = "MOQ_TRACE_QUEUE_CAPACITY")]
	pub queue_capacity: Option<usize>,
}

/// Keeps the configured trace writer alive for the relay process lifetime.
pub struct Trace {
	#[cfg(feature = "trace")]
	handle: moq_trace::Handle,
}

impl Trace {
	/// Flush every trace event accepted before this call.
	#[cfg(feature = "trace")]
	pub fn flush(&self) -> bool {
		self.handle.flush()
	}

	/// Report success when tracing is not compiled in.
	#[cfg(not(feature = "trace"))]
	pub fn flush(&self) -> bool {
		true
	}
}

impl TraceConfig {
	/// Configure the process-global trace destination.
	#[cfg(feature = "trace")]
	pub fn build(&self) -> anyhow::Result<Trace> {
		let Some(path) = self.path.clone() else {
			let handle = moq_trace::Handle::disabled();
			moq_trace::install_global(&handle)?;
			return Ok(Trace { handle });
		};

		let mut config = moq_trace::Config::default();
		config.path = Some(path.clone());
		config.object_sample = self.object_sample.unwrap_or(config.object_sample).max(1);
		config.packet_sample = self.packet_sample.unwrap_or(config.packet_sample).max(1);
		config.socket_sample = self.socket_sample.unwrap_or(config.socket_sample).max(1);
		config.queue_capacity = self.queue_capacity.unwrap_or(config.queue_capacity).max(1);
		let object_sample = config.object_sample;
		let packet_sample = config.packet_sample;
		let socket_sample = config.socket_sample;
		let handle = moq_trace::Handle::new(config)?;
		moq_trace::install_global(&handle)?;
		tracing::info!(
			path = %path.display(),
			object_sample,
			packet_sample,
			socket_sample,
			"raw trace enabled"
		);
		Ok(Trace { handle })
	}

	/// Reject trace settings when the relay was built without tracing.
	#[cfg(not(feature = "trace"))]
	pub fn build(&self) -> anyhow::Result<Trace> {
		anyhow::ensure!(
			self.path.is_none()
				&& self.object_sample.is_none()
				&& self.packet_sample.is_none()
				&& self.socket_sample.is_none()
				&& self.queue_capacity.is_none(),
			"relay tracing requires building moq-relay with --features trace"
		);
		Ok(Trace {})
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
