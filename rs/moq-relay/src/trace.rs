//! Relay-side LTTng-UST trace configuration.

use clap::Args;
use serde::{Deserialize, Serialize};

/// Sampling configuration for LTTng-UST relay tracepoints.
#[derive(Args, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
#[group(id = "trace-config")]
pub struct TraceConfig {
	/// Emit all events for every Nth moq-transport object ID. Defaults to 1.
	#[arg(long = "trace-object-sample", env = "MOQ_TRACE_OBJECT_SAMPLE")]
	pub object_sample: Option<u64>,

	/// Emit all events for every Nth QUIC packet number. Defaults to 1.
	#[arg(long = "trace-packet-sample", env = "MOQ_TRACE_PACKET_SAMPLE")]
	pub packet_sample: Option<u64>,

	/// Emit every Nth UDP socket operation. Defaults to 1.
	#[arg(long = "trace-socket-sample", env = "MOQ_TRACE_SOCKET_SAMPLE")]
	pub socket_sample: Option<u64>,
}

impl TraceConfig {
	/// Install process-global relay tracing.
	#[cfg(feature = "trace")]
	pub fn install(&self) -> anyhow::Result<()> {
		let mut config = moq_trace::Config::default();
		config.object_sample = self.object_sample.unwrap_or(config.object_sample).max(1);
		config.packet_sample = self.packet_sample.unwrap_or(config.packet_sample).max(1);
		config.socket_sample = self.socket_sample.unwrap_or(config.socket_sample).max(1);
		let object_sample = config.object_sample;
		let packet_sample = config.packet_sample;
		let socket_sample = config.socket_sample;
		moq_trace::install(config)?;
		tracing::info!(
			object_sample,
			packet_sample,
			socket_sample,
			"LTTng-UST tracepoints registered"
		);
		Ok(())
	}

	/// Reject trace settings when the relay was built without tracing.
	#[cfg(not(feature = "trace"))]
	pub fn install(&self) -> anyhow::Result<()> {
		anyhow::ensure!(
			self.object_sample.is_none() && self.packet_sample.is_none() && self.socket_sample.is_none(),
			"relay tracing requires building moq-relay with --features trace"
		);
		Ok(())
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
			object_sample: Some(2),
			..Default::default()
		};

		let Err(err) = config.install() else {
			panic!("trace config should require the trace feature");
		};
		assert!(err.to_string().contains("--features trace"));
	}
}
