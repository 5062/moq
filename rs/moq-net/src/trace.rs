//! Tracing adapter for MoQ object instrumentation.

pub(crate) use moq_trace::*;

/// Return the handle a session records object events through.
///
/// Object identity and stream offsets are compiled only with this crate's `trace`
/// feature, but emission follows `moq-trace/lttng`, which another crate such as
/// the Quinn fork can turn on alone. Without `trace` a session therefore gets a
/// disabled handle, so it never emits objects that lack the logical identity and
/// stream position the analysis joins on. This crate's feature is the one switch.
pub(crate) fn session_handle() -> Handle {
	#[cfg(feature = "trace")]
	{
		global()
	}
	#[cfg(not(feature = "trace"))]
	{
		Handle::disabled()
	}
}
