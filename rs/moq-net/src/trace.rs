//! Tracing adapter for MoQ object instrumentation.

pub(crate) use moq_trace::*;

/// Return the handle a session records object events through.
///
/// Object identity and stream offsets are compiled only with this crate's `trace`
/// feature, but emission follows `moq-trace/lttng`, which the Quinn fork can pull
/// in alone. Without `trace` a session therefore gets a disabled handle, so it
/// never emits objects that lack the logical identity and stream position the
/// analysis joins on. This crate's feature is the one switch.
#[cfg(feature = "trace")]
pub(crate) fn session_handle<S: web_transport_trait::Session>(session: &S) -> Handle {
	let handle = global().with_new_session_id();
	match session.connection_id() {
		Some(connection_id) => handle.with_connection_id(connection_id.into_inner()),
		None => handle,
	}
}

#[cfg(not(feature = "trace"))]
pub(crate) fn session_handle<S: web_transport_trait::Session>(_session: &S) -> Handle {
	Handle::disabled()
}

/// Finishes an object phase from the result of the work it covered.
pub(crate) trait FinishResult {
	/// Record `Success` or `Failed` from `result`, then hand it back.
	///
	/// Write it as `object.phase(p).finish_result(work())`: the receiver is
	/// evaluated before the argument, so the phase starts before `work` runs.
	fn finish_result<T, E>(self, result: Result<T, E>) -> Result<T, E>;
}

impl FinishResult for ObjectPhaseTrace<'_> {
	fn finish_result<T, E>(self, result: Result<T, E>) -> Result<T, E> {
		self.finish(match result {
			Ok(_) => ObjectOutcome::Success,
			Err(_) => ObjectOutcome::Failed,
		});
		result
	}
}
