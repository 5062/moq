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
	let handle = global().with_session_id(next_session_id());
	match session.connection_id() {
		Some(connection_id) => handle.with_connection_id(connection_id.into_inner()),
		None => handle,
	}
}

#[cfg(not(feature = "trace"))]
pub(crate) fn session_handle<S: web_transport_trait::Session>(_session: &S) -> Handle {
	Handle::disabled()
}

/// How an object ended, given the error that stopped it.
///
/// A reset or stopped stream, and a stream closed under the copy, end it as
/// reset. A group the cache evicted or a reader lagged past ends it as dropped,
/// and a deadline as expired. Every other error is a failure.
pub(crate) fn outcome_of(err: &crate::Error) -> ObjectOutcome {
	use crate::Error;
	match err {
		Error::Remote(_) | Error::Cancel => ObjectOutcome::Reset,
		Error::Lagged | Error::Evicted | Error::Old => ObjectOutcome::Dropped,
		Error::Timeout => ObjectOutcome::Expired,
		_ => ObjectOutcome::Failed,
	}
}

/// Ends an object with the outcome of the error that stopped it.
pub(crate) trait FinishOnError {
	/// Finish `object` from the error, if there is one, then hand the result back.
	///
	/// Write it as `step.await.finish_on_error(&mut object)?`, so an object an
	/// early return leaves behind records why it ended instead of being abandoned.
	fn finish_on_error(self, object: &mut ObjectTrace) -> Self;
}

impl<T> FinishOnError for Result<T, crate::Error> {
	fn finish_on_error(self, object: &mut ObjectTrace) -> Self {
		if let Err(err) = &self {
			std::mem::replace(object, ObjectTrace::disabled()).finish(outcome_of(err));
		}
		self
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn errors_map_to_the_outcome_that_describes_them() {
		use crate::Error;
		assert_eq!(outcome_of(&Error::Remote(7)), ObjectOutcome::Reset);
		assert_eq!(outcome_of(&Error::Cancel), ObjectOutcome::Reset);
		assert_eq!(outcome_of(&Error::Evicted), ObjectOutcome::Dropped);
		assert_eq!(outcome_of(&Error::Lagged), ObjectOutcome::Dropped);
		assert_eq!(outcome_of(&Error::Timeout), ObjectOutcome::Expired);
		assert_eq!(outcome_of(&Error::WrongSize), ObjectOutcome::Failed);
	}
}
