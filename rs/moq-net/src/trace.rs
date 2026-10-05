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

/// Run a step that makes received data readable in the relay model.
///
/// The model wakes waiting consumers while the step's state guard drops, inside
/// the step. Each wake is recorded as [`ObjectPhase::Notify`], and the rest of
/// the step as `phase`, so the step becomes alternating occurrences that never
/// overlap. The last occurrence carries the step's outcome. Phases are emitted
/// after the step returns, so emission adds nothing to the measured intervals.
#[cfg(feature = "trace")]
pub(crate) fn publish<T, E>(
	object: &mut ObjectTrace,
	phase: ObjectPhase,
	step: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
	if !object.records_phases() {
		return step();
	}
	let start_ns = now_ns();
	let (result, wakes) = kio::probe::observe(now_ns, step);
	let end_ns = now_ns();
	let mut cursor = start_ns;
	for &(wake_start, wake_end) in wakes.intervals() {
		object
			.phase_at(phase, cursor)
			.finish_at(ObjectOutcome::Success, wake_start);
		object
			.phase_at(ObjectPhase::Notify, wake_start)
			.finish_at(ObjectOutcome::Success, wake_end);
		cursor = wake_end;
	}
	let outcome = match result {
		Ok(_) => ObjectOutcome::Success,
		Err(_) => ObjectOutcome::Failed,
	};
	object.phase_at(phase, cursor).finish_at(outcome, end_ns);
	result
}

/// Run a step that makes received data readable; untraced builds record nothing.
#[cfg(not(feature = "trace"))]
pub(crate) fn publish<T, E>(
	_object: &mut ObjectTrace,
	_phase: ObjectPhase,
	step: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
	step()
}
