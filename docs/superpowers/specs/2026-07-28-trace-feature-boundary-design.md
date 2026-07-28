# Trace Feature Boundary Design

## Goal

Keep detailed MoQ and Quinn tracing opt-in without letting conditional
compilation reshape the core protocol control flow.

## Scope

This change updates the workspace tracing integration in `rs/moq-net`,
`rs/moq-relay`, and `rs/moq-trace`. The patched Quinn branch keeps its
`moq-trace` feature gate and existing typed packet and socket tokens. Trace
schema compatibility is unchanged.

## Architecture

`moq-net` keeps an optional `moq-trace` dependency behind its `trace` feature.
Its public `trace` module becomes a stable facade in both feature
configurations. The enabled facade re-exports the real tracing API. The
disabled facade supplies zero-sized handles and object tokens whose methods
inline to no-ops.

Core protocol code stores a trace handle unconditionally and uses one object
token API. Starting a token performs the enabled and sampling decision before
reading the clock or constructing a complete event. Phase and completion
methods own timestamping and event emission. In a build without the feature,
the compiler can remove the zero-sized token operations. In a trace-enabled
build with a disabled or sampled-out handle, token creation returns a no-op
token before timestamping.

## Object tracing

The facade exposes an object metadata value containing stable fields known by
the protocol and an object trace token containing optional sampled state.
Protocol code creates one token per object, records phase boundaries through
methods on that token, updates payload size and stream offsets as they become
known, and completes the token explicitly.

The token replaces free functions that require fully constructed
`ObjectEvent` values at every call site. Dropping an unfinished object token
does not emit an end event because many existing error paths deliberately
terminate object processing early and the current schema has no object outcome
field.

Outbound model lookup uses one shared `poll_next_frame` implementation. An
optional object token surrounds the frame construction without duplicating the
polling and prefetch logic.

## Offsets and handles

`Reader::offset()` and `Writer::offset()` are available in every build. Both
types already maintain offsets unconditionally, so their public API does not
need to vary by feature.

Publisher and subscriber structures store `crate::trace::Handle`
unconditionally. The disabled handle is zero-sized. Constructors and function
signatures remain identical across feature configurations.

## Relay behavior

Relay trace configuration remains present in every build. A build without the
feature accepts an entirely empty trace configuration and rejects any requested
trace option with the existing actionable error. A trace-enabled build creates
the real handle and installs it globally.

## Quinn boundary

Quinn packet and socket instrumentation remains conditionally compiled under
its `moq-trace` feature. These hooks execute at packet and syscall frequency,
and Quinn should not acquire a MoQ-specific dependency or instrumentation state
by default. No workspace refactor weakens that boundary.

## Performance requirements

- A build without `trace` has no timestamp reads, allocations, atomics, or
  branches from object instrumentation after optimization.
- A trace-enabled build with a disabled handle checks the handle once per
  object and does not read the clock or construct serialized events.
- A sampled-out object checks sampling once and does not emit phase records.
- Sampled objects preserve the current event names, fields, phase order, and
  JSON representation.

## Testing

Unit tests in `moq-trace` cover disabled and sampled-out object token behavior,
sampled object phase emission, mutable metadata, and explicit completion.
`moq-net` tests cover unconditional reader and writer offsets and normal object
flow with tracing enabled.

Verification compiles and tests `moq-net` both without and with `trace`, tests
`moq-trace`, tests `moq-relay` both without and with `trace`, runs Clippy for
the affected crates, and runs the repository formatting checks. The existing
Quinn feature tests remain the evidence for its separate conditional boundary.
