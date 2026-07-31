# Object Timeline Design

## Goal

Add a static timeline artifact to the relay-latency experiment so a reader can
compare the complete traced lifecycle of typical and tail-latency objects.

The chart answers: where does time accumulate between the relay starting an
inbound object and finishing every outbound copy?

## Selection

Use only steady-state objects that already pass the analyzer's completeness,
payload-size, warmup, and cooldown validation.

For each logical `(group_id, object_id)`:

1. Calculate `full_span` for every outbound copy.
2. Use the slowest copy as the object's selection value.
3. Calculate the mean, median, and p99 across those per-object values.
4. For each statistic, select the real object with the nearest value.
5. Break equal-distance ties by `(group_id, object_id)`.

The reported p99 is therefore the p99 of each object's slowest subscriber
copy, not the p99 across every individual copy. The definition stays the same
as fanout changes, although the values can increase when more subscribers are
included in each maximum.

Selections remain separate when two statistics choose the same object. Each
panel states the target statistic, selected object identity, actual slowest
`full_span`, and subscriber count.

## Timeline Data

Retain every object event for each selected identity:

- object lifecycle start and end;
- RX `header_parse`, `create`, and `payload_read` phase starts and completions;
- TX `clone`, `header_encode`, and `payload_write` phase starts and completions;
- every repeated payload read or write interval.

Use `session_id` to keep outbound copies separate. Normalize every timestamp to
the selected object's RX object start and display elapsed microseconds. The
chart therefore compares in-process durations without implying synchronized
wall clocks or network latency.

The MoQ session driver assigns monotonically increasing process-local
`session_id` values, starting at 1 when the relay starts, before cloning its
trace handle into the session halves. Object scopes inherit that ID unless an
event supplies a more specific one. This makes each subscriber forwarding copy
distinguishable and orders sessions by creation at the relay without changing
MoQ or QUIC wire data. IDs reset on process restart and do not identify a
protocol, connection, stream, or subscriber outside that trace.

Reject a selected object if a phase start cannot be paired with its completion
within the same direction, session, and phase. Existing analysis validation
continues to reject incomplete object lifecycles before plotting.

## Chart

Write `object_timeline.png` beside the existing artifacts.

Use three vertically stacked panels with one shared elapsed-microseconds x-axis:

1. Mean-nearest object
2. Median-nearest object
3. P99-nearest object

Within each panel:

- use exactly six y-axis rows in `ObjectPhase` declaration order:
  `RX Header Parse`, `RX Create`, `RX Payload Read`, `TX Clone`,
  `TX Header Encode`, and `TX Payload Write`;
- draw phase durations as horizontal intervals;
- display only the first-created and last-created outbound sessions, determined
  by the lowest and highest TX `session_id` present for the selected object;
- display one session only when first and last are the same;
- offset the displayed sessions within each TX phase row and label them by
  subscriber creation ordinal (`TX #1`, `TX #50`) instead of raw session ID;
- draw object lifecycle start/end boundaries as vertical guides because
  lifecycle is not an `ObjectPhase` variant;
- use restrained blue for RX and an orange palette for TX sessions;
- retain repeated payload intervals rather than merging away scheduling gaps.

The panel title states which subscriber copy was slowest across the complete
fanout and its `full_span`, even when that subscriber is not one of the two
displayed copies. Intermediate sessions remain in the trace and aggregate
statistics but are omitted from the timeline. An optional mode that adds the
slowest copy as a third lane is deferred until it is needed.

All panels use the same x-axis limit, starting at zero, so typical and tail
objects are visually comparable. The title is descriptive, and the subtitle
states units, selection rule, object size, and subscriber fanout.

## Runner Integration

Extend the validated analysis result with the event data and deterministic
selection metadata needed by the timeline renderer. Keep selection and
rendering as separate private functions so they can be tested independently.

Generate the new chart after trace analysis, alongside `objects.csv`,
`summary.json`, and `latency.png`. Print its path on successful completion.
Record the selected group/object IDs, target statistics, actual values, and the
first-created, last-created, and slowest copy metadata in `summary.json`. Each
copy record includes its raw `session_id`, subscriber creation ordinal, and
`full_span` so the image is reproducible from the raw trace.

## Failure Handling

Fail the experiment with `TraceError` when:

- no steady-state object is eligible;
- a selected object has no RX start;
- an event lacks the session identity needed to separate copies;
- a selected phase has an unmatched start or completion.

Do not silently omit malformed events or substitute a different object.

## Testing and QA

Add tests that:

- select the nearest real object for mean, median, and p99;
- use the slowest outbound copy for multi-subscriber selection;
- resolve ties deterministically;
- preserve duplicate selections when statistics resolve to one object;
- pair repeated payload intervals by session and phase;
- reject unmatched phase boundaries;
- allocate sequential process-local session IDs;
- display only first-created and last-created subscriber copies;
- retain slowest-copy selection when an intermediate subscriber is slowest;
- write a non-empty `object_timeline.png`;
- include selection metadata and the artifact path in the summary and CLI output.

Run the complete relay-latency Python test suite and a short end-to-end
experiment. Inspect the exported PNG at its actual resolution to confirm that
titles, row labels, markers, repeated intervals, and all three panels are
readable without collisions or clipping.
