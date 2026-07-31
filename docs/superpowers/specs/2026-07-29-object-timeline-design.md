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

The MoQ session driver assigns one process-local `session_id` before cloning
its trace handle into the session halves. Object scopes inherit that ID unless
an event supplies a more specific one. This makes each subscriber forwarding
copy distinguishable in the trace without changing MoQ or QUIC wire data.

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
- offset outbound sessions within each TX phase row and identify them in a
  legend instead of adding session-specific y-axis rows;
- draw object lifecycle start/end boundaries as vertical guides because
  lifecycle is not an `ObjectPhase` variant;
- use restrained blue for RX and an orange palette for TX sessions;
- retain repeated payload intervals rather than merging away scheduling gaps.

All panels use the same x-axis limit, starting at zero, so typical and tail
objects are visually comparable. The title is descriptive, and the subtitle
states units, selection rule, object size, and subscriber fanout.

## Runner Integration

Extend the validated analysis result with the event data and deterministic
selection metadata needed by the timeline renderer. Keep selection and
rendering as separate private functions so they can be tested independently.

Generate the new chart after trace analysis, alongside `objects.csv`,
`summary.json`, and `latency.png`. Print its path on successful completion.
Record the selected group/object IDs, target statistics, actual values, and
slowest-copy session IDs in `summary.json` so the image is reproducible from the
raw trace.

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
- write a non-empty `object_timeline.png`;
- include selection metadata and the artifact path in the summary and CLI output.

Run the complete relay-latency Python test suite and a short end-to-end
experiment. Inspect the exported PNG at its actual resolution to confirm that
titles, row labels, markers, repeated intervals, and all three panels are
readable without collisions or clipping.
