# Relay Latency Experiment Runner Design

## Goal

Provide a reproducible Python runner that builds and launches a local MoQ relay,
one publisher, and one or more subscribers; optionally pins the relay to one
logical CPU; collects relay trace data; computes latency statistics; and writes a Matplotlib
report.

The workload is configurable. Its defaults are:

- one publisher;
- one subscriber;
- one object per group;
- 16 KiB per object;
- 30 objects and groups per second;
- Quinn over loopback;
- `moq-transport-19`.

## Exact workload representation

`moq-bench` currently writes a JSON header object followed by `group_size`
zero-filled objects. With `group_size = 0`, the header is the only object, but
`frame_size` does not affect its size.

When `group_size` is zero, the producer will extend the serialized JSON header
with trailing ASCII spaces until it reaches `frame_size`. JSON permits trailing
whitespace, so subscribers can still parse the header. Padding only occurs when
the serialized header is shorter than `frame_size`; the producer never truncates
JSON. Existing zero-size control-plane workloads therefore remain unchanged.

This makes the default benchmark arguments an exact representation of the
requested workload:

```text
--group-size 0 --frame-size 16384 --fps 30
```

The existing producer test for zero group size will also verify that the group
contains one parseable object whose payload is exactly the configured size.

## Runner interface

The runner will live at `rs/moq-trace/scripts/relay_latency.py`. It will use PEP
723 inline script metadata for its Matplotlib dependency and will be runnable
with:

```bash
uv run rs/moq-trace/scripts/relay_latency.py
```

The relay is unpinned by default. Passing `--relay-cpu N` prepends
`taskset -c N` to the relay command. The publisher and subscriber remain
unpinned so they do not consume the measured relay CPU deliberately.

The command line will expose:

- `--relay-cpu`;
- `--subscribers`;
- `--duration`;
- `--warmup`;
- `--cooldown`;
- `--fps`;
- `--object-size`;
- `--output`;
- `--port`;
- `--skip-build`;
- `--debug-build`;
- `--relay-bin`;
- `--bench-bin`.

Release builds are the default. Unless `--skip-build` is used, the runner builds
`moq-relay` with the `trace` feature and builds `moq-bench`. Binary overrides
allow analysis against custom local builds without changing the script.

The runner will validate that Linux CPU affinity is available when
`--relay-cpu` is set, that the selected CPU belongs to the process's allowed
affinity set, and that both binaries accept `moq-transport-19`. Subscriber
count, object size, and fps must all be positive integers. The subscriber
process uses one `moq-bench` connection per requested subscriber, with every
connection subscribing to the publisher's broadcast.

## Process lifecycle

The runner will:

1. create a unique output directory;
2. build the required binaries;
3. launch the relay with full object, packet, and socket tracing;
4. wait until the relay log reports that it is listening;
5. launch the publisher and wait until its log reports a connection;
6. launch the subscriber process with the configured connection count;
7. wait for the configured benchmark duration;
8. verify successful publisher and subscriber exit statuses;
9. send the relay an interrupt and wait for graceful trace flushing;
10. parse and validate the completed trace.

Startup uses log conditions with bounded timeouts rather than fixed sleeps.
Every child runs in its own process group. Error handling terminates remaining
children, preserves their logs, and reports the output directory for diagnosis.

Both client and server commands force `moq-transport-19`. The runner verifies
the relay's negotiated-version log before analyzing results.

## Data analysis

The experiment is intentionally limited to one publisher and one track, with a
configurable number of subscribers. Completed inbound and outbound objects are
grouped by group ID and object ID after filtering for the configured payload
size. Each logical object must have exactly one inbound trace and exactly one
outbound trace per configured subscriber.

Object events do not currently carry a stable subscriber session identity. The
analyzer therefore treats the outbound traces for a logical object as an
unordered set of forwarded copies. It records deterministic copy ordinals after sorting each boundary type by
timestamp. An ordinal is scoped to one logical object and one metric boundary; it
is not joined across metrics or presented as a stable subscriber identity.

For each outbound boundary of a matched object, the runner records independent
long-form metric samples:

- forwarding start delay: inbound object start to outbound object start;
- model handoff delay: inbound create completion to outbound clone start;
- drain gap: inbound object completion to outbound object completion;
- full relay span: inbound object start to outbound object completion.

Warm-up and cool-down are removed using the inbound object's timestamp relative
to the matched live object window. The analyzer rejects an empty steady-state
window, noncontiguous group sequences, a fanout cardinality other than the
configured subscriber count, unsuccessful packet outcomes, and mismatched
packet or socket start/end counts. Malformed final records are treated as errors
because the runner shuts the relay down gracefully.

The runner computes count, mean, p50, p95, p99, and maximum for every latency
metric.

## Outputs

Each run directory contains:

- `relay.jsonl`;
- `relay.log`;
- `publisher.log`;
- `subscriber.log`;
- `objects.csv`;
- `summary.json`;
- `latency.png`.

`summary.json` includes the full command configuration, CPU affinity mode,
binary paths, protocol version, subscriber count, object size, fps, trace
validation counts, and latency statistics.

The Matplotlib report uses the noninteractive `Agg` backend and contains:

1. empirical cumulative distribution curves for the latency metrics;
2. grouped p50, p95, and p99 bars;
3. per-object latency over elapsed experiment time.

All plotted axes use milliseconds and identify whether the relay was pinned or
unpinned. Multi-subscriber runs plot the aggregate distribution across every
forwarded copy.

## Testing

Rust coverage will extend the zero-group-size producer test to prove the padded
header is exactly the configured size and remains parseable JSON.

Python unit tests will use standard-library `unittest` and synthetic JSONL
fixtures. They will cover:

- pinned and unpinned command construction;
- rejection of a CPU outside the allowed affinity set;
- positive subscriber-count, object-size, and fps validation;
- one-to-many inbound/outbound object matching;
- fanout-cardinality mismatch rejection;
- warm-up and cool-down trimming;
- percentile calculations;
- incomplete and malformed trace rejection;
- protocol and trace-count validation;
- CSV and JSON artifact generation.

A short local smoke run will verify process orchestration and generation of a
nonempty PNG using `moq-transport-19`. The repository's focused Rust tests and
Python tests will run before the smoke experiment.

## Non-goals

- Running publisher and subscriber on remote hosts.
- Sweeping multiple CPU counts or workload rates automatically.
- Comparing multiple runs in one chart.
- Measuring client-side end-to-end wall-clock latency.
- Supporting more than one publisher or track in trace matching.
- Stable per-subscriber labels or per-subscriber time series.
