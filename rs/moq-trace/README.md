# moq-trace

`moq-trace` is the Rust instrumentation facade for MoQ relay and Quinn latency
measurements. It has no command-line interface and performs no analysis. The
native provider lives in `moq-trace-lttng-sys`; experiment orchestration,
analysis, and plotting live in `tools/moq-trace`.

The split keeps three concerns independent:

- `rs/moq-trace` defines typed, scoped instrumentation.
- `rs/moq-trace-lttng-sys` owns LTTng-UST code generation and native linking.
- `tools/moq-trace` captures workloads and turns CTF into a DuckDB artifact.

## Instrumentation

The crate is a zero-work facade unless its `lttng` feature is enabled on Linux.
`moq-net` enables that feature through its `trace` feature, so build the
experiment binaries with:

```sh
nix develop --command just trace-build
```

There is no installation call. `moq_trace::global()` initializes the provider
on first use and returns a disabled handle when the backend is unavailable.
Scoped packet, object, phase, and socket tokens record `abandoned` when dropped
without an explicit terminal outcome.

The provider emits:

- `moq_object_start`, `moq_object_phase`, and `moq_object_end`
- `quic_packet_start`, `quic_packet_phase`, and `quic_packet_end`
- `quic_stream_frame`
- `udp_socket_start` and `udp_socket_end`

Object and packet phases use process-unique span IDs, allowing overlapping
occurrences to be paired exactly. Every lifecycle completion includes an
explicit outcome. Successful object correlation uses the structured logical
object identity plus transport stream ranges. It never infers identity from
event timing.

Transport ranges are half-open:

```text
[stream_offset_start, stream_offset_end)
```

Packet correlation requires a matching direction, connection ID, stream ID,
and non-empty overlap with a STREAM frame range. The analyzer rejects missing
or incomplete coverage.

The Quinn instrumentation is maintained in the pinned fork described in
[QUINN.md](QUINN.md).

## Experiment tool

Use the Nix package or run the tool from the development shell:

```sh
nix run .#moq-trace -- --help
nix develop --command moq-trace --help
```

The tool consumes validated TOML rather than a growing set of experiment flags.
It runs already-built binaries and never patches source code or invokes Cargo.

One local experiment:

```toml
output = "target/moq-trace/baseline"
relay_bin = "target/release/moq-relay"
bench_bin = "target/release/moq-bench"
relay_cpu = 2
subscribers = 8
object_size = 16384
fps = 30
warmup_seconds = 1
duration_seconds = 20
cooldown_seconds = 1
```

```sh
moq-trace run experiment.toml
```

For a remote subscriber, add an externally reachable relay URL and host:

```toml
relay_url = "https://192.0.2.10:4443"

[subscriber]
ssh = "user@192.0.2.20"
workdir = "/home/user/moq"
binary = "target/release/moq-bench"
```

The remote host needs the matching `moq-bench` binary and non-interactive SSH
authentication. LTTng and the analysis tool remain on the relay host.

Comparison configuration wraps one experiment and changes exactly one
dimension:

```toml
dimension = "subscribers"
values = [1, 8, 32]

[experiment]
output = "target/moq-trace/subscribers"
relay_bin = "target/release/moq-relay"
bench_bin = "target/release/moq-bench"
object_size = 16384
fps = 30
duration_seconds = 20
```

```sh
moq-trace compare comparison.toml
```

Valid comparison dimensions are `subscribers` and `object_size`.

## Analysis artifact

Analyze an existing CTF trace directly:

```sh
moq-trace analyze relay.ctf \
  --output target/moq-trace/analysis.duckdb \
  --object-size 16384 \
  --subscribers 8
```

The analyzer atomically publishes one DuckDB database and refuses to overwrite
it. The database is the authoritative derived artifact. It contains raw typed
events, validated lifecycles, correlated samples, run metadata, metric
statistics, and representative timeline rows. Durations remain integer
nanoseconds in storage; conversion to display units happens only at
presentation boundaries.

The artifact records metric populations. `analysis_window` stores the inclusive
object-start bounds and time origin in nanoseconds. Packet statistics include the distinct packets
in the coverage prefixes of selected inbound objects and their outbound copies,
including packets crossing the window boundaries. Object statistics measure
individual copies; representative timelines select by the slowest copy of each
object. All raw packets remain available in `packet_lifecycles` for capture-wide
queries.

Query it directly:

```sql
SELECT d.label,
       count(*) AS samples,
       quantile_cont(s.latency_ns, 0.99) / 1000 AS p99_us
FROM latency_samples AS s
JOIN metric_definitions AS d USING (domain, metric)
GROUP BY d.display_order, d.label
ORDER BY d.display_order;
```

Run directories additionally contain process logs, native CTF, and figures
under `plots/`. Comparison directories contain `comparison.duckdb`, one run
directory per value, and `plots/comparison_cdf.png`. Regenerate figures without
rerunning a workload by passing the authoritative database:

```sh
moq-trace plot target/moq-trace/baseline/analysis.duckdb
```

The main correlated metrics are:

```text
full_span = TX object end - RX object start

quic_forward_start = first outbound covering packet end
                   - first inbound covering packet start

quic_tail_gap = first complete outbound coverage end
              - first complete inbound coverage end

quic_full_span = first complete outbound coverage end
               - first inbound covering packet start
```

Raw CTF remains the authoritative capture. Monotonic timestamps are meaningful
for deltas within the recorded process, not as wall-clock timestamps across
hosts. The LTTng session records only the relay PID in discard mode, and the
analyzer rejects traces with discarded events.

## Verification

```sh
nix develop --command just trace check
nix develop --command cargo test -p moq-trace --all-features
nix develop --command cargo check -p moq-relay --features trace
```
