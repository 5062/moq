# moq-trace

Raw JSONL tracing for measuring MoQ relay and QUIC processing overhead.

`moq-trace` is an opt-in Rust crate with three independent trace scopes:

- MoQ objects processed by the relay.
- QUIC packets encoded or decoded by the patched Quinn fork.
- UDP socket operations performed by Quinn.

Tracing is disabled unless an output path is configured and the relevant crates
are built with the `trace` feature.

## Enabling

Build the relay with tracing enabled:

```sh
cargo run -p moq-relay --features trace -- \
  demo/relay/localhost.toml \
  --trace-path /tmp/moq.trace.jsonl
```

The relay accepts the same settings through TOML, CLI flags, or environment
variables:

| TOML field | CLI flag | Environment variable | Default |
| --- | --- | --- | --- |
| `trace.path` | `--trace-path` | `MOQ_TRACE_PATH` | disabled |
| `trace.object_sample` | `--trace-object-sample` | `MOQ_TRACE_OBJECT_SAMPLE` | `1` |
| `trace.packet_sample` | `--trace-packet-sample` | `MOQ_TRACE_PACKET_SAMPLE` | `1` |
| `trace.socket_sample` | `--trace-socket-sample` | `MOQ_TRACE_SOCKET_SAMPLE` | `1` |
| `trace.queue_capacity` | `--trace-queue-capacity` | `MOQ_TRACE_QUEUE_CAPACITY` | `4096` |

A relay built without `--features trace` rejects trace configuration at
startup instead of silently ignoring it.

The relay installs one trace destination during startup and retains it until
shutdown. MoQ, QUIC, and socket instrumentation all emit through that same
process-global destination. Session, connection, and object identifiers remain
per-event context, so the analyzer can separate sessions without splitting one
cross-layer lifecycle across multiple files.

## Output

The output is newline-delimited JSON. Every line has a `type` field. The first
record is an exact schema and clock header. Readers reject any other revision.
Monotonic `timestamp_ns` timestamps are useful for latency deltas inside one
process, not for wall-clock comparison between hosts.

The record types are:

- `trace_header`
- `moq_object_start`, `moq_object_phase`, and `moq_object_end`
- `quic_packet_start`, `quic_packet_phase`, and `quic_packet_end`
- `quic_stream_frame`
- `udp_socket_start` and `udp_socket_end`

Example packet lifecycle:

```json
{"type":"trace_header","revision":1,"clock":"monotonic_ns"}
{"type":"quic_packet_start","timestamp_ns":123456700,"trace_id":17,"connection_id":42,"direction":"tx","packet_number":9901,"packet_space":"data","sample_rate":1}
{"type":"quic_packet_phase","timestamp_ns":123456710,"trace_id":17,"phase":"packet_encrypt","edge":"start"}
{"type":"quic_packet_phase","timestamp_ns":123456760,"trace_id":17,"phase":"packet_encrypt","edge":"done","outcome":"success"}
{"type":"quic_stream_frame","timestamp_ns":123456770,"trace_id":17,"stream_id":16,"offset_start":120,"offset_end":520,"outcome":"success"}
{"type":"quic_packet_end","timestamp_ns":123456780,"trace_id":17,"packet_number":9901,"packet_space":"data","byte_len":1232,"outcome":"success"}
```

Example receive socket operation with GRO:

```json
{"type":"udp_socket_start","timestamp_ns":123456800,"trace_id":18,"direction":"rx","sample_rate":1}
{"type":"udp_socket_end","timestamp_ns":123456850,"trace_id":18,"outcome":"success","stats":{"buffers":2,"datagrams":5,"bytes":6144}}
```

## Event semantics

A socket trace represents one kernel I/O attempt. The end record reports an
explicit outcome and separate buffer, datagram, and byte counts. A GRO receive
can therefore report one buffer containing several datagrams without treating
the batch byte total as a packet length.

A packet trace represents one QUIC packet. Creating the scoped token emits one
start record. Consuming it emits one end record. Dropping an unfinished token
emits `abandoned`, so early returns do not leave open intervals.

Packet phases use a `phase` and `edge` pair. Done edges include an `outcome`.
The phase values are:

- `header_parse`
- `routing`
- `scheduling`
- `header_unprotect`
- `payload_decrypt`
- `frame_process`
- `frame_encode`
- `packet_encrypt`

Packet outcomes are `success`, `malformed`, `authentication_failed`, `dropped`,
and `abandoned`. Socket outcomes additionally distinguish `pending`,
`would_block`, `connection_reset`, and `error`.

A packet containing several STREAM frames still has one packet start and one
packet end. Each `quic_stream_frame` child shares the packet `trace_id` and
records its stream ID and exclusive byte range. Packet context can be enriched
after the start record when RX header processing discovers the number space or
packet number.

MoQ object phases follow the same scoped shape: a `phase` and `edge` pair, with an
`outcome` on done edges. The phase values are `header_parse`, `create`,
`payload_read`, `frame_commit`, `clone`, `header_encode`, and `payload_write`.
The `clone` phase measures creation of an outbound object representation from
relay storage. This relay shares reference-counted payload storage, while another
implementation may copy payload bytes during the same conceptual phase.
Object outcomes are `success`, `failed`, and `abandoned`. Each lifecycle has a
`trace_id`. Ingress and every outbound copy share a structured `logical_id`
containing a process-unique group instance and the frame ordinal within that
group. Fan-out is joined directly instead of inferred from timestamps. Wire
identity within one lifecycle is:

```text
(session_id, track_alias, group_id, object_id)
```

`session_id` is sequential trace metadata that distinguishes subscriber copies.
It is not a MoQ or QUIC wire identifier. `connection_id` is Quinn's
process-local stable connection identity. The WebTransport adapter also exposes
a transport stream ID and the underlying QUIC offset corresponding to
application offset zero. That base accounts for the HTTP/3 WebTransport stream
prefix.

Completed object records use half-open transport-coordinate ranges:

```text
[stream_offset_start, stream_offset_end)
```

Offline packet correlation requires the same direction, `connection_id`, and
`stream_id`, plus a non-empty overlap with the STREAM frame's half-open range.
The analyzer does not infer connections from session order or timestamps.

## Sampling and backpressure

`object_sample`, `packet_sample`, and `socket_sample` are independent. Sampling
is decided once when a scoped trace starts, so its start, phase, child, and end
records stay together. TX packets sample by packet number. RX packets sample by
decode order because their packet number is unavailable before header
unprotection.

Events enter a bounded queue with non-blocking `try_send`. A full or closed
queue increments `Handle::dropped`; instrumentation never waits for disk I/O.
`Handle::writer_failed` reports terminal serialization or I/O failure.
`Handle::flush` is an explicit writer barrier for consumers that need to read a
live trace file while cached handle clones still exist.

## Relay latency analysis

Install the workspace binary once, then run the local publisher, relay, and
subscriber experiment directly:

```sh
cargo install --path rs/moq-trace
moq-trace experiment
```

The flake also exposes the binary as `.#moq-trace` for Nix profile or shell
installation.

Rust owns workload orchestration, trace validation, packet correlation, metric
calculation, comparisons, summaries, and tabular artifacts. Python and
Matplotlib only render figures from the Rust-produced artifact bundle. Analyze
an existing trace directly with:

```sh
moq-trace analyze relay.jsonl \
  --output target/moq-trace/analysis \
  --object-size 16384 \
  --subscribers 1
```

The analyzer atomically publishes `objects.csv`, `quic_objects.csv`,
`quic_packets.csv`, and a `manifest.json` to a new output directory.
It refuses to replace an existing bundle.

To compare delivery-copy latency across subscriber counts, run each workload in
sequence with one shared build:

```sh
moq-trace experiment \
  --compare-subscribers 1,50,100
```

The comparison directory contains one `subscribers-N` run directory per count,
plus `per_copy_latency.csv`, `per_copy_latency_summary.json`, and
`per_copy_latency_cdf.png`. Each CDF sample is one outbound delivery copy, so
`n` scales with both logical objects and subscribers. The runner scales the
bounded trace queue to 4096 events per subscriber so connection bursts do not
drop lifecycle records during high-fanout runs.

To compare per-copy latency across object sizes, use binary size suffixes:

```sh
moq-trace experiment \
  --compare-object-sizes 16k,64k,256k
```

This writes one `object-size-BYTES` run directory per size, plus
`object_size_latency.csv`, `object_size_latency_summary.json`, and
`object_size_latency_cdf.png`. The subscriber count and frame rate remain fixed,
so larger objects also increase the offered byte rate.

To test uncommitted instrumentation in a local Quinn checkout, override the
workspace's pinned fork revision:

```sh
moq-trace experiment --quinn-path ~/quinn
```

The selected build command is stored in `summary.json`.

Plot generation can be skipped on a headless machine and repeated later without
rerunning the workload:

```sh
moq-trace experiment --no-plot
moq-trace plot target/moq-trace/RUN
```

Correlated object analysis requires `packet_sample = 1`, complete packet
lifecycles, transport identity on every completed object, and complete STREAM
frame coverage. Validation fails when any requirement is missing instead of
guessing a join.

Frames are ordered by packet completion. The analyzer accumulates their clipped
interval union and stops at the first packet completion that fully covers the
object. Later retransmissions do not extend the measured latency.

The MoQ baseline is:

```text
full_span = TX object end - RX object start
```

Each outbound subscriber copy also has three QUIC-inclusive metrics:

```text
quic_forward_start = first outbound covering packet end
                   - first inbound covering packet start

quic_tail_gap = first complete outbound coverage end
              - first complete inbound coverage end

quic_full_span = first complete outbound coverage end
               - first inbound covering packet start
```

`quic_full_span` includes inbound QUIC parsing, header unprotection, decryption,
frame processing, MoQ relay work, outbound frame encoding, encryption, and
header protection. It ends when Quinn completes the first packet set covering
the outbound object. It excludes UDP socket completion and peer acknowledgement.

Packet diagnostics report RX and TX packet spans, RX connection-processing spans,
and each successful packet phase occurrence. The RX connection-processing span
starts when scheduling finishes and ends when packet processing completes.
Completed packets with non-success QUIC outcomes are validated and
excluded because they cannot contribute STREAM data to object coverage. They are kept separate from object metrics because packet and
object work can overlap.

The experiment writes:

- `analysis/objects.csv`: MoQ `full_span` samples.
- `analysis/quic_objects.csv`: the three QUIC-inclusive metrics per subscriber copy.
- `analysis/quic_packets.csv`: packet-span and packet-phase samples.
- `analysis/manifest.json`: typed metadata consumed by the plotting layer.
- `summary.json`: workload metadata, counts, and all three statistics sections.
- `latency.png`: MoQ latency distributions, percentiles, and time series.
- `quic_latency.png`: QUIC-inclusive object plots.
- `packet_latency.png`: packet-span and packet-phase plots.
- `latency_cdf.png`: MoQ and QUIC-inclusive object latency empirical CDFs with
  p50 and p99 markers.
- `packet_latency_cdf.png`: separate RX and TX connection-processing empirical
  CDFs with packet-level sample counts, p50, and p99 markers.
- `object_timeline.png`: correlated QUIC packet phases and MoQ phases for the
  first-created and last-created subscriber sessions of representative objects.

Object timelines use the RX MoQ object start as zero. Correlated RX QUIC packet
work therefore appears at negative elapsed times, while TX QUIC work can extend
beyond the TX MoQ object end. On RX, `routing` spans from endpoint header parsing
completion through the connection channel handoff. `scheduling` spans from that
handoff until the connection task begins handling the datagram. This queue wait
never overlaps later phases of the same packet, but it can overlap processing of
earlier packets on the same connection.

## Quinn patch

The relay uses the `moq-trace/quinn-0.11` Quinn fork. Endpoints and connections
capture the process-global handle once at creation. Packet and socket hot paths
then use cached handles, avoiding repeated global registry locks.

The root workspace patches crates.io `quinn` and `quinn-proto` to the fork and
patches `moq-trace` back to this workspace. Fork details and refresh instructions
live in [QUINN.md](QUINN.md).

## Verification

Useful focused checks are:

```sh
cargo test -p moq-trace
cargo test -p moq-relay --features trace
cargo clippy -p moq-trace -p moq-relay --all-targets --features moq-relay/trace -- -D warnings
```

Run the repository check before landing a tracing change:

```sh
nix develop --command just check
```
