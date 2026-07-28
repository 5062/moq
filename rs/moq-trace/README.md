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
cargo run -p moq-relay --features trace -- --trace-path /tmp/moq.trace.jsonl
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

## Output

The output is newline-delimited JSON. Every line has a `type` field. Monotonic
`timestamp_ns` timestamps are useful for latency deltas inside one process, not for
wall-clock comparison between hosts.

The record types are:

- `moq_object_start`, `moq_object_phase`, and `moq_object_end`
- `quic_packet_start`, `quic_packet_phase`, and `quic_packet_end`
- `quic_stream_frame`
- `udp_socket_start` and `udp_socket_end`

Example packet lifecycle:

```json
{"type":"quic_packet_start","timestamp_ns":123456700,"trace_id":17,"connection_id":42,"direction":"tx","packet_number":9901,"packet_space":"data","sample_rate":1}
{"type":"quic_packet_phase","timestamp_ns":123456710,"trace_id":17,"connection_id":42,"direction":"tx","packet_number":9901,"packet_space":"data","byte_len":1232,"sample_rate":1,"phase":"packet_encrypt","edge":"start"}
{"type":"quic_packet_phase","timestamp_ns":123456760,"trace_id":17,"connection_id":42,"direction":"tx","packet_number":9901,"packet_space":"data","byte_len":1232,"sample_rate":1,"phase":"packet_encrypt","edge":"done","outcome":"success"}
{"type":"quic_stream_frame","timestamp_ns":123456770,"trace_id":17,"connection_id":42,"direction":"tx","packet_number":9901,"packet_space":"data","byte_len":1232,"sample_rate":1,"stream_id":16,"offset_start":120,"offset_end":520,"outcome":"success"}
{"type":"quic_packet_end","timestamp_ns":123456780,"trace_id":17,"connection_id":42,"direction":"tx","packet_number":9901,"packet_space":"data","byte_len":1232,"sample_rate":1,"outcome":"success"}
```

Example receive socket operation with GRO:

```json
{"type":"udp_socket_start","timestamp_ns":123456800,"trace_id":18,"direction":"rx","sample_rate":1}
{"type":"udp_socket_end","timestamp_ns":123456850,"trace_id":18,"direction":"rx","sample_rate":1,"outcome":"success","buffers":2,"datagrams":5,"bytes":6144}
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

MoQ object phases retain the existing `point` field. Object identity is:

```text
(session_id, track_alias, group_id, object_id)
```

Offline tooling can correlate objects with `quic_stream_frame` records by
connection or session, direction, stream ID when available, and overlapping
stream byte ranges.

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
