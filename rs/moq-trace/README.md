# moq-trace

Raw JSONL tracing for measuring MoQ relay processing overhead.

`moq-trace` is an opt-in Rust crate used by the relay to emit timestamped events
from two layers:

- MoQ object events from `moq-transport` publisher and subscriber group streams.
- QUIC packet events from the patched Quinn fork.

Tracing is disabled unless a trace output path is configured and the relevant
crates are built with the `trace` feature.

## Enabling

Build the relay with the `trace` feature:

```sh
cargo run -p moq-relay --features trace -- --trace-path /tmp/moq.trace.jsonl
```

The relay config also accepts the same settings through TOML, CLI flags, or
environment variables:

| TOML field | CLI flag | Environment variable | Default |
| --- | --- | --- | --- |
| `trace.path` | `--trace-path` | `MOQ_TRACE_PATH` | disabled |
| `trace.object_sample` | `--trace-object-sample` | `MOQ_TRACE_OBJECT_SAMPLE` | `1` |
| `trace.packet_sample` | `--trace-packet-sample` | `MOQ_TRACE_PACKET_SAMPLE` | `1` |
| `trace.queue_capacity` | `--trace-queue-capacity` | `MOQ_TRACE_QUEUE_CAPACITY` | `4096` |

If tracing is configured in a relay built without `--features trace`, startup
fails with a clear error instead of silently ignoring the setting.

## Output

The trace file is newline-delimited JSON. Each line is one event with a `type`
field:

- `moq_object_start`
- `moq_object_end`
- `moq_object_phase`
- `quic_packet_start`
- `quic_packet_end`
- `quic_packet_phase`

All timestamps are monotonic nanoseconds from the local process clock. They are
intended for latency deltas inside one process, not wall-clock comparison across
hosts.

Example object event:

```json
{"type":"moq_object_end","at_ns":123456789,"direction":"tx","protocol":"moq_transport","track_alias":7,"group_id":42,"object_id":3,"stream_offset_start":120,"stream_offset_end":520,"payload_bytes":400,"sample_rate":1}
```

Example object trace point event:

```json
{"type":"moq_object_phase","point":"tx_object_header_encoded","at_ns":123456900,"direction":"tx","protocol":"moq_transport","track_alias":7,"group_id":42,"object_id":3,"stream_offset_start":120,"stream_offset_end":144,"payload_bytes":400,"sample_rate":1}
```

Example packet event:

```json
{"type":"quic_packet_end","at_ns":123456999,"direction":"tx","packet_number":9901,"packet_space":"data","udp_len":1232,"stream_id":16,"stream_offset_start":120,"stream_offset_end":520,"sample_rate":1}
```

Example packet trace point event:

```json
{"type":"quic_packet_phase","point":"tx_packet_encrypted","at_ns":123457050,"direction":"tx","packet_number":9901,"packet_space":"data","udp_len":1232,"sample_rate":1}
```

## Event Semantics

MoQ object latency is measured from the first object header byte observed to the
final object payload byte read or written.

`moq_object_phase` records narrower moq-transport object trace points in a
`point` field. Rust variants are PascalCase, like
`ObjectTracePoint::RxObjectHeaderParsed`, and JSON values are snake case, like
`rx_object_header_parsed`. Inbound points are:

- `rx_object_header_parse_start`: entry to object header parsing after an object is present.
- `rx_object_header_parsed`: object header parsing completed.
- `rx_lookup_start`: entry to model lookup for the inbound object slot.
- `rx_lookup_done`: model lookup for the inbound object slot completed.
- `rx_object_create_start`: entry to inbound object creation.
- `rx_object_created`: inbound object creation completed.
- `rx_payload_read_start`: entry to an object payload chunk read.
- `rx_payload_read_done`: object payload chunk read completed.

Outbound points are:

- `tx_object_clone_start`: entry to outbound object selection or consumer creation.
- `tx_object_cloned`: outbound object selection or consumer creation completed.
- `tx_object_header_encode_start`: entry to object header encoding.
- `tx_object_header_encoded`: object header encoding completed.
- `tx_payload_write_start`: entry to an object payload chunk write.
- `tx_payload_write_done`: object payload chunk write completed.

QUIC packet latency is measured around packet handling in `quinn-proto`:

- Inbound `quic_packet_start` is emitted after receive and decrypt reaches a
  STREAM frame.
- Inbound `quic_packet_end` is emitted after the STREAM frame is handed to
  stream receive state.
- Outbound `quic_packet_start` is emitted when a STREAM frame is selected for a
  packet before encryption.
- Outbound `quic_packet_end` is emitted after packet build and encryption, when
  transmit is queued.

`quic_packet_phase` records narrower QUIC trace points in a `point` field.
Inbound points are:

- `rx_socket_io_start`: entry to UDP socket receive.
- `rx_socket_io_done`: UDP socket receive returned.
- `rx_packet_header_parse_start`: entry to QUIC packet header parsing.
- `rx_packet_header_parsed`: QUIC packet header parsing completed.
- `rx_packet_decrypt_start`: entry to QUIC header unprotect or packet body decrypt.
- `rx_packet_decrypted`: QUIC header unprotect or packet body decrypt completed.
- `rx_stream_frame_process_start`: entry to STREAM frame delivery into receive state.
- `rx_stream_frame_processed`: STREAM frame delivery into receive state completed.

Outbound points are:

- `tx_packet_encode_start`: entry to QUIC packet frame encoding.
- `tx_packet_encoded`: QUIC packet frame encoding completed.
- `tx_packet_encrypt_start`: entry to QUIC packet body encryption and header protection.
- `tx_packet_encrypted`: QUIC packet body encryption and header protection completed.
- `tx_socket_io_start`: entry to UDP socket send.
- `tx_socket_io_done`: UDP socket send returned.

Socket scoped points omit `packet_space` because UDP reads and writes are not tied
to one packet number space. True start points may omit
`udp_len` when the final datagram or packet length is not known yet. Packet
numbers, stream IDs, and stream byte ranges are included when the hook has that
metadata.

MoQ object identity is:

```text
(session_id, track_alias, group_id, object_id)
```

Packet mapping fields are:

```text
(session_id, stream_id, stream_offset_start, stream_offset_end)
```

In the current Rust transport stack, MoQ object hooks record stream byte ranges
but only include `stream_id` when the backend exposes it. The QUIC packet hooks
record STREAM frame stream IDs and byte ranges. Offline tooling should join
objects to packets by matching direction, session, stream ID when present, and
overlapping stream byte ranges.

## Sampling And Backpressure

`object_sample` and `packet_sample` emit every Nth event for their layer. Emitted
events include the active `sample_rate`.

Events are sent to a bounded queue and written by a background thread. When the
queue is full or closed, new events are dropped and counted by `Handle::dropped`.
Instrumentation paths never block on disk I/O.

## Quinn Patch

The relay uses a patched Quinn fork for packet-level hooks. The root workspace
patches crates.io `quinn` and `quinn-proto` to one pinned fork revision, and
patches crates.io `moq-trace` back to this workspace so Quinn and MoQ share the
same process-global trace handle.

Fork details, upstream base commits, and refresh instructions live in
[QUINN.md](QUINN.md).

Keep instrumentation changes isolated and marked with `MoQ trace hook` comments
so future upstream refreshes can review and reapply the patch.

## Verification

Useful checks while changing this crate or the hooks:

```sh
cargo test -p moq-trace
cargo test -p moq-net --features trace
cargo test -p moq-relay
cargo test -p moq-relay --features trace
cargo test -p moq-native --features quinn,trace
nix develop /home/siyuan/moq-trace --command cargo test -p quinn-proto --features moq-trace --config 'patch.crates-io.moq-trace.path="/home/siyuan/moq-trace/rs/moq-trace"'
```

Before landing a relay tracing change, run the repository check:

```sh
nix develop --command just check
```
