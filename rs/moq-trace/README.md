# MoQ tracing integration

This repository keeps the instrumentation calls in `moq-net`, `moq-relay`, and
the pinned Quinn fork. The event providers, Rust and C++ facades, capture tool,
and combined analysis live in the sibling `moq-trace2` repository during local
development.

The workspace resolves `moq-trace` from
`../moq-trace2/crates/moq-trace`. Once the toolkit has a published repository,
replace that bootstrap path with a pinned remote or registry dependency.

Build the instrumented relay and benchmark with:

```sh
nix develop --command just trace-build
```

The relay emits MoQ object events under `moq_trace:*` and transport events under
`quic_trace:*`. See [QUINN.md](QUINN.md) for the transport hook locations and
fork maintenance workflow.
