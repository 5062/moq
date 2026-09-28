# MoQ tracing integration

This repository keeps the instrumentation calls in `moq-net`, `moq-relay`, and
the pinned Quinn fork. The event providers, Rust and C++ facades, capture tool,
and combined analysis live in the
[moq-trace](https://github.com/5062/moq-trace) toolkit repository.

The workspace resolves `moq-trace` from that repository's `main` branch through
`[patch.crates-io]`, and `Cargo.lock` pins the revision. Run
`cargo update -p moq-trace` to pick up newer toolkit commits. To build against a
local toolkit checkout instead, pass
`--config 'patch.crates-io.moq-trace.path="../moq-trace2/crates/moq-trace"'`.
That rewrites `Cargo.lock` to the local path, so restore it before committing.
Once the toolkit ships a registry release, replace the git source with it.

Build the instrumented relay and benchmark with:

```sh
nix develop --command just trace-build
```

The relay emits MoQ object events under `moq_trace:*` and transport events under
`quic_trace:*`. See [QUINN.md](QUINN.md) for the transport hook locations and
fork maintenance workflow.
