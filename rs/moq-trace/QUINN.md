# Quinn Patch

MoQ relay tracing uses a patched Quinn fork instead of vendored Quinn sources.

Fork:

```text
https://github.com/5062/quinn
branch: moq-trace/quinn-0.11
rev: 4751ef1067eee423beace786aa361b99441f814e
```

Upstream base:

```text
quinn-proto-0.11.16
a96949f6cd257c665f544626af4e8ce668a40b30
```

That base contains `quinn` 0.11.11 and `quinn-proto` 0.11.16, matching the
crate versions this repository previously vendored. Cargo patches both crates to
the same fork revision so Quinn uses one coherent workspace source.

The fork declares an optional `moq-trace` dependency by version. This repository
patches crates.io `moq-trace` to the sibling tracing toolkit during local
development. MoQ and transport hooks therefore use the same process-global
facade and emit the toolkit's `moq_trace:*` and `quic_trace:*` providers.

The RX packet envelope starts before Quinn's initial protected-header parse.
`routing` covers the remainder of endpoint processing through the connection
channel send. `scheduling` covers the channel wait until the connection task
enters the datagram handler. It can overlap QUIC processing for earlier packets
queued to the same connection.

To update Quinn:

```sh
cd ~/quinn
git fetch upstream --tags
git switch moq-trace/quinn-0.11
git rebase <new-upstream-tag-or-commit>
# resolve MoQ trace hook conflicts, then run tests from the MoQ Nix shell
```

After committing and pushing the refreshed fork branch, update the locked
`quinn` and `quinn-proto` revisions with `cargo update`, then run the trace
checks.
