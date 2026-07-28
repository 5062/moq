# Quinn Patch

MoQ relay tracing uses a patched Quinn fork instead of vendored Quinn sources.

Fork:

```text
https://github.com/5062/quinn
branch: moq-trace-0.11
rev: 7d95828f86da0e8027c90251b3b0977cc445877b
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
patches crates.io `moq-trace` back to `rs/moq-trace` so Quinn and MoQ share the
same trace crate instance and the same process-global trace handle.

To update Quinn:

```sh
cd ~/quinn
git fetch upstream --tags
git switch moq-trace-0.11
git rebase <new-upstream-tag-or-commit>
# resolve MoQ trace hook conflicts, then run tests from the MoQ Nix shell
```

After committing and pushing the refreshed fork branch, update the `quinn` and
`quinn-proto` `rev` values in the root `Cargo.toml`, then refresh `Cargo.lock`
and run the trace checks.
