# Quinn Upstream

This directory vendors the quinn crates used by the relay tracing build.

- quinn: crates.io `quinn` 0.11.11
- quinn-proto: crates.io `quinn-proto` 0.11.16

The root workspace patches crates.io to these local paths. Do not use a git
submodule for this copy. Keep MoQ tracing changes isolated and mark them with
`MoQ trace hook` comments so upstream diffs stay reviewable.

To refresh, replace `quinn/` and `quinn-proto/` with the desired crates.io
sources, update this file, then reapply the local MoQ trace hooks.
