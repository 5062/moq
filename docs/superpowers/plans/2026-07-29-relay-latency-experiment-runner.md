# Relay Latency Experiment Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a configurable Python runner that executes a local MoQ relay latency experiment, optionally pins the relay to one CPU, validates full trace output, and produces CSV, JSON, and Matplotlib artifacts.

**Architecture:** Make `moq-bench` express the exact one-object workload by safely padding its lone JSON header. Add graceful trace flushing to relay shutdown. Keep the experiment runner in one importable Python script with pure command-building and trace-analysis functions around a thin subprocess orchestration layer.

**Tech Stack:** Rust 2024, Tokio, Python 3.10+, standard-library `argparse`/`subprocess`/`csv`/`json`/`unittest`, Matplotlib through PEP 723 and `uv`, Linux `taskset`.

## Global Constraints

- Force `moq-transport-19` on the publisher, subscriber, and relay.
- Default to one publisher, one subscriber, one 16 KiB object per group, and 30 fps.
- Make subscriber count, object size, fps, and relay CPU affinity configurable.
- Leave the publisher and subscriber processes unpinned.
- Use release binaries by default and provide an explicit debug-build option.
- Use no em dashes in source, comments, documentation, or commit messages.
- Preserve valid JSON when padding the benchmark header. Never truncate it.
- Reject ambiguous or incomplete traces instead of silently dropping samples.
- Do not label a metric sample as belonging to a stable subscriber because object events do not expose that identity.

---

## File Structure

- Modify `rs/moq-bench/src/connection.rs`: make a zero-data-object group contain one JSON object padded up to `frame_size`.
- Modify `rs/moq-relay/src/main.rs`: handle Ctrl-C and flush the trace writer before returning.
- Create `rs/moq-trace/scripts/relay_latency.py`: configuration, command construction, orchestration, trace analysis, artifact writing, and plotting.
- Create `rs/moq-trace/scripts/test_relay_latency.py`: standard-library unit tests with literal synthetic trace fixtures.
- Modify `rs/moq-trace/README.md`: document prerequisites, pinned and unpinned examples, outputs, and interpretation.

### Task 1: Make a lone benchmark object honor `frame_size`

**Files:**
- Modify: `rs/moq-bench/src/connection.rs`, `produce` and `produce_zero_group_size_is_keyframe_only`

**Interfaces:**
- Consumes: `Rolled.frame_size` and `Rolled.group_size`.
- Produces: when `group_size == 0`, one parseable JSON frame whose length is `max(serialized_header_len, frame_size)`.

- [ ] **Step 1: Strengthen the zero-group-size regression test**

Change `produce_zero_group_size_is_keyframe_only` to request a 1,024-byte frame and assert the actual payload:

```rust
let task = tokio::spawn(produce(0, "bench/test".into(), rolled(10, 1024, 0), track, stats.clone()));
tokio::time::advance(Duration::from_millis(250)).await;

let mut sub = consumer.track(TRACK).unwrap().subscribe(None).await.unwrap();
let mut group = sub.next_group().await.unwrap().expect("a group");
let payload = group.read_frame().await.unwrap().expect("keyframe").payload;

assert_eq!(payload.len(), 1024);
let header: serde_json::Value = serde_json::from_slice(&payload).unwrap();
assert_eq!(header["frame_size"], 1024);
assert_eq!(header["group_size"], 0);
assert!(group.read_frame().await.unwrap().is_none(), "no payload frames");
```

The production change that makes this test fail is removing the conditional header padding.

- [ ] **Step 2: Run the focused Rust test and observe the expected failure**

Run:

```bash
nix develop --command cargo test -p moq-bench connection::tests::produce_zero_group_size_is_keyframe_only -- --exact
```

Expected: failure because the serialized header is shorter than 1,024 bytes.

- [ ] **Step 3: Implement safe conditional padding**

Replace direct conversion of the JSON vector with:

```rust
let mut header = serde_json::to_vec(&header)?;
if rolled.group_size == 0 && header.len() < rolled.frame_size as usize {
	header.resize(rolled.frame_size as usize, b' ');
}
let header = Bytes::from(header);
```

This keeps `frame_size = 0` behavior unchanged and never truncates a header larger than the requested size.

- [ ] **Step 4: Run all `moq-bench` tests**

Run:

```bash
nix develop --command cargo test -p moq-bench
```

Expected: all tests pass.

- [ ] **Step 5: Commit the exact workload support**

```bash
git add rs/moq-bench/src/connection.rs
git commit -m "fix(bench): honor frame size for lone keyframes" \
  -m "Co-Authored-By: Codex <codex@openai.com>"
```

### Task 2: Flush relay traces on interrupt

**Files:**
- Modify: `rs/moq-relay/src/main.rs`

**Interfaces:**
- Consumes: the existing `moq_net::trace::Handle` created by `TraceConfig::build`.
- Produces: `run_until_shutdown<F, S>(trace, server, shutdown) -> anyhow::Result<()>`, which waits for server completion or a shutdown future and flushes every accepted trace record before returning.

- [ ] **Step 1: Add a failing shutdown-flush unit test**

Add a `#[cfg(test)]` module to `main.rs`. Use a real temporary trace writer and a ready shutdown future:

```rust
#[tokio::test]
async fn shutdown_flushes_trace_writer() {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("trace.jsonl");
	let trace = moq_trace::Handle::new(moq_trace::Config {
		path: Some(path.clone()),
		..moq_trace::Config::default()
	})
	.unwrap();
	let object = trace.object(moq_trace::ObjectContext::new(
		moq_trace::Direction::Tx,
		moq_trace::ObjectIdentity::new(1, 2, 3),
	));
	object.finish();

	run_until_shutdown(trace, std::future::pending::<anyhow::Result<()>>(), std::future::ready(())).await.unwrap();

	let output = std::fs::read_to_string(path).unwrap();
	assert_eq!(output.lines().count(), 2);
	assert!(output.ends_with('\n'));
}
```

Add `tempfile = "3"` under `rs/moq-relay/Cargo.toml` dev-dependencies only if it is not already present. It is already present in the current manifest.

The production change that makes this test fail is returning from shutdown without calling `Handle::flush`.

- [ ] **Step 2: Run the focused test and observe the expected compile failure**

Run:

```bash
nix develop --command cargo test -p moq-relay --features trace shutdown_flushes_trace_writer
```

Expected: compile failure because `run_until_shutdown` does not exist.

- [ ] **Step 3: Implement the shutdown helper**

Add:

```rust
fn flush_trace(trace: &moq_net::trace::Handle) -> bool {
	#[cfg(feature = "trace")]
	{
		trace.flush()
	}
	#[cfg(not(feature = "trace"))]
	{
		let _ = trace;
		true
	}
}

async fn run_until_shutdown<F, S>(trace: moq_net::trace::Handle, server: F, shutdown: S) -> anyhow::Result<()>
where
	F: std::future::Future<Output = anyhow::Result<()>>,
	S: std::future::Future<Output = ()>,
{
	tokio::pin!(server);
	tokio::pin!(shutdown);
	let result = tokio::select! {
		result = &mut server => result,
		() = &mut shutdown => Ok(()),
	};
	anyhow::ensure!(flush_trace(&trace), "failed to flush relay trace");
	result
}
```

Keep a trace clone for the helper:

```rust
let server = server.with_trace(trace.clone());
```

Move the existing server `tokio::select!` into an async block passed as `server`, and invoke:

```rust
run_until_shutdown(trace, server_run, tokio::signal::ctrl_c().map(|result| {
	if let Err(err) = result {
		tracing::warn!(%err, "failed to listen for interrupt");
	}
}))
.await
```

Import `futures::FutureExt` only if needed for `.map`.

- [ ] **Step 4: Run relay trace tests**

Run:

```bash
nix develop --command cargo test -p moq-relay --features trace
```

Expected: all relay tests pass, including a trace file ending on a complete newline.

- [ ] **Step 5: Commit graceful trace shutdown**

```bash
git add rs/moq-relay/src/main.rs
git commit -m "fix(relay): flush traces on interrupt" \
  -m "Co-Authored-By: Codex <codex@openai.com>"
```

### Task 3: Add configuration and exact command construction

**Files:**
- Create: `rs/moq-trace/scripts/relay_latency.py`
- Create: `rs/moq-trace/scripts/test_relay_latency.py`

**Interfaces:**
- Produces: `ExperimentConfig`, `validate_config`, `build_relay_command`, `build_publisher_command`, and `build_subscriber_command`.
- Consumed by: Tasks 4 through 6.

- [ ] **Step 1: Add command-construction tests**

Create the test module with an `importlib.util.spec_from_file_location` loader for the sibling script. Add literal assertions for:

```python
def test_relay_command_is_unpinned_by_default(self):
    config = self.config(relay_cpu=None)
    command = relay_latency.build_relay_command(config)
    self.assertEqual(command[0], str(config.relay_bin))
    self.assertNotIn("taskset", command)
    self.assertIn("moq-transport-19", command)

def test_relay_command_can_pin_one_cpu(self):
    config = self.config(relay_cpu=7)
    command = relay_latency.build_relay_command(config)
    self.assertEqual(command[:3], ["taskset", "-c", "7"])

def test_subscriber_command_uses_configured_fanout_and_workload(self):
    config = self.config(subscribers=4, fps=60, object_size=32768)
    command = relay_latency.build_subscriber_command(config)
    self.assertEqual(command[command.index("--connections") + 1], "4")
    self.assertEqual(command[command.index("--fps") + 1], "60")
    self.assertEqual(command[command.index("--frame-size") + 1], "32768")
    self.assertEqual(command[command.index("--group-size") + 1], "0")
    self.assertEqual(command[command.index("--client-version") + 1], "moq-transport-19")
```

Add validation tests asserting `ValueError` for zero subscribers, fps, object size, duration, or an unavailable relay CPU.

The production changes caught by these tests are a missing affinity prefix, the wrong fanout, a two-object group, and an invalid workload reaching subprocess launch.

- [ ] **Step 2: Run the Python tests and observe the expected import failure**

Run:

```bash
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
```

Expected: error because `relay_latency.py` does not exist.

- [ ] **Step 3: Add the script metadata, configuration, and builders**

Start the script with:

```python
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib"]
# ///
```

Define:

```python
PROTOCOL = "moq-transport-19"

@dataclasses.dataclass(frozen=True)
class ExperimentConfig:
    repo: pathlib.Path
    output: pathlib.Path
    relay_bin: pathlib.Path
    bench_bin: pathlib.Path
    relay_cpu: int | None = None
    subscribers: int = 1
    fps: int = 30
    object_size: int = 16 * 1024
    duration: float = 20.0
    warmup: float = 1.0
    cooldown: float = 1.0
    port: int = 4443
    release: bool = True
    skip_build: bool = False
```

Implement `validate_config(config: ExperimentConfig, allowed_cpus: set[int] | None = None) -> None` with explicit positive-value checks and affinity membership. Implement `build_relay_command`, `build_publisher_command`, and `build_subscriber_command` as pure functions returning argument lists. Build commands from literal flags, convert every numeric value with `str`, and prepend `["taskset", "-c", str(config.relay_cpu)]` only when relay affinity is configured.

The subscriber duration is `warmup + duration + cooldown`; the publisher has no duration and is interrupted after the subscriber exits.

- [ ] **Step 4: Run command and validation tests**

Run:

```bash
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
```

Expected: all Task 3 tests pass.

- [ ] **Step 5: Commit the configuration layer**

```bash
git add rs/moq-trace/scripts/relay_latency.py rs/moq-trace/scripts/test_relay_latency.py
git commit -m "feat(trace): configure relay latency experiments" \
  -m "Co-Authored-By: Codex <codex@openai.com>"
```

### Task 4: Parse and validate one-to-many trace samples

**Files:**
- Modify: `rs/moq-trace/scripts/relay_latency.py`
- Modify: `rs/moq-trace/scripts/test_relay_latency.py`

**Interfaces:**
- Produces: `MetricSample`, `Analysis`, `analyze_trace`, and `summarize`.
- Consumed by: artifact and plotting functions in Task 5.

- [ ] **Step 1: Add a literal two-subscriber trace fixture**

Write JSONL fixture records for two groups. Each group has:

- one RX object start;
- one RX create-done phase;
- one RX object end with the configured payload size;
- two TX object starts;
- two TX clone-start phases;
- two TX object ends;
- paired successful packet and socket start/end records with literal trace IDs.

Add:

```python
analysis = relay_latency.analyze_trace(path, subscribers=2, object_size=16384, warmup=0, cooldown=0)
self.assertEqual(len(analysis.samples["forward_start"]), 4)
self.assertEqual(
    [sample.latency_us for sample in analysis.samples["forward_start"]],
    [100.0, 120.0, 100.0, 120.0],
)
self.assertEqual(analysis.packet_count, 1)
self.assertEqual(analysis.socket_count, 1)
```

Derive the expected microseconds directly from literal nanosecond timestamps in the fixture.

- [ ] **Step 2: Add failure and trimming tests**

Add separate tests proving:

- one outbound copy with `subscribers=2` raises `TraceError`;
- a malformed JSON line raises `TraceError`;
- a failed packet outcome raises `TraceError`;
- mismatched packet start/end trace-ID sets raise `TraceError`;
- noncontiguous steady-state groups raise `TraceError`;
- warm-up and cool-down remove groups based on the RX start timestamp;
- `summarize([100, 200, 300, 400])` returns hand-derived mean, p50, p95, p99, and max values.

The production changes caught are silent fanout loss, accepting incomplete trace output, and computing percentiles from the wrong sample window.

- [ ] **Step 3: Run the new tests and observe missing-symbol failures**

Run:

```bash
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
```

Expected: errors because `analyze_trace`, `TraceError`, and `summarize` do not exist.

- [ ] **Step 4: Implement long-form metric analysis**

Define:

```python
@dataclasses.dataclass(frozen=True)
class MetricSample:
    group_id: int
    object_id: int
    metric: str
    copy_ordinal: int
    elapsed_ms: float
    latency_us: float

@dataclasses.dataclass(frozen=True)
class Analysis:
    samples: dict[str, list[MetricSample]]
    statistics: dict[str, dict[str, float | int]]
    packet_count: int
    socket_count: int
    group_count: int

class TraceError(RuntimeError):
    pass
```

Use logical key `(group_id, object_id)`. Keep boundary arrays independently:

```python
rx_starts[key]
rx_create_done[key]
rx_ends[key]
tx_starts[key]
tx_clone_starts[key]
tx_ends[key]
```

After payload filtering and window trimming, require one RX boundary and exactly `subscribers` entries in every TX boundary array. Sort each TX boundary array before assigning its boundary-local `copy_ordinal`. Compute forwarding and handoff samples from TX starts; compute drain-gap and full-span samples from TX ends. Do not join those ordinals across metrics.

Validate packet and socket start/end sets by `trace_id`, require successful packet outcomes, and reject malformed JSON.

Implement linear-interpolated percentiles equivalent to the manual experiment calculation:

```python
position = (len(values) - 1) * percentile
```

- [ ] **Step 5: Run the analyzer tests**

Run:

```bash
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
```

Expected: all Task 3 and Task 4 tests pass.

- [ ] **Step 6: Commit trace analysis**

```bash
git add rs/moq-trace/scripts/relay_latency.py rs/moq-trace/scripts/test_relay_latency.py
git commit -m "feat(trace): analyze relay fanout latency" \
  -m "Co-Authored-By: Codex <codex@openai.com>"
```

### Task 5: Write artifacts and render the Matplotlib report

**Files:**
- Modify: `rs/moq-trace/scripts/relay_latency.py`
- Modify: `rs/moq-trace/scripts/test_relay_latency.py`

**Interfaces:**
- Consumes: `ExperimentConfig` and `Analysis`.
- Produces: `write_csv`, `write_summary`, and `plot_analysis`.

- [ ] **Step 1: Add real artifact tests**

Using `tempfile.TemporaryDirectory`, call the real writers and assert:

```python
relay_latency.write_csv(output / "objects.csv", analysis)
relay_latency.write_summary(output / "summary.json", config, analysis, commands)
relay_latency.plot_analysis(output / "latency.png", config, analysis)

self.assertEqual(
    (output / "objects.csv").read_text().splitlines()[0],
    "group_id,object_id,metric,copy_ordinal,elapsed_ms,latency_us",
)
summary = json.loads((output / "summary.json").read_text())
self.assertEqual(summary["protocol"], "moq-transport-19")
self.assertEqual(summary["workload"]["subscribers"], 2)
self.assertGreater((output / "latency.png").stat().st_size, 1000)
```

The production changes caught are missing long-form fields, stale protocol metadata, omitted workload configuration, or an empty plot.

- [ ] **Step 2: Run the tests and observe missing-writer failures**

Run:

```bash
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
```

Expected: errors because the three artifact functions do not exist.

- [ ] **Step 3: Implement deterministic artifact writers**

Implement `write_csv(path: pathlib.Path, analysis: Analysis) -> None`, `write_summary(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis, commands: dict[str, list[str]]) -> None`, and `plot_analysis(path: pathlib.Path, config: ExperimentConfig, analysis: Analysis) -> None` with the behavior below.

Sort CSV rows by group, object, metric, and copy ordinal. Serialize JSON with `indent=2`, `sort_keys=True`, and a final newline.

Call `matplotlib.use("Agg")` before importing `pyplot`. Render one figure with:

1. ECDF curves built from sorted latency values;
2. grouped p50, p95, and p99 bars;
3. latency versus elapsed time for every metric.

Use milliseconds on chart axes by dividing stored microseconds by 1,000. Include `pinned CPU N` or `unpinned`, subscriber count, object size, fps, and `moq-transport-19` in the figure title.

- [ ] **Step 4: Run all Python tests and Ruff**

Run:

```bash
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
uv run --no-sync ruff check rs/moq-trace/scripts
uv run --no-sync ruff format --check rs/moq-trace/scripts
```

Expected: tests pass and Ruff reports no changes.

- [ ] **Step 5: Commit artifacts and plotting**

```bash
git add rs/moq-trace/scripts/relay_latency.py rs/moq-trace/scripts/test_relay_latency.py
git commit -m "feat(trace): visualize relay latency results" \
  -m "Co-Authored-By: Codex <codex@openai.com>"
```

### Task 6: Orchestrate the experiment and document usage

**Files:**
- Modify: `rs/moq-trace/scripts/relay_latency.py`
- Modify: `rs/moq-trace/scripts/test_relay_latency.py`
- Modify: `rs/moq-trace/README.md`

**Interfaces:**
- Consumes: all prior configuration, analysis, and artifact interfaces.
- Produces: `run_experiment(config) -> pathlib.Path` and executable `main() -> int`.

- [ ] **Step 1: Add process-boundary tests**

Add tests for real local helpers without mocking subprocess behavior:

- `wait_for_log` returns when a temporary log already contains the requested expression;
- `wait_for_log` raises `ExperimentError` when a supplied child process has exited;
- `validate_protocol` accepts a temporary executable that exits zero for `--help` and rejects one that exits nonzero;
- `default_output` creates a UTC timestamped path below `target/moq-trace`.

The production changes caught are waiting forever after child failure, skipping version validation, and scattering results outside the repository target directory.

- [ ] **Step 2: Run the tests and observe missing-orchestration failures**

Run:

```bash
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
```

Expected: errors because the orchestration helpers do not exist.

- [ ] **Step 3: Implement build and process management**

Define `ExperimentError(RuntimeError)`, `wait_for_log(path: pathlib.Path, pattern: re.Pattern[str], process: subprocess.Popen[bytes], timeout: float) -> None`, `validate_protocol(binary: pathlib.Path, flag: str) -> None`, `run_experiment(config: ExperimentConfig) -> pathlib.Path`, and `main() -> int`. `wait_for_log` must poll both the growing file and `process.poll()` against a `time.monotonic()` deadline. `validate_protocol` must execute the binary with the provided version flag plus `--help` and require exit status zero. `main` must parse arguments, call `run_experiment`, print errors to stderr, and return one on failure.

Build with:

```text
cargo build [--release] -p moq-relay --features trace -p moq-bench
```

Launch each process with `start_new_session=True` and a dedicated binary log file. Wait for:

- relay: `listening`;
- publisher: `connected`;
- subscriber: `connections=<subscribers>` and `subscriptions=<subscribers>`.

The subscriber duration is the configured warm-up, measurement, and cool-down sum. After it exits successfully, send SIGINT to the publisher process group and then the relay process group. Require both to exit within a bounded timeout, escalating to SIGKILL only during error cleanup. Analyze only after the relay exits successfully and flushes its trace.

Print the statistics table and absolute artifact paths. Return nonzero on any build, startup, workload, trace, or plotting failure while preserving the run directory.

- [ ] **Step 4: Add CLI parsing**

Use `argparse` defaults from `ExperimentConfig`. Map:

```text
--relay-cpu N
--subscribers 1
--fps 30
--object-size 16384
--duration 20
--warmup 1
--cooldown 1
--port 4443
--output PATH
--skip-build
--debug-build
--relay-bin PATH
--bench-bin PATH
```

Resolve the repository root from the script path, not the caller's current directory. Use `os.sched_getaffinity(0)` when CPU pinning is requested.

- [ ] **Step 5: Document runnable examples**

Add a `Latency experiment` section to `rs/moq-trace/README.md`:

```bash
# Unpinned, default workload.
nix develop --command uv run rs/moq-trace/scripts/relay_latency.py

# Pin only the relay to logical CPU 2 and increase fanout.
nix develop --command uv run rs/moq-trace/scripts/relay_latency.py \
  --relay-cpu 2 \
  --subscribers 8 \
  --object-size 32768 \
  --fps 60
```

Document `objects.csv` as long-form metric samples whose copy ordinal is local to one boundary type, not a subscriber ID.

- [ ] **Step 6: Run the full focused verification**

Run:

```bash
nix develop --command cargo test -p moq-bench
nix develop --command cargo test -p moq-relay --features trace
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
uv run --no-sync ruff check rs/moq-trace/scripts
uv run --no-sync ruff format --check rs/moq-trace/scripts
```

Expected: all commands exit zero.

- [ ] **Step 7: Run a short real smoke experiment**

Select an allowed CPU from `taskset -pc $$`, then run:

```bash
nix develop --command uv run rs/moq-trace/scripts/relay_latency.py \
  --relay-cpu 2 \
  --subscribers 2 \
  --fps 30 \
  --object-size 16384 \
  --warmup 1 \
  --duration 3 \
  --cooldown 1
```

Expected:

- relay log negotiates `moq-transport-19` for all three connections;
- steady-state fanout is exactly two outbound copies per inbound object;
- packet and socket trace start/end IDs pair exactly;
- `relay.jsonl` ends with a newline;
- `objects.csv`, `summary.json`, and `latency.png` are nonempty;
- Matplotlib can decode the generated PNG.

If CPU 2 is not in the allowed affinity set, substitute an allowed CPU shown by `taskset -pc $$`.

- [ ] **Step 8: Run the repository formatter and focused checks again**

Run:

```bash
nix develop --command just fix
nix develop --command cargo test -p moq-bench
nix develop --command cargo test -p moq-relay --features trace
uv run --with matplotlib python -m unittest rs/moq-trace/scripts/test_relay_latency.py -v
```

Expected: formatting leaves the intended files clean and every focused test passes.

- [ ] **Step 9: Commit orchestration and documentation**

```bash
git add rs/moq-trace/scripts/relay_latency.py \
  rs/moq-trace/scripts/test_relay_latency.py \
  rs/moq-trace/README.md
git commit -m "feat(trace): automate relay latency experiments" \
  -m "Co-Authored-By: Codex <codex@openai.com>"
```

### Task 7: Final scope and regression verification

**Files:**
- Verify all files changed in Tasks 1 through 6.

**Interfaces:**
- Consumes: the complete implementation.
- Produces: verified working tree and user-facing artifact paths.

- [ ] **Step 1: Inspect the final diff and cross-package sync**

Run:

```bash
git diff HEAD~4 --check
git diff HEAD~4 --stat
git status --short
```

Confirm the change touches only `moq-bench`, relay shutdown, `moq-trace` scripts/tests, and `moq-trace` documentation. No wire format changes occur, so IETF drafts and JS packages do not need updates.

- [ ] **Step 2: Run the complete relevant check**

Run:

```bash
nix develop --command just check
```

Expected: repository checks exit zero. If the full check exposes an unrelated pre-existing failure, record its exact command and output separately while preserving the focused passing evidence.

- [ ] **Step 3: Verify generated artifacts visually**

Open the smoke run's `latency.png` and confirm:

- all three panels render;
- titles show `moq-transport-19`, workload, and pinned state;
- legends are readable;
- axes use milliseconds;
- no series is empty or clipped.

- [ ] **Step 4: Prepare the final handoff**

Report:

- exact commands used;
- focused and full check results;
- smoke experiment configuration;
- p50, p95, and p99 forwarding latency;
- absolute paths to `summary.json`, `objects.csv`, and `latency.png`;
- any retained caveats about tracing overhead or logical versus physical CPU affinity.
