//! Local relay latency experiment orchestration and reporting.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::num::{NonZeroU16, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::analysis::{self, Metric, Report, Sample, Statistics};

const PROTOCOL: &str = "moq-transport-19";

mod lttng;

/// Arguments for one experiment or workload comparison.
#[derive(Clone, Debug, ClapArgs)]
pub(crate) struct Args {
	/// Repository containing the experiment binaries and plotting script.
	#[arg(long, default_value = ".")]
	repo: PathBuf,

	/// Directory for logs, analysis artifacts, summaries, and plots.
	#[arg(long)]
	output: Option<PathBuf>,

	/// TOML file describing remote experiment topology.
	#[arg(long)]
	config: Option<PathBuf>,

	/// CPU on which only the relay process runs.
	#[arg(long)]
	relay_cpu: Option<usize>,

	/// Number of subscriber connections.
	#[arg(long, default_value = "1")]
	subscribers: NonZeroUsize,

	/// Compare per-copy latency across comma-separated subscriber counts.
	#[arg(long, value_delimiter = ',', conflicts_with = "compare_object_sizes")]
	compare_subscribers: Option<Vec<NonZeroUsize>>,

	/// Compare per-copy latency across comma-separated object sizes.
	#[arg(
		long,
		value_delimiter = ',',
		value_parser = parse_byte_size,
		conflicts_with = "compare_subscribers"
	)]
	compare_object_sizes: Option<Vec<NonZeroU64>>,

	/// Published frames per second.
	#[arg(long, default_value = "30")]
	fps: NonZeroU64,

	/// Object payload size in bytes.
	#[arg(long, default_value = "16384", value_parser = parse_byte_size)]
	object_size: NonZeroU64,

	/// Measured workload duration.
	#[arg(long, default_value = "20s", value_parser = humantime::parse_duration)]
	duration: Duration,

	/// Duration excluded from the beginning of the workload.
	#[arg(long, default_value = "1s", value_parser = humantime::parse_duration)]
	warmup: Duration,

	/// Duration excluded from the end of the workload.
	#[arg(long, default_value = "1s", value_parser = humantime::parse_duration)]
	cooldown: Duration,

	/// UDP port used by the local relay.
	#[arg(long, default_value = "4443")]
	port: NonZeroU16,

	/// Use debug binaries instead of release binaries.
	#[arg(long)]
	debug_build: bool,

	/// Reuse binaries already present under target.
	#[arg(long)]
	skip_build: bool,

	/// Override the relay binary path.
	#[arg(long)]
	relay_bin: Option<PathBuf>,

	/// Override the benchmark binary path.
	#[arg(long)]
	bench_bin: Option<PathBuf>,

	/// Build with quinn and quinn-proto from this local checkout.
	#[arg(long)]
	quinn_path: Option<PathBuf>,

	/// Produce analytical artifacts without rendering plots.
	#[arg(long)]
	no_plot: bool,

	/// Python interpreter used by the Matplotlib renderer.
	#[arg(long, default_value = "python3")]
	python: PathBuf,
}

/// Topology settings for the local relay, publisher, and subscriber workload.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
struct Topology {
	/// URL reachable by subscribers. Defaults to the local relay URL.
	relay_url: Option<String>,

	/// Host on which the subscriber workload runs.
	subscriber: SubscriberHost,
}

/// Execution settings for the subscriber workload.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
struct SubscriberHost {
	/// SSH destination, such as `user@subscriber.example.com`.
	ssh: Option<String>,

	/// Benchmark binary on the subscriber host. Defaults to `moq-bench`.
	binary: Option<String>,

	/// Working directory on the subscriber host.
	workdir: Option<String>,
}

impl Topology {
	fn load(path: Option<&Path>) -> Result<Self> {
		let Some(path) = path else {
			return Ok(Self::default());
		};
		let contents = std::fs::read_to_string(path)
			.with_context(|| format!("failed to read experiment config {}", path.display()))?;
		let topology: Self = toml::from_str(&contents)
			.with_context(|| format!("failed to parse experiment config {}", path.display()))?;
		topology
			.validate()
			.with_context(|| format!("invalid experiment config {}", path.display()))?;
		Ok(topology)
	}

	fn validate(&self) -> Result<()> {
		if let Some(ssh) = &self.subscriber.ssh {
			if self.relay_url.is_none() {
				bail!("subscriber.ssh ({ssh}) requires relay_url so the remote host can reach the relay");
			}
		} else if self.relay_url.is_some() || self.subscriber.binary.is_some() || self.subscriber.workdir.is_some() {
			bail!("remote subscriber settings require subscriber.ssh");
		}
		Ok(())
	}

	fn is_remote(&self) -> bool {
		self.subscriber.ssh.is_some()
	}
}

/// Arguments for rendering plots from an existing experiment directory.
#[derive(Clone, Debug, ClapArgs)]
pub(crate) struct PlotArgs {
	/// Experiment or comparison directory containing Rust-produced artifacts.
	input: PathBuf,

	/// Repository containing the plotting script.
	#[arg(long, default_value = ".")]
	repo: PathBuf,

	/// Python interpreter used by the Matplotlib renderer.
	#[arg(long, default_value = "python3")]
	python: PathBuf,
}

#[derive(Clone, Debug)]
struct Config {
	repo: PathBuf,
	output: PathBuf,
	relay_bin: PathBuf,
	bench_bin: PathBuf,
	quinn_path: Option<PathBuf>,
	relay_cpu: Option<usize>,
	subscribers: NonZeroUsize,
	fps: NonZeroU64,
	object_size: NonZeroU64,
	duration: Duration,
	warmup: Duration,
	cooldown: Duration,
	port: NonZeroU16,
	release: bool,
	skip_build: bool,
	plot: bool,
	python: PathBuf,
	topology: Topology,
}

#[derive(Debug)]
struct Commands {
	build: Vec<String>,
	relay: Vec<String>,
	publisher: Vec<String>,
	subscriber: Vec<String>,
}

#[derive(Debug)]
struct Run {
	output: PathBuf,
	report: Report,
}

struct Capture {
	ctf: PathBuf,
	relay_pid: u32,
}

#[derive(Clone, Copy, Debug)]
enum Dimension {
	Subscribers,
	ObjectSize,
}

impl Dimension {
	fn directory_prefix(self) -> &'static str {
		match self {
			Self::Subscribers => "subscribers",
			Self::ObjectSize => "object-size",
		}
	}

	fn value_column(self) -> &'static str {
		match self {
			Self::Subscribers => "subscribers",
			Self::ObjectSize => "object_size_bytes",
		}
	}

	fn values_key(self) -> &'static str {
		match self {
			Self::Subscribers => "subscriber_counts",
			Self::ObjectSize => "object_sizes_bytes",
		}
	}

	fn artifact_stem(self) -> &'static str {
		match self {
			Self::Subscribers => "per_copy_latency",
			Self::ObjectSize => "object_size_latency",
		}
	}
}

#[derive(Debug)]
struct ComparisonRow {
	value: u64,
	layer: &'static str,
	sample: Sample,
}

struct ManagedChild {
	child: Child,
	name: &'static str,
}

impl ManagedChild {
	fn spawn(command: &[String], cwd: &Path, log: &Path, name: &'static str) -> Result<Self> {
		let log = File::create(log).with_context(|| format!("failed to create {}", log.display()))?;
		let stderr = log.try_clone()?;
		let mut process = Command::new(&command[0]);
		process
			.args(&command[1..])
			.current_dir(cwd)
			.stdout(Stdio::from(log))
			.stderr(Stdio::from(stderr));
		let child = process
			.spawn()
			.with_context(|| format!("failed to start {name}: {}", display_command(command)))?;
		Ok(Self { child, name })
	}

	fn wait_until(&mut self, timeout: Duration) -> Result<ExitStatus> {
		let deadline = Instant::now() + timeout;
		loop {
			if let Some(status) = self.child.try_wait()? {
				return Ok(status);
			}
			if Instant::now() >= deadline {
				bail!(
					"{} did not finish within {}",
					self.name,
					humantime::format_duration(timeout)
				);
			}
			thread::sleep(Duration::from_millis(50));
		}
	}

	fn stop(mut self, graceful: bool) -> Result<()> {
		if let Some(status) = self.child.try_wait()? {
			return require_success(self.name, status);
		}
		let signal = if graceful { "-INT" } else { "-KILL" };
		if let Err(error) = signal_process(self.child.id(), signal) {
			if let Some(status) = self.child.try_wait()? {
				return require_success(self.name, status);
			}
			return Err(error);
		}
		let status = self.wait_until(if graceful {
			Duration::from_secs(10)
		} else {
			Duration::from_secs(2)
		})?;
		require_success(self.name, status)
	}
}

impl Drop for ManagedChild {
	fn drop(&mut self) {
		if matches!(self.child.try_wait(), Ok(None)) {
			if signal_process(self.child.id(), "-KILL").is_err() {
				let _ = self.child.kill();
			}
			let _ = self.child.wait();
		}
	}
}

pub(crate) fn run(args: Args) -> Result<()> {
	if args.duration.is_zero() {
		bail!("--duration must be greater than zero");
	}
	let comparison: Option<(Dimension, Vec<u64>)> = match (&args.compare_subscribers, &args.compare_object_sizes) {
		(Some(values), None) => Some((
			Dimension::Subscribers,
			values.iter().map(|value| value.get() as u64).collect::<Vec<_>>(),
		)),
		(None, Some(values)) => Some((
			Dimension::ObjectSize,
			values.iter().map(|value| value.get()).collect::<Vec<_>>(),
		)),
		(None, None) => None,
		(Some(_), Some(_)) => unreachable!("clap rejects conflicting comparison options"),
	};
	if let Some((_, values)) = &comparison {
		validate_comparison(values)?;
	}
	let config = Config::new(args)?;
	validate(&config)?;
	let output = if let Some((dimension, values)) = comparison {
		run_comparison(&config, dimension, &values)?
	} else {
		let run = run_one(&config)?;
		print_statistics("MoQ object metrics", &run.report.statistics);
		print_statistics("QUIC-inclusive object metrics", &run.report.quic_object_statistics);
		print_statistics("QUIC packet diagnostics", &run.report.packet_statistics);
		run.output
	};
	println!("run directory: {}", output.display());
	Ok(())
}

pub(crate) fn plot(args: PlotArgs) -> Result<()> {
	let repo = canonical_repo(&args.repo)?;
	render(&repo, &args.python, &args.input)
}

impl Config {
	fn new(args: Args) -> Result<Self> {
		let repo = canonical_repo(&args.repo)?;
		let profile = if args.debug_build { "debug" } else { "release" };
		let output = args.output.unwrap_or_else(|| default_output(&repo));
		let topology = Topology::load(args.config.as_deref())?;
		Ok(Self {
			relay_bin: args
				.relay_bin
				.unwrap_or_else(|| repo.join("target").join(profile).join("moq-relay")),
			bench_bin: args
				.bench_bin
				.unwrap_or_else(|| repo.join("target").join(profile).join("moq-bench")),
			repo,
			output,
			quinn_path: args.quinn_path,
			relay_cpu: args.relay_cpu,
			subscribers: args.subscribers,
			fps: args.fps,
			object_size: args.object_size,
			duration: args.duration,
			warmup: args.warmup,
			cooldown: args.cooldown,
			port: args.port,
			release: !args.debug_build,
			skip_build: args.skip_build,
			plot: !args.no_plot,
			python: args.python,
			topology,
		})
	}
}

fn canonical_repo(path: &Path) -> Result<PathBuf> {
	let path = path
		.canonicalize()
		.with_context(|| format!("failed to resolve repository {}", path.display()))?;
	if !path.join("Cargo.toml").is_file() {
		bail!("repository is missing {}", path.join("Cargo.toml").display());
	}
	Ok(path)
}

fn default_output(repo: &Path) -> PathBuf {
	let timestamp = humantime::format_rfc3339_seconds(SystemTime::now())
		.to_string()
		.replace(['-', ':'], "");
	repo.join("target").join("moq-trace").join(timestamp)
}

fn validate(config: &Config) -> Result<()> {
	if let Some(cpu) = config.relay_cpu {
		let status = Command::new("taskset")
			.args(["-c", &cpu.to_string(), "true"])
			.status()
			.context("failed to validate --relay-cpu with taskset")?;
		if !status.success() {
			bail!("relay CPU {cpu} is unavailable to this process");
		}
	}
	if let Some(path) = &config.quinn_path {
		if config.skip_build {
			bail!("--quinn-path cannot be combined with --skip-build");
		}
		for name in ["quinn", "quinn-proto"] {
			let manifest = path.join(name).join("Cargo.toml");
			if !manifest.is_file() {
				bail!("local Quinn checkout is missing {}", manifest.display());
			}
		}
	}
	Ok(())
}

fn run_one(config: &Config) -> Result<Run> {
	let output = config.output.canonicalize().unwrap_or_else(|_| {
		if config.output.is_absolute() {
			config.output.clone()
		} else {
			config.repo.join(&config.output)
		}
	});
	create_new_dir(&output, "run")?;
	let commands = Commands::new(config)?;
	build(config, &commands.build, &output)?;
	let capture = capture(config, &commands, &output)?;
	let report = analysis::run(
		analysis::Source {
			ctf: &capture.ctf,
			python: &config.python,
			expected_pid: Some(capture.relay_pid),
		},
		&output.join("analysis"),
		analysis::Options {
			object_size: config.object_size,
			subscribers: config.subscribers,
			warmup: config.warmup,
			cooldown: config.cooldown,
		},
	)
	.with_context(|| format!("failed to analyze {}", capture.ctf.display()))?;
	write_summary(&output.join("summary.json"), config, &commands, &report)?;
	if config.plot {
		render(&config.repo, &config.python, &output)?;
	}
	Ok(Run { output, report })
}

impl Commands {
	fn new(config: &Config) -> Result<Self> {
		Ok(Self {
			build: build_command(config)?,
			relay: relay_command(config),
			publisher: publisher_command(config),
			subscriber: subscriber_command(config)?,
		})
	}
}

fn relay_command(config: &Config) -> Vec<String> {
	let mut command = vec![
		config.relay_bin.display().to_string(),
		"--server-bind".into(),
		format!("[::]:{}", config.port),
		"--server-backend".into(),
		"quinn".into(),
		"--server-version".into(),
		PROTOCOL.into(),
		"--tls-generate".into(),
		"localhost".into(),
		"--auth-public".into(),
		String::new(),
	];
	if let Some(cpu) = config.relay_cpu {
		command.splice(0..0, ["taskset".into(), "-c".into(), cpu.to_string()]);
	}
	command
}

fn bench_command(config: &Config, url: &str, binary: &str) -> Vec<String> {
	vec![
		binary.into(),
		"--client-connect".into(),
		url.into(),
		"--client-backend".into(),
		"quinn".into(),
		"--client-version".into(),
		PROTOCOL.into(),
		"--client-tls-disable-verify".into(),
		"--startup".into(),
		"0s".into(),
		"--report".into(),
		"200ms".into(),
		"--fps".into(),
		config.fps.to_string(),
		"--frame-size".into(),
		config.object_size.to_string(),
		"--group-size".into(),
		"0".into(),
	]
}

fn local_relay_url(config: &Config) -> String {
	format!("https://localhost:{}", config.port)
}

fn publisher_command(config: &Config) -> Vec<String> {
	let mut command = bench_command(
		config,
		&local_relay_url(config),
		&config.bench_bin.display().to_string(),
	);
	command.extend([
		"--name".into(),
		"relay-latency".into(),
		"--connections".into(),
		"1".into(),
		"--broadcasts".into(),
		"1".into(),
		"--subscribe".into(),
		"0".into(),
	]);
	command
}

fn subscriber_command(config: &Config) -> Result<Vec<String>> {
	let local_binary = config.bench_bin.display().to_string();
	let binary = if config.topology.is_remote() {
		config.topology.subscriber.binary.as_deref().unwrap_or("moq-bench")
	} else {
		&local_binary
	};
	let url = config
		.topology
		.relay_url
		.as_deref()
		.map(ToOwned::to_owned)
		.unwrap_or_else(|| local_relay_url(config));
	let mut command = bench_command(config, &url, binary);
	command.extend([
		"--name".into(),
		"relay-latency-subscribers".into(),
		"--connections".into(),
		config.subscribers.to_string(),
		"--broadcasts".into(),
		"0".into(),
		"--subscribe".into(),
		"1".into(),
		"--duration".into(),
		humantime::format_duration(config.warmup + config.duration + config.cooldown).to_string(),
	]);
	if let Some(ssh) = &config.topology.subscriber.ssh {
		Ok(ssh_command(
			ssh,
			config.topology.subscriber.workdir.as_deref(),
			&command,
		))
	} else {
		Ok(command)
	}
}

fn ssh_command(target: &str, workdir: Option<&str>, command: &[String]) -> Vec<String> {
	let mut remote = String::new();
	if let Some(workdir) = workdir {
		remote.push_str("cd ");
		remote.push_str(&shell_quote(workdir));
		remote.push_str(" && ");
	}
	remote.push_str("exec ");
	remote.push_str(&shell_join(command));
	vec![
		"ssh".into(),
		"-T".into(),
		"-o".into(),
		"BatchMode=yes".into(),
		target.into(),
		remote,
	]
}

fn shell_join(values: &[String]) -> String {
	values
		.iter()
		.map(|value| shell_quote(value))
		.collect::<Vec<_>>()
		.join(" ")
}

fn shell_quote(value: &str) -> String {
	format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn build_command(config: &Config) -> Result<Vec<String>> {
	let mut command = vec!["cargo".into(), "build".into()];
	if config.release {
		command.push("--release".into());
	}
	command.extend([
		"-p".into(),
		"moq-relay".into(),
		"--features".into(),
		"trace".into(),
		"-p".into(),
		"moq-bench".into(),
	]);
	if let Some(root) = &config.quinn_path {
		for name in ["quinn", "quinn-proto"] {
			let path = root
				.join(name)
				.canonicalize()
				.with_context(|| format!("failed to resolve local {name}"))?;
			command.extend([
				"--config".into(),
				format!("patch.crates-io.{name}.path={}", serde_json::to_string(&path)?),
			]);
		}
	}
	Ok(command)
}

fn build(config: &Config, command: &[String], output: &Path) -> Result<()> {
	if config.skip_build {
		return Ok(());
	}
	let lock_path = config.repo.join("Cargo.lock");
	let lock = config
		.quinn_path
		.as_ref()
		.map(|_| std::fs::read(&lock_path))
		.transpose()?;
	let result = (|| {
		let log_path = output.join("build.log");
		let log = File::create(&log_path)?;
		let stderr = log.try_clone()?;
		Command::new(&command[0])
			.args(&command[1..])
			.current_dir(&config.repo)
			.stdout(Stdio::from(log))
			.stderr(Stdio::from(stderr))
			.status()
			.with_context(|| format!("failed to run {}", display_command(command)))
	})();
	if let Some(contents) = lock {
		std::fs::write(&lock_path, contents).with_context(|| format!("failed to restore {}", lock_path.display()))?;
	}
	let status = result?;
	if !status.success() {
		bail!(
			"build failed with status {status}; see {}",
			output.join("build.log").display()
		);
	}
	Ok(())
}

fn capture(config: &Config, commands: &Commands, output: &Path) -> Result<Capture> {
	let ctf = output.join("relay.ctf");
	let session = lttng::Session::create(&ctf)?;
	let mut relay = ManagedChild::spawn(&commands.relay, output, &output.join("relay.log"), "relay")?;
	wait_for_log(
		&output.join("relay.log"),
		&mut relay,
		Duration::from_secs(15),
		|contents| contents.contains("listening"),
		"listening",
	)?;
	session.wait_for_provider(relay.child.id(), Duration::from_secs(10))?;
	let relay_pid = relay.child.id();
	session.start(relay_pid)?;
	let mut subscriber = ManagedChild::spawn(
		&commands.subscriber,
		output,
		&output.join("subscriber.log"),
		"subscriber",
	)?;
	let connections = format!("connections={}", config.subscribers);
	wait_for_log(
		&output.join("subscriber.log"),
		&mut subscriber,
		Duration::from_secs(20),
		|contents| contents.contains(&connections),
		"subscriber connections",
	)?;
	let mut publisher = ManagedChild::spawn(&commands.publisher, output, &output.join("publisher.log"), "publisher")?;
	wait_for_log(
		&output.join("publisher.log"),
		&mut publisher,
		Duration::from_secs(15),
		|contents| contents.contains("connections=1"),
		"connections=1",
	)?;
	let subscriptions = format!("subscriptions={}", config.subscribers);
	wait_for_log(
		&output.join("subscriber.log"),
		&mut subscriber,
		Duration::from_secs(20),
		|contents| contents.contains(&connections) && contents.contains(&subscriptions),
		"subscriber connections and subscriptions",
	)?;
	let timeout = config.warmup + config.duration + config.cooldown + Duration::from_secs(15);
	require_success("subscriber", subscriber.wait_until(timeout)?)?;
	publisher.stop(true)?;
	relay.stop(true)?;
	session.finish()?;
	Ok(Capture { ctf, relay_pid })
}

fn wait_for_log(
	path: &Path,
	process: &mut ManagedChild,
	timeout: Duration,
	matches: impl Fn(&str) -> bool,
	description: &str,
) -> Result<()> {
	let deadline = Instant::now() + timeout;
	loop {
		let mut contents = String::new();
		if let Ok(mut file) = File::open(path) {
			file.read_to_string(&mut contents)?;
		}
		let contents = strip_ansi(&contents)?;
		if matches(&contents) {
			return Ok(());
		}
		if let Some(status) = process.child.try_wait()? {
			bail!(
				"{} exited with status {status} before log contained {description}",
				process.name
			);
		}
		if Instant::now() >= deadline {
			bail!(
				"timed out after {} waiting for {description} in {}",
				humantime::format_duration(timeout),
				path.display()
			);
		}
		thread::sleep(Duration::from_millis(50));
	}
}

fn strip_ansi(value: &str) -> Result<String> {
	String::from_utf8(strip_ansi_escapes::strip(value)).context("ANSI stripping produced invalid UTF-8")
}

#[cfg(unix)]
fn signal_process(pid: u32, signal: &str) -> Result<()> {
	let status = Command::new("kill")
		.args([signal, "--", &pid.to_string()])
		.status()
		.context("failed to invoke kill")?;
	if !status.success() {
		bail!("kill {signal} failed for process {pid}");
	}
	Ok(())
}

#[cfg(not(unix))]
fn signal_process(_pid: u32, _signal: &str) -> Result<()> {
	bail!("relay latency experiments require Unix process signaling")
}

fn require_success(name: &str, status: ExitStatus) -> Result<()> {
	if !status.success() {
		bail!("{name} exited with status {status}");
	}
	Ok(())
}

fn write_summary(path: &Path, config: &Config, commands: &Commands, report: &Report) -> Result<()> {
	let correlated_objects = report
		.object_samples
		.iter()
		.map(|sample| (sample.group_id, sample.object_id))
		.collect::<BTreeSet<_>>()
		.len();
	let correlated_copies = report
		.quic_object_samples
		.iter()
		.map(|sample| (sample.group_id, sample.object_id, sample.copy_ordinal))
		.collect::<BTreeSet<_>>()
		.len();
	let affinity = config.relay_cpu.map_or_else(
		|| json!({"mode": "unpinned"}),
		|cpu| json!({"mode": "single-core", "cpu": cpu}),
	);
	let value = json!({
		"protocol": PROTOCOL,
		"affinity": affinity,
		"workload": {
			"publishers": 1,
			"subscribers": config.subscribers,
			"objects_per_group": 1,
			"object_size": config.object_size,
			"fps": config.fps,
			"duration_seconds": config.duration.as_secs_f64(),
			"warmup_seconds": config.warmup.as_secs_f64(),
			"cooldown_seconds": config.cooldown.as_secs_f64(),
		},
		"binaries": {
			"relay": config.relay_bin,
			"bench": config.bench_bin,
		},
		"commands": {
			"build": commands.build,
			"relay": commands.relay,
			"publisher": commands.publisher,
			"subscriber": commands.subscriber,
		},
		"counts": {
			"groups": report.group_count,
			"packets": report.packet_count,
			"correlated_objects": correlated_objects,
			"correlated_object_copies": correlated_copies,
			"quic_object_samples": report.quic_object_samples.len(),
			"quic_packet_samples": report.packet_samples.len(),
		},
		"statistics_us": report.statistics,
		"quic_object_statistics_us": report.quic_object_statistics,
		"quic_packet_statistics_us": report.packet_statistics,
		"timeline_objects": report.timelines.iter().map(|timeline| json!({
			"statistic": timeline.selection.statistic,
			"target_us": timeline.selection.target_us,
			"group_id": timeline.selection.group_id,
			"object_id": timeline.selection.object_id,
			"actual_us": timeline.selection.actual_us,
			"copies": {
				"first": timeline.first_copy,
				"last": timeline.last_copy,
				"slowest": timeline.slowest_copy,
			},
		})).collect::<Vec<_>>(),
	});
	write_json(path, &value)
}

fn run_comparison(config: &Config, dimension: Dimension, values: &[u64]) -> Result<PathBuf> {
	let output = if config.output.is_absolute() {
		config.output.clone()
	} else {
		config.repo.join(&config.output)
	};
	create_new_dir(&output, "comparison")?;
	let mut runs = Vec::new();
	for (index, value) in values.iter().copied().enumerate() {
		let mut run_config = config.clone();
		run_config.output = output.join(format!("{}-{value}", dimension.directory_prefix()));
		run_config.skip_build = config.skip_build || index > 0;
		match dimension {
			Dimension::Subscribers => {
				run_config.subscribers = NonZeroUsize::new(usize::try_from(value)?)
					.ok_or_else(|| anyhow!("subscriber count must be positive"))?;
			}
			Dimension::ObjectSize => {
				run_config.object_size =
					NonZeroU64::new(value).ok_or_else(|| anyhow!("object size must be positive"))?;
			}
		}
		runs.push(run_one(&run_config)?);
	}
	write_comparison(&output, dimension, values, &runs)?;
	if config.plot {
		render(&config.repo, &config.python, &output)?;
	}
	Ok(output)
}

fn validate_comparison(values: &[u64]) -> Result<()> {
	if values.len() < 2 {
		bail!("a workload comparison requires at least two values");
	}
	if values.iter().collect::<BTreeSet<_>>().len() != values.len() {
		bail!("workload comparison values must be unique");
	}
	Ok(())
}

fn write_comparison(output: &Path, dimension: Dimension, values: &[u64], runs: &[Run]) -> Result<()> {
	let mut rows = Vec::new();
	for (value, run) in values.iter().copied().zip(runs) {
		for (layer, metric, samples) in [
			("moq", Metric::FullSpan, &run.report.object_samples),
			("quic", Metric::QuicFullSpan, &run.report.quic_object_samples),
		] {
			rows.extend(
				samples
					.iter()
					.filter(|sample| sample.metric == metric)
					.cloned()
					.map(|sample| ComparisonRow { value, layer, sample }),
			);
		}
	}
	rows.sort_by(|left, right| {
		(
			left.value,
			left.layer,
			left.sample.group_id,
			left.sample.object_id,
			left.sample.copy_ordinal,
		)
			.cmp(&(
				right.value,
				right.layer,
				right.sample.group_id,
				right.sample.object_id,
				right.sample.copy_ordinal,
			))
	});
	let stem = dimension.artifact_stem();
	let mut writer = csv::Writer::from_path(output.join(format!("{stem}.csv")))?;
	writer.write_record([
		"group_id",
		"object_id",
		"metric",
		"copy_ordinal",
		"elapsed_ms",
		"latency_us",
		dimension.value_column(),
		"layer",
	])?;
	for row in rows {
		writer.write_record([
			row.sample.group_id.to_string(),
			row.sample.object_id.to_string(),
			row.sample.metric.as_str().into(),
			row.sample.copy_ordinal.to_string(),
			format!("{:.6}", row.sample.elapsed_ms),
			format!("{:.6}", row.sample.latency_us),
			row.value.to_string(),
			row.layer.into(),
		])?;
	}
	writer.flush()?;

	let run_values = values
		.iter()
		.copied()
		.zip(runs)
		.map(|(value, run)| {
			let directory = run
				.output
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap_or_default();
			let full_span = &run.report.statistics["full_span"];
			let quic_full_span = &run.report.quic_object_statistics["quic_full_span"];
			let mut entry = serde_json::Map::new();
			entry.insert(dimension.value_column().into(), json!(value));
			entry.insert("directory".into(), json!(directory));
			entry.insert("delivery_copies".into(), json!(quic_full_span.count));
			entry.insert(
				"statistics_us".into(),
				json!({
					"full_span": full_span,
					"quic_full_span": quic_full_span,
				}),
			);
			serde_json::Value::Object(entry)
		})
		.collect::<Vec<_>>();
	let mut summary = serde_json::Map::new();
	summary.insert("sample_unit".into(), json!("delivery copy"));
	summary.insert(dimension.values_key().into(), json!(values));
	summary.insert("runs".into(), serde_json::Value::Array(run_values));
	write_json(
		&output.join(format!("{stem}_summary.json")),
		&serde_json::Value::Object(summary),
	)
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
	let mut writer = BufWriter::new(File::create(path)?);
	serde_json::to_writer_pretty(&mut writer, value)?;
	writer.write_all(b"\n")?;
	writer.flush()?;
	Ok(())
}

fn create_new_dir(path: &Path, kind: &str) -> Result<()> {
	if path.exists() {
		bail!("{kind} output already exists: {}", path.display());
	}
	let parent = path
		.parent()
		.filter(|parent| !parent.as_os_str().is_empty())
		.unwrap_or(Path::new("."));
	std::fs::create_dir_all(parent)
		.with_context(|| format!("failed to create {kind} output parent {}", parent.display()))?;
	std::fs::create_dir(path).with_context(|| format!("failed to create {kind} directory {}", path.display()))
}

fn render(repo: &Path, python: &Path, input: &Path) -> Result<()> {
	let script = repo.join("rs/moq-trace/scripts/plot.py");
	if !script.is_file() {
		bail!("plotting script is missing: {}", script.display());
	}
	let log_path = input.join("plot.log");
	let log = File::create(&log_path)?;
	let stderr = log.try_clone()?;
	let status = Command::new(python)
		.arg(&script)
		.arg(input)
		.current_dir(repo)
		.stdout(Stdio::from(log))
		.stderr(Stdio::from(stderr))
		.status()
		.with_context(|| format!("failed to run Matplotlib renderer with {}", python.display()))?;
	if !status.success() {
		bail!("plotting failed with status {status}; see {}", log_path.display());
	}
	Ok(())
}

fn print_statistics(title: &str, statistics: &std::collections::BTreeMap<String, Statistics>) {
	println!("{title}");
	println!("metric              count    mean_us     p50_us     p95_us     p99_us");
	for (metric, values) in statistics {
		println!(
			"{metric:18} {:6} {:10.2} {:10.2} {:10.2} {:10.2}",
			values.count, values.mean, values.p50, values.p95, values.p99
		);
	}
}

fn display_command(command: &[String]) -> String {
	command.join(" ")
}

#[cfg(test)]
fn format_byte_size(value: u64) -> String {
	for (divisor, suffix) in [(1024 * 1024, "MiB"), (1024, "KiB")] {
		if value % divisor == 0 {
			return format!("{} {suffix}", value / divisor);
		}
	}
	format!("{value} bytes")
}

fn parse_byte_size(value: &str) -> Result<NonZeroU64, String> {
	let bytes = parse_size::Config::new()
		.with_binary()
		.parse_size(value)
		.map_err(|error| format!("invalid byte size {value:?}: {error}"))?;
	NonZeroU64::new(bytes).ok_or_else(|| format!("byte size must be positive: {value:?}"))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn statistics(count: usize) -> Statistics {
		Statistics {
			count,
			mean: 10.0,
			p50: 9.0,
			p95: 15.0,
			p99: 18.0,
			max: 20.0,
		}
	}

	fn report(value: f64) -> Report {
		let object = Sample {
			group_id: 1,
			object_id: 2,
			metric: Metric::FullSpan,
			copy_ordinal: 0,
			elapsed_ms: 3.0,
			latency_us: value,
		};
		let quic = Sample {
			metric: Metric::QuicFullSpan,
			latency_us: value + 1.0,
			..object.clone()
		};
		Report {
			statistics: [("full_span".into(), statistics(1))].into(),
			quic_object_statistics: [("quic_full_span".into(), statistics(1))].into(),
			packet_statistics: [("rx_packet_span".into(), statistics(1))].into(),
			packet_count: 1,
			group_count: 1,
			timelines: Vec::new(),
			object_samples: vec![object],
			quic_object_samples: vec![quic],
			packet_samples: Vec::new(),
		}
	}

	fn config(subscribers: usize) -> Config {
		Config {
			repo: "/repo".into(),
			output: "/output".into(),
			relay_bin: "/relay".into(),
			bench_bin: "/bench".into(),
			quinn_path: None,
			relay_cpu: None,
			subscribers: NonZeroUsize::new(subscribers).unwrap(),
			fps: NonZeroU64::new(30).unwrap(),
			object_size: NonZeroU64::new(16 * 1024).unwrap(),
			duration: Duration::from_secs(20),
			warmup: Duration::from_secs(1),
			cooldown: Duration::from_secs(1),
			port: NonZeroU16::new(4443).unwrap(),
			release: true,
			skip_build: false,
			plot: true,
			python: "python3".into(),
			topology: Topology::default(),
		}
	}

	#[test]
	fn relay_command_has_no_recorder_configuration() {
		let command = relay_command(&config(50));
		assert!(!command.iter().any(|value| value.starts_with("--trace-")));
	}

	#[test]
	fn local_subscriber_command_uses_local_binary_and_url() {
		let command = subscriber_command(&config(8)).unwrap();
		assert_eq!(command[0], "/bench");
		assert_eq!(command[2], "https://localhost:4443");
		let connections = command.iter().position(|value| value == "--connections").unwrap();
		assert_eq!(command[connections + 1], "8");
	}

	#[test]
	fn remote_subscriber_command_uses_ssh_topology() {
		let mut config = config(8);
		config.topology = Topology {
			relay_url: Some("https://192.0.2.10:4443".into()),
			subscriber: SubscriberHost {
				ssh: Some("user@subscriber.example.com".into()),
				binary: Some("/opt/moq/target/release/moq-bench".into()),
				workdir: Some("/opt/moq".into()),
			},
		};

		let command = subscriber_command(&config).unwrap();
		assert_eq!(command[0], "ssh");
		assert_eq!(command[1], "-T");
		assert_eq!(command[2], "-o");
		assert_eq!(command[3], "BatchMode=yes");
		assert_eq!(command[4], "user@subscriber.example.com");
		let remote = command.last().unwrap();
		assert!(remote.starts_with("cd '/opt/moq' && exec '/opt/moq/target/release/moq-bench'"));
		assert!(remote.contains("'--client-connect' 'https://192.0.2.10:4443'"));
		assert!(remote.contains("'--connections' '8'"));
	}

	#[test]
	fn remote_topology_requires_a_relay_url() {
		let topology = Topology {
			relay_url: None,
			subscriber: SubscriberHost {
				ssh: Some("user@subscriber.example.com".into()),
				..Default::default()
			},
		};
		let error = topology.validate().unwrap_err();
		assert!(error.to_string().contains("requires relay_url"));
	}

	#[test]
	fn local_topology_rejects_remote_settings() {
		let topology = Topology {
			relay_url: Some("https://192.0.2.10:4443".into()),
			subscriber: SubscriberHost::default(),
		};
		let error = topology.validate().unwrap_err();
		assert!(error.to_string().contains("require subscriber.ssh"));
	}

	#[test]
	fn topology_loads_from_toml() {
		let directory = tempfile::tempdir().unwrap();
		let path = directory.path().join("topology.toml");
		std::fs::write(
			&path,
			r#"
relay_url = "https://192.0.2.10:4443"

[subscriber]
ssh = "user@subscriber.example.com"
binary = "/opt/moq-bench"
workdir = "/opt/moq"
"#,
		)
		.unwrap();

		let topology = Topology::load(Some(&path)).unwrap();
		assert_eq!(topology.relay_url.as_deref(), Some("https://192.0.2.10:4443"));
		assert_eq!(topology.subscriber.ssh.as_deref(), Some("user@subscriber.example.com"));
		assert_eq!(topology.subscriber.binary.as_deref(), Some("/opt/moq-bench"));
	}

	#[test]
	fn shell_quote_handles_single_quotes() {
		assert_eq!(shell_quote("publisher's bench"), "'publisher'\"'\"'s bench'");
	}

	#[test]
	fn build_does_not_relink_the_running_analyzer() {
		let command = build_command(&config(1)).unwrap();
		assert!(!command.iter().any(|value| value == "moq-trace"));
	}

	#[test]
	fn parses_binary_size_suffixes() {
		assert_eq!(parse_byte_size("16kb").unwrap().get(), 16 * 1024);
		assert_eq!(parse_byte_size("64KiB").unwrap().get(), 64 * 1024);
		assert_eq!(parse_byte_size("262144").unwrap().get(), 256 * 1024);
		assert!(parse_byte_size("large").is_err());
	}

	#[test]
	fn formats_integral_binary_sizes() {
		assert_eq!(format_byte_size(256 * 1024), "256 KiB");
	}

	#[test]
	fn comparisons_require_distinct_values() {
		assert!(validate_comparison(&[1]).is_err());
		assert!(validate_comparison(&[1, 1]).is_err());
		assert!(validate_comparison(&[1, 50]).is_ok());
	}

	#[test]
	fn comparison_artifacts_are_written_by_rust() {
		let output = tempfile::tempdir().unwrap();
		let runs = [
			Run {
				output: output.path().join("subscribers-1"),
				report: report(10.0),
			},
			Run {
				output: output.path().join("subscribers-2"),
				report: report(20.0),
			},
		];

		write_comparison(output.path(), Dimension::Subscribers, &[1, 2], &runs).unwrap();

		let summary: serde_json::Value =
			serde_json::from_slice(&std::fs::read(output.path().join("per_copy_latency_summary.json")).unwrap())
				.unwrap();
		assert_eq!(summary["subscriber_counts"], json!([1, 2]));
		assert_eq!(summary["runs"][1]["subscribers"], 2);
		let samples = std::fs::read_to_string(output.path().join("per_copy_latency.csv")).unwrap();
		assert!(samples.contains("quic_full_span"));
	}

	#[test]
	fn strips_ansi_control_sequences() {
		assert_eq!(strip_ansi("\u{1b}[32mlistening\u{1b}[0m").unwrap(), "listening");
	}
}
