use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Serialize;

use super::model::Report;

const ARTIFACT_REVISION: u32 = 1;

#[derive(Serialize)]
struct Files {
	objects: &'static str,
	quic_objects: &'static str,
	quic_packets: &'static str,
}

#[derive(Serialize)]
struct Manifest<'a> {
	artifact_revision: u32,
	files: Files,
	#[serde(flatten)]
	report: &'a Report,
}

pub(super) fn publish(output: &Path, report: &Report) -> Result<()> {
	if output.exists() {
		bail!("analysis output already exists: {}", output.display());
	}
	let parent = output
		.parent()
		.filter(|parent| !parent.as_os_str().is_empty())
		.unwrap_or_else(|| Path::new("."));
	std::fs::create_dir_all(parent)
		.with_context(|| format!("failed to create analysis parent {}", parent.display()))?;
	let staging = tempfile::Builder::new()
		.prefix(".moq-trace-analysis-")
		.tempdir_in(parent)
		.with_context(|| format!("failed to stage analysis beside {}", output.display()))?;
	write_csv(&staging.path().join("objects.csv"), &report.object_samples)?;
	write_csv(&staging.path().join("quic_objects.csv"), &report.quic_object_samples)?;
	write_csv(&staging.path().join("quic_packets.csv"), &report.packet_samples)?;
	let manifest = Manifest {
		artifact_revision: ARTIFACT_REVISION,
		files: Files {
			objects: "objects.csv",
			quic_objects: "quic_objects.csv",
			quic_packets: "quic_packets.csv",
		},
		report,
	};
	let mut writer = BufWriter::new(File::create(staging.path().join("manifest.json"))?);
	serde_json::to_writer_pretty(&mut writer, &manifest)?;
	writer.flush()?;
	let staging = staging.keep();
	if let Err(error) = std::fs::rename(&staging, output) {
		let _ = std::fs::remove_dir_all(&staging);
		return Err(error).with_context(|| format!("failed to publish analysis to {}", output.display()));
	}
	Ok(())
}

fn write_csv(path: &Path, rows: &[impl Serialize]) -> Result<()> {
	let mut writer = csv::Writer::from_path(path)?;
	for row in rows {
		writer.serialize(row)?;
	}
	writer.flush()?;
	Ok(())
}
