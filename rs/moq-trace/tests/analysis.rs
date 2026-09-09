use std::process::Command;

#[test]
fn python_analysis_suite() {
	let available = Command::new("python3")
		.args(["-c", "import bt2, duckdb, pyarrow, pydantic"])
		.status()
		.is_ok_and(|status| status.success());
	if !available {
		return;
	}
	let scripts = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts");
	let output = Command::new("python3")
		.args(["-m", "unittest", "discover", "-s"])
		.arg(scripts.join("tests"))
		.arg("-v")
		.env("PYTHONPATH", &scripts)
		.output()
		.unwrap();
	assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}
