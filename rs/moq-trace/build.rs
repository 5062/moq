#[path = "build/codegen.rs"]
mod codegen;

fn main() {
	let schema = std::path::Path::new("schema/events.json");
	println!("cargo:rerun-if-changed={}", schema.display());
	codegen::generate(schema).expect("failed to generate LTTng-UST tracepoints");
}
