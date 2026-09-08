use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::Deserialize;

pub fn generate(path: &Path) -> Result<(), Error> {
	let source = std::fs::read_to_string(path).map_err(Error::ReadSchema)?;
	let schema: Schema = serde_json::from_str(&source).map_err(Error::ParseSchema)?;
	schema.validate()?;
	let output = PathBuf::from(std::env::var_os("OUT_DIR").ok_or(Error::MissingOutputDirectory)?);

	std::fs::write(output.join("bindings.rs"), schema.rust()).map_err(Error::WriteGenerated)?;
	std::fs::write(output.join("moq_trace.h"), schema.header()).map_err(Error::WriteGenerated)?;

	if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux") {
		std::fs::write(output.join("moq_trace_provider.h"), schema.provider_header()).map_err(Error::WriteGenerated)?;
		let provider = output.join("moq_trace_provider.c");
		let interface = output.join("moq_trace_interface.c");
		std::fs::write(&provider, schema.provider_source()).map_err(Error::WriteGenerated)?;
		std::fs::write(&interface, schema.interface_source()).map_err(Error::WriteGenerated)?;
		let library = pkg_config::Config::new()
			.atleast_version("2.13")
			.cargo_metadata(false)
			.probe("lttng-ust")
			.map_err(Error::Lttng)?;
		let mut build = cc::Build::new();
		build.files([provider, interface]).include(&output);
		for include in library.include_paths {
			build.include(include);
		}
		build.compile("moq_trace_provider");
		pkg_config::Config::new()
			.atleast_version("2.13")
			.probe("lttng-ust")
			.map_err(Error::Lttng)?;
	}
	Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
	#[error("failed to read trace schema")]
	ReadSchema(#[source] std::io::Error),
	#[error("failed to parse trace schema")]
	ParseSchema(#[source] serde_json::Error),
	#[error("OUT_DIR is not set")]
	MissingOutputDirectory,
	#[error("failed to write generated trace source")]
	WriteGenerated(#[source] std::io::Error),
	#[error("failed to locate LTTng-UST with pkg-config")]
	Lttng(#[source] pkg_config::Error),
	#[error("invalid trace schema identifier: {0}")]
	InvalidIdentifier(String),
	#[error("trace event or enum {0} has no members")]
	Empty(String),
	#[error("unsupported trace field type: {0}")]
	UnsupportedType(String),
	#[error("duplicate trace schema name: {0}")]
	Duplicate(String),
	#[error("enum_fields references unknown event: {0}")]
	UnknownEvent(String),
	#[error("enum_fields references unknown enum: {0}")]
	UnknownEnum(String),
	#[error("enum_fields references unknown field: {0}")]
	UnknownField(String),
	#[error("enum_fields field must have type u8: {0}")]
	InvalidEnumField(String),
	#[error("event is missing an enum_fields entry: {0}")]
	MissingEnumFields(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Schema {
	enums: Vec<Enum>,
	enum_fields: std::collections::HashMap<String, std::collections::HashMap<String, String>>,
	events: Vec<Event>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Enum {
	name: String,
	rust_type: String,
	values: Vec<EnumValue>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnumValue {
	name: String,
	rust_variant: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
	name: String,
	fields: Vec<(String, String)>,
}

impl Schema {
	fn validate(&self) -> Result<(), Error> {
		let mut names = HashSet::new();
		let mut enum_names = HashSet::new();
		for enumeration in &self.enums {
			identifier(&enumeration.name)?;
			rust_identifier(&enumeration.rust_type)?;
			if enumeration.values.is_empty() {
				return Err(Error::Empty(enumeration.name.clone()));
			}
			if !names.insert(format!("enum:{}", enumeration.name)) {
				return Err(Error::Duplicate(enumeration.name.clone()));
			}
			enum_names.insert(enumeration.name.as_str());
			let mut values = HashSet::new();
			for value in &enumeration.values {
				identifier(&value.name)?;
				rust_identifier(&value.rust_variant)?;
				if !values.insert(&value.name) {
					return Err(Error::Duplicate(format!("{}:{}", enumeration.name, value.name)));
				}
			}
		}
		for event in &self.events {
			identifier(&event.name)?;
			if event.fields.is_empty() {
				return Err(Error::Empty(event.name.clone()));
			}
			if !names.insert(format!("event:{}", event.name)) {
				return Err(Error::Duplicate(event.name.clone()));
			}
			let mut fields = HashSet::new();
			for (name, ty) in &event.fields {
				identifier(name)?;
				if !fields.insert(name) {
					return Err(Error::Duplicate(format!("{}:{name}", event.name)));
				}
				FieldType::parse(ty)?;
			}
		}
		let events = self
			.events
			.iter()
			.map(|event| (event.name.as_str(), event))
			.collect::<std::collections::HashMap<_, _>>();
		for (event_name, mappings) in &self.enum_fields {
			let event = events
				.get(event_name.as_str())
				.ok_or_else(|| Error::UnknownEvent(event_name.clone()))?;
			for (field_name, enum_name) in mappings {
				if !enum_names.contains(enum_name.as_str()) {
					return Err(Error::UnknownEnum(enum_name.clone()));
				}
				let field = event
					.fields
					.iter()
					.find(|(name, _)| name == field_name)
					.ok_or_else(|| Error::UnknownField(format!("{event_name}:{field_name}")))?;
				if field.1 != "u8" {
					return Err(Error::InvalidEnumField(format!("{event_name}:{field_name}")));
				}
			}
		}
		for event in &self.events {
			if !self.enum_fields.contains_key(&event.name) {
				return Err(Error::MissingEnumFields(event.name.clone()));
			}
		}
		Ok(())
	}

	fn rust(&self) -> String {
		let mut out = String::from("// Generated from schema/events.json.\n\n");
		for enumeration in &self.enums {
			writeln!(
				out,
				"pub(crate) fn {}(value: crate::{}) -> u8 {{",
				enumeration.name, enumeration.rust_type
			)
			.unwrap();
			out.push_str("\tmatch value {\n");
			for (index, value) in enumeration.values.iter().enumerate() {
				writeln!(
					out,
					"\t\tcrate::{}::{} => {index},",
					enumeration.rust_type, value.rust_variant
				)
				.unwrap();
			}
			out.push_str("\t}\n}\n\n");
		}
		for event in &self.events {
			writeln!(
				out,
				"#[repr(C)]\n#[derive(Clone, Copy)]\npub(crate) struct {} {{",
				pascal(&event.name)
			)
			.unwrap();
			for (name, ty) in &event.fields {
				writeln!(out, "\tpub(crate) {name}: {},", FieldType::parse(ty).unwrap().rust()).unwrap();
			}
			out.push_str("}\n\n");
		}
		out.push_str("#[cfg(target_os = \"linux\")]\nunsafe extern \"C\" {\n");
		for event in &self.events {
			writeln!(out, "\tpub(crate) fn moq_trace_{}_enabled() -> bool;", event.name).unwrap();
			writeln!(
				out,
				"\tpub(crate) fn moq_trace_{}(event: *const {});",
				event.name,
				pascal(&event.name)
			)
			.unwrap();
		}
		out.push_str("\tpub(crate) fn moq_trace_provider_keep_sections();\n}\n");
		out
	}

	fn header(&self) -> String {
		let mut out = String::from(
			"/* Generated from schema/events.json. */\n#ifndef MOQ_TRACE_H\n#define MOQ_TRACE_H\n#include <stdbool.h>\n#include <stdint.h>\n\n",
		);
		for event in &self.events {
			writeln!(out, "struct moq_trace_{} {{", event.name).unwrap();
			for (name, ty) in &event.fields {
				writeln!(out, "\t{} {name};", FieldType::parse(ty).unwrap().c()).unwrap();
			}
			writeln!(
				out,
				"}};\nbool moq_trace_{}_enabled(void);\nvoid moq_trace_{}(const struct moq_trace_{} *event);\n",
				event.name, event.name, event.name
			)
			.unwrap();
		}
		out.push_str("void moq_trace_provider_keep_sections(void);\n#endif\n");
		out
	}

	fn provider_header(&self) -> String {
		let mut out = String::from(
			"/* Generated from schema/events.json. */\n#undef TRACEPOINT_PROVIDER\n#define TRACEPOINT_PROVIDER moq_trace\n#undef TRACEPOINT_INCLUDE\n#define TRACEPOINT_INCLUDE \"moq_trace_provider.h\"\n#if !defined(MOQ_TRACE_PROVIDER) || defined(TRACEPOINT_HEADER_MULTI_READ)\n#define MOQ_TRACE_PROVIDER\n#include <lttng/tracepoint.h>\n#include \"moq_trace.h\"\n\n",
		);
		for event in &self.events {
			writeln!(
				out,
				"TRACEPOINT_EVENT(\n\tmoq_trace,\n\t{},\n\tTP_ARGS(const struct moq_trace_{} *, event),\n\tTP_FIELDS(",
				event.name, event.name
			)
			.unwrap();
			for (name, ty) in &event.fields {
				let ty = FieldType::parse(ty).unwrap();
				writeln!(out, "\t\tctf_integer({}, {name}, event->{name})", ty.c()).unwrap();
			}
			out.push_str("\t)\n)\n\n");
		}
		out.push_str("#endif\n#include <lttng/tracepoint-event.h>\n");
		out
	}

	fn provider_source(&self) -> String {
		let mut out = String::from(
			"/* Generated from schema/events.json. */\n#define TRACEPOINT_CREATE_PROBES\n#define TRACEPOINT_DEFINE\n#include \"moq_trace_provider.h\"\n\n__attribute__((noinline, used))\nvoid moq_trace_provider_keep_sections(void) {\n\tvolatile void *pointers[] = {\n",
		);
		for event in &self.events {
			writeln!(out, "\t\t&lttng_ust_tracepoint_ptr_moq_trace___{},", event.name).unwrap();
		}
		out.push_str("\t};\n\t__asm__ __volatile__(\"\" : : \"g\"(pointers) : \"memory\");\n}\n");
		out
	}

	fn interface_source(&self) -> String {
		let mut out = String::from("/* Generated from schema/events.json. */\n#include \"moq_trace_provider.h\"\n\n");
		for event in &self.events {
			writeln!(
				out,
				"bool moq_trace_{}_enabled(void) {{ return tracepoint_enabled(moq_trace, {}); }}",
				event.name, event.name
			)
			.unwrap();
			writeln!(
				out,
				"void moq_trace_{}(const struct moq_trace_{} *event) {{ tracepoint(moq_trace, {}, event); }}\n",
				event.name, event.name, event.name
			)
			.unwrap();
		}
		out
	}
}

#[derive(Clone, Copy)]
enum FieldType {
	U8,
	U32,
	U64,
}

impl FieldType {
	fn parse(value: &str) -> Result<Self, Error> {
		match value {
			"u8" => Ok(Self::U8),
			"u32" => Ok(Self::U32),
			"u64" => Ok(Self::U64),
			_ => Err(Error::UnsupportedType(value.to_owned())),
		}
	}

	fn rust(self) -> &'static str {
		match self {
			Self::U8 => "u8",
			Self::U32 => "u32",
			Self::U64 => "u64",
		}
	}

	fn c(self) -> &'static str {
		match self {
			Self::U8 => "uint8_t",
			Self::U32 => "uint32_t",
			Self::U64 => "uint64_t",
		}
	}
}

fn identifier(value: &str) -> Result<(), Error> {
	const KEYWORDS: &[&str] = &[
		"auto", "break", "case", "char", "const", "continue", "default", "do", "double", "else", "enum", "extern",
		"float", "for", "goto", "if", "inline", "int", "long", "register", "restrict", "return", "short", "signed",
		"sizeof", "static", "struct", "switch", "typedef", "union", "unsigned", "void", "volatile", "while",
	];
	let valid = value.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
		&& value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
		&& !KEYWORDS.contains(&value);
	if valid {
		Ok(())
	} else {
		Err(Error::InvalidIdentifier(value.to_owned()))
	}
}

fn rust_identifier(value: &str) -> Result<(), Error> {
	identifier(&value.to_ascii_lowercase())
}

fn pascal(value: &str) -> String {
	value
		.split('_')
		.filter(|part| !part.is_empty())
		.map(|part| {
			let mut chars = part.chars();
			chars
				.next()
				.map(|character| character.to_ascii_uppercase())
				.into_iter()
				.chain(chars)
				.collect::<String>()
		})
		.collect()
}
