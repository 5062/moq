#!/usr/bin/env python3
"""Convert moq_trace LTTng CTF events to the analyzer's JSONL format."""

import argparse
import json
import os
import sys
from pathlib import Path

import bt2


class ConversionError(RuntimeError):
    """A CTF trace cannot be converted into a complete normalized trace."""

    def __init__(self, message, summary):
        super().__init__(message)
        self.summary = summary


def load_schema(path):
    """Load the generated-provider schema used to interpret numeric fields."""
    with path.open(encoding="utf-8") as source:
        schema = json.load(source)
    enums = {
        item["name"]: [value["name"] for value in item["values"]]
        for item in schema["enums"]
    }
    events = {item["name"] for item in schema["events"]}
    return schema["revision"], enums, schema["enum_fields"], events


def scalar(value):
    """Convert a Babeltrace scalar field to a plain Python value."""
    return int(value)


def normalize(name, payload, enums, event_enums):
    """Normalize one generated provider payload to the stable JSON event schema."""
    event = {key: scalar(payload[key]) for key in payload}
    for key in tuple(event):
        if not key.startswith("has_"):
            continue
        value_key = key[4:]
        event[value_key] = event[value_key] if event.pop(key) else None

    for key, enum_name in event_enums[name].items():
        if event[key] is not None:
            try:
                event[key] = enums[enum_name][event[key]]
            except (KeyError, IndexError) as error:
                raise RuntimeError(
                    f"unknown {enum_name} value {event[key]} in {name}.{key}"
                ) from error

    if name == "moq_object_start":
        event["logical_id"] = {
            "group": event.pop("logical_group"),
            "frame": event.pop("logical_frame"),
        }
    elif name == "udp_socket_end":
        event["stats"] = {
            "buffers": event.pop("buffers"),
            "datagrams": event.pop("datagrams"),
            "bytes": event.pop("bytes"),
        }
    event["type"] = name
    return event


def discarded_count(message):
    """Return an exact discarded count when Babeltrace provides one."""
    count = message.count
    return 1 if count is None else int(count)


def event_pid(event):
    """Return the LTTng virtual PID recorded in the event context."""
    context = event.common_context_field
    if context is None or "vpid" not in context:
        return None
    return scalar(context["vpid"])


def convert(input_path, output_path, schema_path, expected_pid=None):
    """Read a CTF trace and atomically write a complete normalized JSONL trace."""
    revision, enums, event_enums, event_names = load_schema(schema_path)
    summary = {"events": 0, "discarded_events": 0, "discarded_packets": 0}
    temporary = output_path.with_name(f".{output_path.name}.{os.getpid()}.tmp")
    if output_path.exists() or temporary.exists():
        raise ConversionError(f"output already exists: {output_path}", summary)

    try:
        iterator = bt2.TraceCollectionMessageIterator(str(input_path))
        with temporary.open("x", encoding="utf-8") as output:
            output.write(
                json.dumps(
                    {"type": "trace_header", "revision": revision, "clock": "monotonic_ns"}
                )
                + "\n"
            )
            for message in iterator:
                if isinstance(message, bt2._DiscardedEventsMessageConst):
                    summary["discarded_events"] += discarded_count(message)
                    continue
                if isinstance(message, bt2._DiscardedPacketsMessageConst):
                    summary["discarded_packets"] += discarded_count(message)
                    continue
                if not isinstance(message, bt2._EventMessageConst):
                    continue
                provider, separator, name = message.event.name.partition(":")
                if provider != "moq_trace" or not separator or name not in event_names:
                    continue
                if expected_pid is not None:
                    actual_pid = event_pid(message.event)
                    if actual_pid != expected_pid:
                        raise ConversionError(
                            f"expected relay VPID {expected_pid}, found {actual_pid}", summary
                        )
                output.write(
                    json.dumps(
                        normalize(name, message.event.payload_field, enums, event_enums),
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                summary["events"] += 1

        if summary["discarded_events"] or summary["discarded_packets"]:
            raise ConversionError("LTTng reported discarded trace data", summary)
        temporary.replace(output_path)
        return summary
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def main():
    """Parse command-line arguments and convert the trace."""
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(__file__).parent.parent / "schema" / "events.json",
    )
    parser.add_argument("--expected-pid", type=int)
    args = parser.parse_args()
    try:
        summary = convert(
            args.input,
            args.output,
            args.schema,
            expected_pid=args.expected_pid,
        )
    except ConversionError as error:
        print(json.dumps(error.summary, separators=(",", ":")))
        print(error, file=sys.stderr)
        return 1
    print(json.dumps(summary, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
