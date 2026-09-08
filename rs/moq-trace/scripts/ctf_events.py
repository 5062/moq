#!/usr/bin/env python3
"""Stream validated moq_trace events from an LTTng CTF trace."""

import argparse
import json
import sys
from pathlib import Path

import bt2


def load_schema(source):
    """Load the generated-provider schema used to interpret numeric fields."""
    schema = json.loads(source)
    enums = {
        item["name"]: [value["name"] for value in item["values"]]
        for item in schema["enums"]
    }
    events = {item["name"] for item in schema["events"]}
    return enums, schema["enum_fields"], events


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


def stream(input_path, schema_source, expected_pid=None):
    """Read a CTF trace and stream validated normalized events to stdout."""
    enums, event_enums, event_names = load_schema(schema_source)
    discarded_events = 0
    discarded_packets = 0
    events = 0
    iterator = bt2.TraceCollectionMessageIterator(str(input_path))
    for message in iterator:
        if isinstance(message, bt2._DiscardedEventsMessageConst):
            discarded_events += discarded_count(message)
            continue
        if isinstance(message, bt2._DiscardedPacketsMessageConst):
            discarded_packets += discarded_count(message)
            continue
        if not isinstance(message, bt2._EventMessageConst):
            continue
        provider, separator, name = message.event.name.partition(":")
        if provider != "moq_trace" or not separator or name not in event_names:
            continue
        if expected_pid is not None:
            actual_pid = event_pid(message.event)
            if actual_pid != expected_pid:
                raise RuntimeError(
                    f"expected relay VPID {expected_pid}, found {actual_pid}"
                )
        print(
            json.dumps(
                normalize(name, message.event.payload_field, enums, event_enums),
                separators=(",", ":"),
            ),
            flush=True,
        )
        events += 1

    if discarded_events or discarded_packets:
        raise RuntimeError(
            f"LTTng discarded {discarded_events} events and {discarded_packets} packets"
        )
    if not events:
        raise RuntimeError("CTF trace contains no moq_trace events")


def main():
    """Parse command-line arguments and stream the trace."""
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--schema-json", required=True)
    parser.add_argument("--expected-pid", type=int)
    args = parser.parse_args()
    try:
        stream(
            args.input,
            args.schema_json,
            expected_pid=args.expected_pid,
        )
    except Exception as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
