# Relay Latency Comments Design

## Goal

Document the non-obvious constraints and analysis choices in
`rs/moq-trace/scripts/relay_latency.py` without narrating self-explanatory
Python.

## Scope

Review the entire script. Add short comments or docstrings only where a future
maintainer needs the reason behind the implementation:

- select the headless Matplotlib backend before importing `pyplot`;
- represent each object by its slowest subscriber copy;
- use deterministic identity tie-breaking when selecting real objects;
- group lifecycle events by direction and session before pairing them;
- normalize timelines to the inbound object start;
- trim warmup and cooldown using inbound object timestamps;
- treat copy ordinals as timestamp ordering rather than subscriber identity;
- signal process groups and stop the publisher before the relay so trace data
  is flushed before analysis.

## Style

Keep each comment brief and adjacent to the code it explains. Prefer
consumer-oriented function docstrings for undocumented helpers and inline
comments for local invariants. Do not add section banners, historical notes, or
comments that repeat variable names and control flow.

## Validation

The change must not alter behavior. Run Python syntax compilation, the complete
relay-latency unit-test suite, and the repository whitespace check.
