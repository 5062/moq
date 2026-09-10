"""Analysis errors shared across the offline pipeline."""


class AnalyzeError(RuntimeError):
    """The trace violates an analyzer invariant."""
