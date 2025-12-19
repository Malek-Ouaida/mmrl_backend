from __future__ import annotations

import logging
import sys
from typing import Any, Mapping

import orjson
import structlog


def _json_serializer(obj: Any, default: Any) -> str:
    """
    High-performance JSON serializer for structured logs.

    Uses orjson for speed and deterministic output.
    """
    return orjson.dumps(obj, default=default).decode("utf-8")


def configure_logging(*, level: str = "INFO") -> None:
    """
    Configure structured logging for the entire application.

    This must be called exactly once at process startup.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    processors: list[Any] = [
        # Merge context variables (run_id, component, symbol, etc.)
        structlog.contextvars.merge_contextvars,

        # Standard metadata
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),

        # Exception handling
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.dict_tracebacks,

        # Final JSON output
        structlog.processors.JSONRenderer(serializer=_json_serializer),
    ]

    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        cache_logger_on_first_use=True,
    )

    # Ensure stdlib logging flows through the same output
    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def bind_context(**values: Mapping[str, Any]) -> None:
    """
    Bind contextual information to all future log entries.

    Example:
        bind_context(run_id="20241217_ab12", component="engine")
    """
    structlog.contextvars.bind_contextvars(**values)


def clear_context() -> None:
    """
    Clear all bound logging context.

    Useful between runs or during shutdown.
    """
    structlog.contextvars.clear_contextvars()
