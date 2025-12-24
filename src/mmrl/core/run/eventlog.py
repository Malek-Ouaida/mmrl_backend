from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from mmrl.core.events.base import Event
from mmrl.core.run.artifacts import RunArtifacts
from mmrl.storage.jsonl import JsonlWriter


@dataclass(frozen=True, slots=True)
class EventLogWriter:
    """
    EventBus component: persists all events to run_dir/events.jsonl
    """
    artifacts: RunArtifacts

    def __post_init__(self) -> None:
        self.artifacts.ensure_dirs()

    def subscriptions(self) -> Sequence[tuple[str, callable]]:
        # Subscribe to every event type you care about.
        # If your bus does not support wildcards, we list the key ones.
        return [
            ("system.run_started", self._on_event),
            ("system.run_stopped", self._on_event),
            ("system.engine_tick", self._on_event),
            ("market.best_bid_ask", self._on_event),
            ("order.submitted", self._on_event),
            ("order.accepted", self._on_event),
            ("order.rejected", self._on_event),
            ("order.cancel_requested", self._on_event),
            ("order.canceled", self._on_event),
            ("order.fill", self._on_event),
        ]

    def _on_event(self, e: Event) -> None:
        writer = JsonlWriter(self.artifacts.events_jsonl)
        writer.append(_event_to_dict(e))


def _event_to_dict(e: Event) -> dict:
    d = {
        "event_id": str(e.event_id),
        "timestamp_utc": e.timestamp_utc.isoformat(),
        "event_type": e.event_type,
    }
    # include dataclass fields if present
    # avoid importing dataclasses.is_dataclass to keep it simple; rely on __dict__
    if hasattr(e, "__dict__"):
        for k, v in e.__dict__.items():
            if k in ("event_id", "timestamp_utc"):
                continue
            d[k] = v
    return d
