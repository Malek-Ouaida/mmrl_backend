# src/mmrl/storage/jsonl.py
from __future__ import annotations

import json
import os
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Mapping

from mmrl.core.events.base import Event


class JsonlEventStore:
    """
    Append-only JSONL event store.

    - One event per line (JSON dict).
    - fsync on demand for crash safety.
    - Deterministic: preserves publish order as written.
    """

    def __init__(self, *, path: Path, fsync: bool = True) -> None:
        self._path = path
        self._fsync = fsync
        self._fh = None  # lazy open

        # Ensure parent exists
        self._path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def open(self) -> None:
        if self._fh is not None:
            return
        # line-buffered text mode
        self._fh = self._path.open("a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        if self._fh is None:
            return
        try:
            self._fh.flush()
            if self._fsync:
                os.fsync(self._fh.fileno())
        finally:
            self._fh.close()
            self._fh = None

    def append(self, event: Event) -> None:
        """
        Append an event as a single JSON line.

        Serialization rules:
        - dataclasses -> asdict
        - UUID/datetime -> str via default=str
        """
        self.open()
        assert self._fh is not None

        payload = _event_to_dict(event)
        line = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        self._fh.write(line + "\n")
        self._fh.flush()
        if self._fsync:
            os.fsync(self._fh.fileno())

    def iter_events(self) -> list[Mapping[str, Any]]:
        """
        Read all events as dicts (useful for quick diagnostics/tests).

        For a true replay engine we’ll stream this generator-style,
        but keeping it simple + deterministic here.
        """
        if not self._path.exists():
            return []
        out: list[Mapping[str, Any]] = []
        with self._path.open("r", encoding="utf-8") as fh:
            for line in fh:
                s = line.strip()
                if not s:
                    continue
                out.append(json.loads(s))
        return out


def _event_to_dict(event: Event) -> dict[str, Any]:
    # All events are dataclasses in this project
    if is_dataclass(event):
        d = asdict(event)
    else:
        # fallback: best-effort
        d = dict(event.__dict__)

    # Ensure event_type is always present even though it's ClassVar
    d["event_type"] = event.event_type
    return d
