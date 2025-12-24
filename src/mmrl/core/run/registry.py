from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Literal

from mmrl.core.run.manager import RunInfo

# This is what your API imports
RunStatus = Literal["created", "running", "stopped", "error"]


@dataclass(frozen=True, slots=True)
class RunRecord:
    """
    In-memory view of a run for API/ops visibility.

    Durable truth is still the run directory on disk.
    """
    run_id: str
    run_dir: str
    seed: int

    status: RunStatus
    created_at_utc: datetime
    updated_at_utc: datetime

    error_type: str | None = None
    error_message: str | None = None


class RunRegistry:
    """
    Thread-safe registry for run status.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._runs: dict[str, RunRecord] = {}

    def upsert_created(self, run: RunInfo) -> RunRecord:
        now = datetime.now(timezone.utc)
        rec = RunRecord(
            run_id=run.run_id,
            run_dir=str(run.run_dir),
            seed=run.seed,
            status="created",
            created_at_utc=run.created_at_utc,
            updated_at_utc=now,
        )
        with self._lock:
            self._runs[run.run_id] = rec
        return rec

    def mark_running(self, *, run_id: str) -> None:
        self._set_status(run_id=run_id, status="running")

    def mark_stopped(self, *, run_id: str) -> None:
        self._set_status(run_id=run_id, status="stopped")

    def mark_error(self, *, run_id: str, error_type: str, error_message: str) -> None:
        with self._lock:
            cur = self._runs.get(run_id)
            now = datetime.now(timezone.utc)

            if cur is None:
                # Keep it minimal but visible
                self._runs[run_id] = RunRecord(
                    run_id=run_id,
                    run_dir="",
                    seed=0,
                    status="error",
                    created_at_utc=now,
                    updated_at_utc=now,
                    error_type=error_type,
                    error_message=error_message,
                )
                return

            self._runs[run_id] = RunRecord(
                run_id=cur.run_id,
                run_dir=cur.run_dir,
                seed=cur.seed,
                status="error",
                created_at_utc=cur.created_at_utc,
                updated_at_utc=now,
                error_type=error_type,
                error_message=error_message,
            )

    def get(self, *, run_id: str) -> RunRecord | None:
        with self._lock:
            return self._runs.get(run_id)

    def list(self) -> list[RunRecord]:
        with self._lock:
            items = list(self._runs.values())
        items.sort(key=lambda r: r.updated_at_utc, reverse=True)
        return items

    def _set_status(self, *, run_id: str, status: RunStatus) -> None:
        with self._lock:
            cur = self._runs.get(run_id)
            now = datetime.now(timezone.utc)

            if cur is None:
                self._runs[run_id] = RunRecord(
                    run_id=run_id,
                    run_dir="",
                    seed=0,
                    status=status,
                    created_at_utc=now,
                    updated_at_utc=now,
                )
                return

            self._runs[run_id] = RunRecord(
                run_id=cur.run_id,
                run_dir=cur.run_dir,
                seed=cur.seed,
                status=status,
                created_at_utc=cur.created_at_utc,
                updated_at_utc=now,
                error_type=None,
                error_message=None,
            )
