from __future__ import annotations

import json
import os
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import structlog

from mmrl.core.logging.setup import bind_context

log = structlog.get_logger()


@dataclass(frozen=True)
class RunInfo:
    """
    Immutable metadata describing a single run.
    """

    run_id: str
    run_dir: Path
    created_at_utc: datetime
    seed: int


class RunManager:
    """
    Manages the lifecycle of runs.

    Responsibilities:
    - create unique run identifiers
    - create run directories
    - snapshot configuration
    - bind run_id into logging context
    """

    def __init__(self, runs_dir: Path) -> None:
        self._runs_dir = runs_dir

    def create_run(
        self,
        *,
        seed: int,
        config_snapshot: Mapping[str, Any],
    ) -> RunInfo:
        """
        Create a new run with a unique ID and isolated artifact directory.
        """
        # Ensure base directory exists
        self._runs_dir.mkdir(parents=True, exist_ok=True)

        # UTC, sortable timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        entropy = secrets.token_hex(4)
        run_id = f"{timestamp}_{entropy}"

        run_dir = self._runs_dir / run_id
        run_dir.mkdir(parents=False, exist_ok=False)

        created_at = datetime.now(timezone.utc)

        # Snapshot config for reproducibility
        (run_dir / "config.json").write_text(
            json.dumps(
                config_snapshot,
                indent=2,
                sort_keys=True,
                default=str,
            )
        )

        # Minimal metadata (small, explicit, immutable)
        metadata = {
            "run_id": run_id,
            "created_at_utc": created_at.isoformat(),
            "seed": seed,
            "pid": os.getpid(),
        }
        (run_dir / "meta.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True)
        )

        # Bind run_id to logging context
        bind_context(run_id=run_id)

        log.info(
            "run.created",
            run_id=run_id,
            run_dir=str(run_dir),
            seed=seed,
        )

        return RunInfo(
            run_id=run_id,
            run_dir=run_dir,
            created_at_utc=created_at,
            seed=seed,
        )
