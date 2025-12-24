from __future__ import annotations

import json
import os
import platform
import secrets
import socket
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import structlog

from mmrl.core.logging.setup import bind_context

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
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
    - persist minimal metadata + provenance
    - bind run_id into logging context
    """

    _META_SCHEMA_VERSION = 1

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
        self._runs_dir.mkdir(parents=True, exist_ok=True)

        # Use one timestamp source for everything in this run creation
        created_at = datetime.now(timezone.utc)
        timestamp = created_at.strftime("%Y%m%dT%H%M%SZ")

        # High-entropy suffix to avoid collisions (even if called in same second)
        entropy = secrets.token_hex(4)
        run_id = f"{timestamp}_{entropy}"

        run_dir = self._runs_dir / run_id
        run_dir.mkdir(parents=False, exist_ok=False)

        # Snapshot config for reproducibility
        self._write_json_atomic(
            path=run_dir / "config.json",
            payload=config_snapshot,
            default=str,
        )

        # Minimal metadata (explicit, forward-compatible)
        metadata: dict[str, Any] = {
            "schema_version": self._META_SCHEMA_VERSION,
            "run_id": run_id,
            "created_at_utc": created_at.isoformat(),
            "seed": seed,
            "pid": os.getpid(),
            "cwd": str(Path.cwd()),
            "hostname": socket.gethostname(),
            "python": {
                "version": sys.version.split()[0],
                "executable": sys.executable,
            },
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
            },
            "git": {
                "commit": self._try_get_git_commit(),
            },
        }
        self._write_json_atomic(path=run_dir / "meta.json", payload=metadata)

        # Bind run_id to structured log context
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

    # ---------------- Internals ----------------

    def _write_json_atomic(
        self,
        *,
        path: Path,
        payload: Any,
        default: Any | None = None,
    ) -> None:
        """
        Atomic JSON write:
        write to tmp file then replace, so readers never see partial JSON.
        """
        tmp = path.with_suffix(path.suffix + ".tmp")
        data = json.dumps(payload, indent=2, sort_keys=True, default=default)
        tmp.write_text(data)
        tmp.replace(path)

    def _try_get_git_commit(self) -> str | None:
        """
        Best-effort git commit capture without shelling out.
        Returns None if not in a git repo.
        """
        try:
            # Walk up from cwd until we find .git
            cur = Path.cwd()
            for _ in range(15):
                git_dir = cur / ".git"
                if git_dir.exists():
                    head = (git_dir / "HEAD").read_text().strip()
                    if head.startswith("ref:"):
                        ref = head.split(":", 1)[1].strip()
                        ref_path = git_dir / ref
                        if ref_path.exists():
                            return ref_path.read_text().strip()[:40]
                        # Packed refs (best-effort)
                        packed = git_dir / "packed-refs"
                        if packed.exists():
                            for line in packed.read_text().splitlines():
                                if line.startswith("#") or line.startswith("^") or not line.strip():
                                    continue
                                sha, name = line.split(" ", 1)
                                if name.strip() == ref:
                                    return sha.strip()[:40]
                        return None
                    # Detached HEAD
                    return head[:40] if head else None
                cur = cur.parent
        except Exception:
            return None
        return None
