from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_\-]+$")


def validate_run_id(run_id: str) -> None:
    if not run_id or not _RUN_ID_RE.match(run_id):
        raise ValueError(f"invalid run_id: {run_id!r}")


@dataclass(frozen=True, slots=True)
class RunArtifacts:
    """
    Stable artifact contract for a run directory.
    """
    run_dir: Path

    @property
    def config_json(self) -> Path:
        return self.run_dir / "config.json"

    @property
    def meta_json(self) -> Path:
        return self.run_dir / "meta.json"

    @property
    def events_jsonl(self) -> Path:
        return self.run_dir / "events.jsonl"

    @property
    def metrics_json(self) -> Path:
        return self.run_dir / "metrics.json"

    @property
    def evaluation_json(self) -> Path:
        return self.run_dir / "evaluation.json"

    # ✅ Risk & Inventory artifacts
    @property
    def risk_inventory_parquet(self) -> Path:
        return self.run_dir / "risk_inventory.parquet"

    @property
    def risk_inventory_summary_json(self) -> Path:
        return self.run_dir / "risk_inventory_summary.json"

    @property
    def logs_dir(self) -> Path:
        return self.run_dir / "logs"

    @property
    def engine_log(self) -> Path:
        return self.logs_dir / "engine.log"

    def ensure_dirs(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)


def artifacts_for(*, runs_dir: Path, run_id: str) -> RunArtifacts:
    validate_run_id(run_id)
    run_dir = (runs_dir / run_id).resolve()

    base = runs_dir.resolve()
    if base not in run_dir.parents and run_dir != base:
        raise ValueError("invalid run_dir resolution")

    return RunArtifacts(run_dir=run_dir)
