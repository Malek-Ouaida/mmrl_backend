from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mmrl.core.run.artifacts import RunArtifacts
from mmrl.evaluation.risk_inventory import summarize
from mmrl.storage.parquet import write_series_parquet


def _write_json_atomic(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(path)


def persist_risk_inventory(*, art: RunArtifacts, series: Any, max_inventory: float) -> dict:
    """
    Persists:
      - risk_inventory.parquet
      - risk_inventory_summary.json
    Returns summary.
    """
    art.ensure_dirs()

    # write parquet series (may be skipped if empty)
    write_series_parquet(path=art.risk_inventory_parquet, series=series)

    summary = summarize(series, max_inventory=max_inventory) if series is not None else summarize_empty()
    _write_json_atomic(art.risk_inventory_summary_json, summary)
    return summary


def summarize_empty() -> dict:
    return {
        "inv_max_abs": 0.0,
        "inv_mean": 0.0,
        "inv_std": 0.0,
        "time_near_max_frac": 0.0,
        "max_drawdown": 0.0,
        "pnl_total_end": 0.0,
    }
