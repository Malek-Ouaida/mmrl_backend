from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import structlog

from mmrl.core.run.artifacts import RunArtifacts, artifacts_for
from mmrl.core.run.assembly import RunHandle, build_run
from mmrl.core.run.spec import RunSpec

from mmrl.marketdata.replay.datasource import ReplayDataSource
from mmrl.marketdata.replay.jsonl_datasource import JsonlReplayDataSource
from mmrl.strategies.baselines.fixed_spread import FixedSpreadConfig

log = structlog.get_logger()


class RunFactory:
    """
    Product-grade run builder.

    Responsibilities:
    - load/persist RunSpec (config.json)
    - build RunHandle through canonical assembly
    - write wiring snapshot (meta.json) for audit/repro
    """

    def __init__(self, *, runs_dir: Path):
        self.runs_dir = runs_dir

    # -----------------------
    # Persistence: RunSpec I/O
    # -----------------------

    def load_spec(self, *, run_id: str) -> RunSpec:
        art = artifacts_for(runs_dir=self.runs_dir, run_id=run_id)
        if not art.run_dir.exists():
            raise FileNotFoundError(f"run not found: {run_id}")

        if not art.config_json.exists():
            return RunSpec()

        data = json.loads(art.config_json.read_text(encoding="utf-8"))
        return RunSpec.model_validate(data)

    def save_spec(self, *, run_id: str, spec: RunSpec) -> None:
        art = artifacts_for(runs_dir=self.runs_dir, run_id=run_id)
        art.ensure_dirs()

        payload = spec.to_canonical_dict()
        art.config_json.write_text(
            json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    # -----------------------
    # Assembly
    # -----------------------

    def build(self, *, run_id: str, spec: RunSpec) -> RunHandle:
        """
        Build a run from spec -> canonical assembly -> RunHandle.
        """
        art = artifacts_for(runs_dir=self.runs_dir, run_id=run_id)
        if not art.run_dir.exists():
            raise FileNotFoundError(f"run not found: {run_id}")

        # Persist the spec (source of truth)
        self.save_spec(run_id=run_id, spec=spec)

        # Translate StrategySpec -> StrategyConfig
        strategy_cfg = self._build_strategy_config(spec)

        # Translate MarketDataSpec -> args for build_run
        mode = spec.marketdata.mode
        replay_ds = self._build_replay_datasource(spec) if mode == "paper_replay_l2" else None

        handle = build_run(
            runs_dir=self.runs_dir,
            run_id=run_id,
            mode=mode,
            symbol=spec.symbol,
            strategy_cfg=strategy_cfg,
            replay_l2=replay_ds,
        )

        # Founder-level audit: write wiring snapshot
        self._write_wiring_snapshot(art=handle.artifacts, spec=spec, handle=handle)

        return handle

    # -----------------------
    # Helpers
    # -----------------------

    def _build_strategy_config(self, spec: RunSpec) -> FixedSpreadConfig:
        if spec.strategy.kind != "fixed_spread":
            raise ValueError(f"unsupported strategy kind: {spec.strategy.kind}")

        p = spec.strategy.fixed_spread
        return FixedSpreadConfig(
            symbol=spec.symbol,
            spread=p.spread,
            order_size=p.order_size,
            max_inventory=p.max_inventory,
            inventory_skew_k=p.inventory_skew_k,
            min_mid_move=p.min_mid_move,
            min_ticks_between_quotes=p.min_ticks_between_quotes,
        )

    def _build_replay_datasource(self, spec: RunSpec) -> ReplayDataSource:
        r = spec.marketdata.replay_l2
        if r is None:
            raise ValueError("marketdata.replay_l2 is required when mode='paper_replay_l2'")

        replay_path = Path(r.path)
        if not replay_path.exists():
            raise FileNotFoundError(f"replay datasource not found: {replay_path}")

        # 'format' is optional in your spec; default to jsonl.
        fmt = getattr(r, "format", None) or "jsonl"
        if fmt != "jsonl":
            raise ValueError(f"unsupported replay format: {fmt!r} (only 'jsonl' supported)")

        return JsonlReplayDataSource(path=replay_path)

    def _write_wiring_snapshot(self, *, art: RunArtifacts, spec: RunSpec, handle: RunHandle) -> None:
        """
        Writes a reproducible snapshot of what was wired.
        Uses meta.json (already part of your artifacts contract).
        """
        components = [{"type": type(c).__name__, "module": type(c).__module__} for c in handle.components]

        snapshot: dict[str, Any] = {
            "run_id": handle.run_id,
            "spec_hash": spec.config_hash(),
            "symbol": spec.symbol,
            "mode": spec.marketdata.mode,
            "strategy_kind": spec.strategy.kind,
            "execution_kind": spec.execution.kind,
            "components": components,
        }

        try:
            w = handle.wiring
            snapshot["router_wiring"] = asdict(w) if is_dataclass(w) else str(w)
        except Exception:
            snapshot["router_wiring"] = "unavailable"

        art.meta_json.write_text(
            json.dumps(snapshot, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        log.info("run.wiring_snapshot_written", run_id=handle.run_id, spec_hash=snapshot["spec_hash"])
