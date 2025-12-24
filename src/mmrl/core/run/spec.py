from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


# -----------------------
# RunSpec building blocks
# -----------------------

RunMode = Literal[
    "paper_no_marketdata",
    "paper_external_bbo",
    "paper_replay_l2",
]

StrategyKind = Literal["fixed_spread"]
ExecutionKind = Literal["paper"]


class FixedSpreadParams(BaseModel):
    # Quoting parameters
    spread: float = Field(..., gt=0, description="Absolute spread in price units")
    order_size: float = Field(..., gt=0, description="Base quote size")

    # Inventory control
    max_inventory: float = Field(..., gt=0, description="Hard inventory cap")
    inventory_skew_k: float = Field(0.0, ge=0, description="Price skew per inventory unit")

    # Re-quote throttling
    min_mid_move: float = Field(0.0, ge=0, description="Requote only if mid moved by >= this")
    min_ticks_between_quotes: int = Field(1, ge=1, description="Min ticks between re-quotes")


class StrategySpec(BaseModel):
    kind: StrategyKind = Field(default="fixed_spread")
    fixed_spread: FixedSpreadParams = Field(
        default_factory=lambda: FixedSpreadParams(
            spread=1.0,
            order_size=0.001,
            max_inventory=0.01,
            inventory_skew_k=0.0,
            min_mid_move=0.0,
            min_ticks_between_quotes=1,
        )
    )

    @model_validator(mode="after")
    def _validate_kind_payload(self) -> "StrategySpec":
        # Extendable for future strategies: inventory-aware, RL, etc.
        if self.kind == "fixed_spread":
            _ = self.fixed_spread  # must exist
        return self


class ExecutionSpec(BaseModel):
    kind: ExecutionKind = Field(default="paper")


class ReplayL2Spec(BaseModel):
    """
    Minimal spec for replay.

    You control how ReplayDataSource interprets it.
    For now we store the path and an optional format tag.
    """
    path: str = Field(..., description="Path to replay datasource")
    format: Optional[str] = Field(default=None, description="Optional format hint (csv/jsonl/parquet/etc.)")


class MarketDataSpec(BaseModel):
    mode: RunMode = Field(default="paper_no_marketdata")
    replay_l2: Optional[ReplayL2Spec] = Field(default=None)

    @model_validator(mode="after")
    def _validate_md(self) -> "MarketDataSpec":
        if self.mode == "paper_replay_l2" and self.replay_l2 is None:
            raise ValueError("marketdata.replay_l2 is required when mode='paper_replay_l2'")
        return self


# -----------------------
# The RunSpec (top-level)
# -----------------------

class RunSpec(BaseModel):
    """
    Canonical run configuration. This is the object you persist to artifacts/config.json.

    Founder-grade properties:
    - deterministic config hash
    - versioned schema
    - explicit run mode/strategy/execution
    - auditable (safe JSON)
    """
    schema_version: int = Field(default=1, description="RunSpec schema version")

    # Identity
    symbol: str = Field(default="BTCUSDT")
    created_at_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Components
    marketdata: MarketDataSpec = Field(default_factory=MarketDataSpec)
    execution: ExecutionSpec = Field(default_factory=ExecutionSpec)
    strategy: StrategySpec = Field(default_factory=StrategySpec)

    # Optional metadata knobs
    seed: Optional[int] = Field(default=None, description="Optional RNG seed override")
    tags: dict[str, str] = Field(default_factory=dict, description="Arbitrary run tags")

    def to_canonical_dict(self) -> dict:
        """
        Produce a stable JSON-compatible dict (no datetime objects).
        This is what gets hashed.
        """
        d = self.model_dump()
        # datetime -> iso
        d["created_at_utc"] = self.created_at_utc.astimezone(timezone.utc).isoformat()
        return d

    def config_hash(self) -> str:
        """
        Deterministic hash of the spec. This becomes your run fingerprint.
        """
        payload = self.to_canonical_dict()
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()
