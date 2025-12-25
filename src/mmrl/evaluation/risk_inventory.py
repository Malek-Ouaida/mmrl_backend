# src/mmrl/evaluation/risk_inventory.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RiskInventorySeries:
    seq: list[int]
    inv: list[float]
    reserved: list[float]
    mid: list[float]
    realized: list[float]
    unrealized: list[float]
    total: list[float]
    drawdown: list[float]

    @classmethod
    def empty(cls) -> "RiskInventorySeries":
        return cls(seq=[], inv=[], reserved=[], mid=[], realized=[], unrealized=[], total=[], drawdown=[])

    def append(
        self,
        *,
        seq: int,
        inv: float,
        reserved: float,
        mid: float,
        realized: float,
        unrealized: float,
        total: float,
        drawdown: float,
    ) -> None:
        self.seq.append(seq)
        self.inv.append(inv)
        self.reserved.append(reserved)
        self.mid.append(mid)
        self.realized.append(realized)
        self.unrealized.append(unrealized)
        self.total.append(total)
        self.drawdown.append(drawdown)


def summarize(series: RiskInventorySeries, *, max_inventory: float) -> dict:
    """
    Founder-grade risk & inventory summary.
    Deterministic. No external deps. Replay-safe.
    """
    if not series.seq:
        return {
            "inv_max_abs": 0.0,
            "inv_mean": 0.0,
            "inv_std": 0.0,
            "time_near_max_frac": 0.0,
            "max_drawdown": 0.0,
            "pnl_total_end": 0.0,
        }

    n = len(series.inv)
    mean = sum(series.inv) / n
    var = sum((x - mean) ** 2 for x in series.inv) / max(1, n - 1)
    std = var ** 0.5

    inv_max_abs = max(abs(x) for x in series.inv)
    max_dd = max(series.drawdown)
    near_thr = 0.8 * max_inventory
    time_near = sum(1 for x in series.inv if abs(x) >= near_thr) / n

    return {
        "inv_max_abs": inv_max_abs,
        "inv_mean": mean,
        "inv_std": std,
        "time_near_max_frac": time_near,
        "max_drawdown": max_dd,
        "pnl_total_end": series.total[-1],
    }
