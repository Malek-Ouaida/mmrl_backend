# src/mmrl/execution/oms/risk_component.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from mmrl.core.events.base import Event
from mmrl.core.events.marketdata import BestBidAskUpdate
from mmrl.core.events.orders import Fill, OrderCanceled, OrderSubmitted
from mmrl.execution.oms.positions import Position
from mmrl.execution.oms.risk import RiskManager, RiskLimits
from mmrl.evaluation.risk_inventory import RiskInventorySeries


@dataclass(slots=True)
class RiskInventorySnapshot:
    """
    Minimal live snapshot that stays deterministic and replay-safe.
    """
    peak_total: float = 0.0


class RiskInventoryComponent:
    """
    Institutional Risk + Inventory component.

    Responsibilities:
      - deterministic inventory/reservation tracking (RiskManager)
      - realized pnl + position accounting (Position)
      - mark-to-market + drawdown tracking on BBO
      - produce replay-safe time series for evaluation/artifacts
    """

    def __init__(self, *, limits: RiskLimits) -> None:
        self.risk = RiskManager(limits=limits)
        self.positions: dict[str, Position] = {}
        self.series = RiskInventorySeries.empty()
        self._snap = RiskInventorySnapshot()

        # last mid per symbol (so we can compute series even if fills arrive before next BBO)
        self._last_mid: dict[str, float] = {}

    # --- Event bus wiring -------------------------------------------------

    def subscriptions(self) -> Sequence[tuple[str, callable]]:
        return (
            (OrderSubmitted.event_type, self.on_order_submitted),
            (OrderCanceled.event_type, self.on_order_canceled),
            (Fill.event_type, self.on_fill),
            (BestBidAskUpdate.event_type, self.on_bbo),
        )

    # --- Handlers ---------------------------------------------------------

    def on_order_submitted(self, e: Event) -> None:
        o = e  # OrderSubmitted
        assert isinstance(o, OrderSubmitted)

        # Reserve exposure at submit time (conservative full-fill)
        # (you can move reservation to OrderAccepted if you prefer)
        self.risk.check_new_order(
            symbol=o.symbol,
            side=o.side,
            qty=o.quantity,
            price=o.price,
            order_id=o.order_id,
        )

    def on_order_canceled(self, e: Event) -> None:
        c = e  # OrderCanceled
        assert isinstance(c, OrderCanceled)
        self.risk.on_cancel(order_id=c.order_id)

    def on_fill(self, e: Event) -> None:
        f = e
        assert isinstance(f, Fill)

        pos = self.positions.get(f.symbol)
        if pos is None:
            pos = Position(symbol=f.symbol)
            self.positions[f.symbol] = pos

        # Position accounting (realized pnl + avg entry)
        pos.on_fill(side=f.side, qty=f.fill_quantity, price=f.fill_price)

        # RiskManager inventory + reservation release (partial-fill correct)
        self.risk.on_fill(
            symbol=f.symbol,
            side=f.side,
            qty=f.fill_quantity,
            order_id=f.order_id,
            remaining_qty=f.remaining_quantity,
        )

        # Optional: if we have a last mid, record a snapshot immediately (more granular)
        mid = self._last_mid.get(f.symbol)
        if mid is not None:
            self._record(seq=f.sequence, symbol=f.symbol, mid=mid)

    def on_bbo(self, e: Event) -> None:
        b = e
        assert isinstance(b, BestBidAskUpdate)

        mid = 0.5 * (b.bid_price + b.ask_price)
        self._last_mid[b.symbol] = mid

        self._record(seq=b.sequence, symbol=b.symbol, mid=mid)

    # --- Internal ---------------------------------------------------------

    def _record(self, *, seq: int, symbol: str, mid: float) -> None:
        pos = self.positions.get(symbol)
        inv = 0.0 if pos is None else pos.inventory
        realized = 0.0 if pos is None else pos.realized_pnl

        reserved = self.risk.reserved(symbol=symbol)

        # mark-to-market convention (simple + replay-safe)
        unrealized = inv * mid
        total = realized + unrealized

        if total > self._snap.peak_total:
            self._snap.peak_total = total

        drawdown = self._snap.peak_total - total

        self.series.append(
            seq=seq,
            inv=inv,
            reserved=reserved,
            mid=mid,
            realized=realized,
            unrealized=unrealized,
            total=total,
            drawdown=drawdown,
        )
