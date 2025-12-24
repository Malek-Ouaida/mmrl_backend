from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Protocol

from mmrl.core.events.marketdata import BestBidAskUpdate
from mmrl.execution.oms.orders import OrderRecord


def _finite_pos(x: float) -> bool:
    return isfinite(x) and x > 0.0


@dataclass(frozen=True, slots=True)
class FillDecision:
    """
    Deterministic result of a fill check.

    If executable=False -> ignore other fields.
    If executable=True  -> fill_price and fill_qty must be set and valid.
    """
    executable: bool
    fill_price: float | None = None
    fill_qty: float | None = None

    def validate(self, *, remaining: float, eps: float = 1e-12) -> None:
        if not self.executable:
            return
        if self.fill_price is None or self.fill_qty is None:
            raise ValueError("executable FillDecision must include fill_price and fill_qty")
        if not _finite_pos(self.fill_price):
            raise ValueError("fill_price must be finite and > 0")
        if not isfinite(self.fill_qty) or self.fill_qty <= 0:
            raise ValueError("fill_qty must be finite and > 0")
        if self.fill_qty > remaining + eps:
            raise ValueError("fill_qty exceeds remaining")


class FillModel(Protocol):
    """
    A deterministic fill model.
    """

    def decide(self, *, order: OrderRecord, bbo: BestBidAskUpdate) -> FillDecision:
        ...


@dataclass(frozen=True, slots=True)
class TopOfBookFullFillModel:
    """
    Deterministic top-of-book full-fill model.

    Rules:
      - Limit buy executes when price >= ask, fills at ask.
      - Limit sell executes when price <= bid, fills at bid.
      - Full fill: fill_qty = remaining.
      - No market orders here (price None -> not executable).
    """
    eps: float = 1e-12

    def decide(self, *, order: OrderRecord, bbo: BestBidAskUpdate) -> FillDecision:
        if order.status != "open":
            return FillDecision(executable=False)

        if order.price is None:
            return FillDecision(executable=False)

        if not isfinite(order.price) or not isfinite(order.remaining):
            return FillDecision(executable=False)

        bid = bbo.bid_price
        ask = bbo.ask_price
        if not _finite_pos(bid) or not _finite_pos(ask):
            return FillDecision(executable=False)

        if order.remaining <= self.eps:
            return FillDecision(executable=False)

        if order.side == "buy":
            if order.price + self.eps >= ask:
                d = FillDecision(executable=True, fill_price=ask, fill_qty=order.remaining)
                d.validate(remaining=order.remaining, eps=self.eps)
                return d
            return FillDecision(executable=False)

        # sell
        if order.price - self.eps <= bid:
            d = FillDecision(executable=True, fill_price=bid, fill_qty=order.remaining)
            d.validate(remaining=order.remaining, eps=self.eps)
            return d
        return FillDecision(executable=False)


@dataclass(frozen=True, slots=True)
class TopOfBookCappedFillModel:
    """
    Deterministic top-of-book capped fill model.

    Like TopOfBookFullFillModel, but the fill quantity is capped by displayed
    top-of-book size (still deterministic, still simple).

    Rules:
      - Buy executes when price >= ask, fills at ask, qty=min(remaining, ask_size)
      - Sell executes when price <= bid, fills at bid, qty=min(remaining, bid_size)
    """
    eps: float = 1e-12

    def decide(self, *, order: OrderRecord, bbo: BestBidAskUpdate) -> FillDecision:
        if order.status != "open":
            return FillDecision(executable=False)

        if order.price is None:
            return FillDecision(executable=False)

        if not isfinite(order.price) or not isfinite(order.remaining):
            return FillDecision(executable=False)

        bid = bbo.bid_price
        ask = bbo.ask_price
        if not _finite_pos(bid) or not _finite_pos(ask):
            return FillDecision(executable=False)

        if order.remaining <= self.eps:
            return FillDecision(executable=False)

        if not isfinite(bbo.bid_size) or not isfinite(bbo.ask_size):
            return FillDecision(executable=False)

        if order.side == "buy":
            if order.price + self.eps >= ask and bbo.ask_size > self.eps:
                qty = min(order.remaining, bbo.ask_size)
                d = FillDecision(executable=True, fill_price=ask, fill_qty=qty)
                d.validate(remaining=order.remaining, eps=self.eps)
                return d
            return FillDecision(executable=False)

        # sell
        if order.price - self.eps <= bid and bbo.bid_size > self.eps:
            qty = min(order.remaining, bbo.bid_size)
            d = FillDecision(executable=True, fill_price=bid, fill_qty=qty)
            d.validate(remaining=order.remaining, eps=self.eps)
            return d
        return FillDecision(executable=False)
