# src/mmrl/execution/oms/positions.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Position:
    symbol: str
    inventory: float = 0.0          # + long, - short
    avg_price: float = 0.0          # average entry for current inventory
    realized_pnl: float = 0.0       # realized PnL in quote currency

    _EPS: float = 1e-12

    def on_fill(self, *, side: str, qty: float, price: float) -> None:
        """
        Institutional position accounting:
        - realized PnL computed when reducing/closing
        - flip handled: close remainder at old avg, open new at fill price
        """
        if qty <= 0:
            raise ValueError("qty must be > 0")
        if price <= 0:
            raise ValueError("price must be > 0")

        signed = qty if side == "buy" else -qty

        # If flat -> open new
        if abs(self.inventory) <= self._EPS:
            self.inventory = signed
            self.avg_price = price
            return

        inv = self.inventory

        # Same direction: update weighted average
        if (inv > 0 and signed > 0) or (inv < 0 and signed < 0):
            new_inv = inv + signed
            self.avg_price = (self.avg_price * abs(inv) + price * abs(signed)) / abs(new_inv)
            self.inventory = new_inv
            return

        # Opposite direction: reduce or flip
        close_qty = min(abs(inv), abs(signed))
        if inv > 0:
            # selling into a long
            self.realized_pnl += (price - self.avg_price) * close_qty
        else:
            # buying into a short
            self.realized_pnl += (self.avg_price - price) * close_qty

        new_inv = inv + signed
        self.inventory = new_inv

        # Fully closed
        if abs(self.inventory) <= self._EPS:
            self.inventory = 0.0
            self.avg_price = 0.0
            return

        # Flipped: remaining opens a new position at fill price
        if (inv > 0 and self.inventory < 0) or (inv < 0 and self.inventory > 0):
            self.avg_price = price
