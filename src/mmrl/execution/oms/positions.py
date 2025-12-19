from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Position:
    symbol: str
    inventory: float = 0.0          # + means long, - means short
    avg_price: float = 0.0          # average entry price for current inventory

    def on_fill(self, *, side: str, qty: float, price: float) -> None:
        if qty <= 0:
            raise ValueError("qty must be > 0")
        if price <= 0:
            raise ValueError("price must be > 0")

        signed = qty if side == "buy" else -qty

        # If inventory is zero, reset avg_price to fill price
        if abs(self.inventory) < 1e-12:
            self.inventory = signed
            self.avg_price = price
            return

        # Same direction: update weighted avg
        if (self.inventory > 0 and signed > 0) or (self.inventory < 0 and signed < 0):
            new_inv = self.inventory + signed
            self.avg_price = (self.avg_price * abs(self.inventory) + price * abs(signed)) / abs(new_inv)
            self.inventory = new_inv
            return

        # Opposite direction: reduce position (avg stays for remaining)
        new_inv = self.inventory + signed
        self.inventory = new_inv
        if abs(self.inventory) < 1e-12:
            self.inventory = 0.0
            self.avg_price = 0.0
