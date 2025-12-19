from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from mmrl.core.events.orders import OrderSide


OrderStatus = Literal["open", "filled", "canceled", "rejected"]


@dataclass(slots=True)
class OrderRecord:
    symbol: str
    order_id: str
    side: OrderSide

    price: float | None
    quantity: float
    remaining: float

    status: OrderStatus = "open"

    def apply_fill(self, *, fill_qty: float) -> None:
        if self.status != "open":
            return
        if fill_qty <= 0:
            raise ValueError("fill_qty must be > 0")
        if fill_qty > self.remaining + 1e-12:
            raise ValueError("fill_qty exceeds remaining")

        self.remaining -= fill_qty
        if self.remaining <= 1e-12:
            self.remaining = 0.0
            self.status = "filled"
    def cancel(self) -> None:
        if self.status != "open":
            return
        self.status = "canceled"
        self.remaining = 0.0
