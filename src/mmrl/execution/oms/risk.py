# src/mmrl/execution/oms/risk.py
from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Literal

Side = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_order_qty: float
    max_abs_inventory: float
    max_order_notional: float | None = None


@dataclass(frozen=True, slots=True)
class RiskCheckResult:
    ok: bool
    reason: str  # machine-friendly code


class RiskManager:
    """
    Deterministic risk manager.

    Tracks:
      - realized inventory by symbol (from fills)
      - reserved inventory by symbol (from accepted open orders)
        so multiple in-flight orders cannot bypass limits.
    """

    _EPS: float = 1e-12

    def __init__(self, *, limits: RiskLimits) -> None:
        self._limits = limits
        self._inventory_by_symbol: dict[str, float] = {}
        self._reserved_by_symbol: dict[str, float] = {}

        # order_id -> (symbol, side, reserved_qty_abs)
        # reserved_qty_abs represents remaining open qty (conservative full-fill assumption for that remainder)
        self._reservation_by_order_id: dict[str, tuple[str, Side, float]] = {}

    def inventory(self, *, symbol: str) -> float:
        return self._inventory_by_symbol.get(symbol, 0.0)

    def reserved(self, *, symbol: str) -> float:
        return self._reserved_by_symbol.get(symbol, 0.0)

    def _signed(self, *, side: Side, qty: float) -> float:
        return qty if side == "buy" else -qty

    def _validate_qty(self, qty: float) -> bool:
        return isfinite(qty) and qty > self._EPS

    def _validate_price(self, price: float | None) -> bool:
        return price is None or (isfinite(price) and price > 0.0)

    def on_fill(
        self,
        *,
        symbol: str,
        side: Side,
        qty: float,
        order_id: str | None = None,
        remaining_qty: float | None = None,
    ) -> None:
        """
        Update inventory from a fill.

        Institutional fix:
        - release reservation correctly for partial fills using remaining_qty (from Fill event).
        - deterministic + idempotent behavior.
        """
        if not self._validate_qty(qty):
            raise ValueError("qty must be finite and > 0")

        signed_fill = self._signed(side=side, qty=qty)
        self._inventory_by_symbol[symbol] = self.inventory(symbol=symbol) + signed_fill

        if order_id is None:
            return

        rec = self._reservation_by_order_id.get(order_id)
        if rec is None:
            return

        rsym, rside, rqty_abs = rec
        if rsym != symbol:
            return

        # If remaining_qty not provided, fall back to "assume full fill" behavior
        if remaining_qty is None:
            remaining_qty = 0.0

        if remaining_qty < 0:
            remaining_qty = 0.0

        # Update reserved exposure from "old remaining" -> "new remaining"
        old_reserved_signed = self._signed(side=rside, qty=rqty_abs)
        new_reserved_signed = self._signed(side=rside, qty=remaining_qty)

        delta = new_reserved_signed - old_reserved_signed
        self._reserved_by_symbol[symbol] = self.reserved(symbol=symbol) + delta

        # Clean near-zero drift
        if abs(self._reserved_by_symbol[symbol]) <= self._EPS:
            self._reserved_by_symbol[symbol] = 0.0

        # Update or remove reservation record
        if remaining_qty <= self._EPS:
            self._reservation_by_order_id.pop(order_id, None)
        else:
            self._reservation_by_order_id[order_id] = (symbol, rside, remaining_qty)

    def on_cancel(self, *, order_id: str) -> None:
        rec = self._reservation_by_order_id.pop(order_id, None)
        if rec is None:
            return
        symbol, side, qty_abs = rec
        signed_qty = self._signed(side=side, qty=qty_abs)
        self._reserved_by_symbol[symbol] = self.reserved(symbol=symbol) - signed_qty
        if abs(self._reserved_by_symbol[symbol]) <= self._EPS:
            self._reserved_by_symbol[symbol] = 0.0

    def check_new_order(
        self,
        *,
        symbol: str,
        side: Side,
        qty: float,
        price: float | None = None,
        order_id: str | None = None,
    ) -> RiskCheckResult:
        if not self._validate_qty(qty):
            return RiskCheckResult(ok=False, reason="qty_non_positive_or_invalid")

        if qty > self._limits.max_order_qty + self._EPS:
            return RiskCheckResult(ok=False, reason="qty_exceeds_max_order_qty")

        if not self._validate_price(price):
            return RiskCheckResult(ok=False, reason="invalid_price")

        if self._limits.max_order_notional is not None and price is not None:
            notional = qty * price
            if not isfinite(notional) or notional > self._limits.max_order_notional + self._EPS:
                return RiskCheckResult(ok=False, reason="notional_exceeds_max_order_notional")

        inv = self.inventory(symbol=symbol)
        reserved = self.reserved(symbol=symbol)

        signed = self._signed(side=side, qty=qty)
        projected = inv + reserved + signed
        if abs(projected) > self._limits.max_abs_inventory + self._EPS:
            return RiskCheckResult(ok=False, reason="inventory_limit_breach")

        # Reserve remaining open qty for this order idempotently
        if order_id is not None and order_id not in self._reservation_by_order_id:
            self._reservation_by_order_id[order_id] = (symbol, side, qty)
            self._reserved_by_symbol[symbol] = reserved + signed

        return RiskCheckResult(ok=True, reason="ok")
