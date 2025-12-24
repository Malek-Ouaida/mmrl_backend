from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Literal


Side = Literal["buy", "sell"]


@dataclass(frozen=True, slots=True)
class RiskLimits:
    """
    Risk limits for the execution/OMS layer.

    These should be enforceable deterministically.
    """
    max_order_qty: float
    max_abs_inventory: float

    # Optional, but founder-grade to have:
    # If None -> not enforced
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

        # Track reservations by order_id so cancels/fills can release deterministically
        self._reservation_by_order_id: dict[str, tuple[str, float]] = {}  # order_id -> (symbol, signed_qty)

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

    def on_fill(self, *, symbol: str, side: Side, qty: float, order_id: str | None = None) -> None:
        """
        Update inventory state from a fill.
        If order_id is provided and was reserved, release reservation proportionally
        (for now we assume full-fill models; partial fills can be added later).
        """
        if not self._validate_qty(qty):
            raise ValueError("qty must be finite and > 0")

        signed = self._signed(side=side, qty=qty)
        self._inventory_by_symbol[symbol] = self.inventory(symbol=symbol) + signed

        # Release reservation if this fill corresponds to a reserved order
        if order_id is not None and order_id in self._reservation_by_order_id:
            rsym, rqty = self._reservation_by_order_id.pop(order_id)
            if rsym == symbol:
                self._reserved_by_symbol[symbol] = self.reserved(symbol=symbol) - rqty

                # Clean near-zero drift
                if abs(self._reserved_by_symbol[symbol]) <= self._EPS:
                    self._reserved_by_symbol[symbol] = 0.0

    def on_cancel(self, *, order_id: str) -> None:
        """
        Release reservation for a canceled order.
        """
        rec = self._reservation_by_order_id.pop(order_id, None)
        if rec is None:
            return
        symbol, signed_qty = rec
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
        """
        Validate a new order request *before* accepting it.

        Checks:
          - qty finite positive
          - qty <= max_order_qty
          - (optional) notional <= max_order_notional when price provided
          - projected inventory INCLUDING RESERVED exposure does not exceed max_abs_inventory

        Note: conservative: assumes full fill.
        """
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

        # If an order_id is provided, reserve immediately (idempotent)
        if order_id is not None and order_id not in self._reservation_by_order_id:
            self._reservation_by_order_id[order_id] = (symbol, signed)
            self._reserved_by_symbol[symbol] = reserved + signed

        return RiskCheckResult(ok=True, reason="ok")
