# src/mmrl/core/events/orders.py
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Literal

from mmrl.core.events.base import Event

OrderSide = Literal["buy", "sell"]
OrderType = Literal["limit", "market"]
TimeInForce = Literal["GTC", "IOC", "FOK"]


@dataclass(frozen=True, slots=True)
class OrderSubmitted(Event):
    """
    An order request produced by the strategy/engine, intended for execution.

    This is the intent to trade, before any venue acknowledgement.
    """
    event_type: ClassVar[str] = "order.submitted"

    symbol: str
    order_id: str

    side: OrderSide
    order_type: OrderType
    time_in_force: TimeInForce

    price: float | None
    quantity: float


@dataclass(frozen=True, slots=True)
class OrderCancelRequested(Event):
    """
    Strategy requests cancellation of an existing open order.
    """
    event_type: ClassVar[str] = "order.cancel_requested"

    symbol: str
    order_id: str


@dataclass(frozen=True, slots=True)
class OrderAccepted(Event):
    """
    Venue/execution layer acknowledged the order (it is now live exposure).
    """
    event_type: ClassVar[str] = "order.accepted"

    symbol: str
    order_id: str

    # Important: include details so risk can reserve on accept (institutional)
    side: OrderSide
    price: float | None
    quantity: float


@dataclass(frozen=True, slots=True)
class OrderRejected(Event):
    """
    Venue/execution layer rejected the order.
    """
    event_type: ClassVar[str] = "order.rejected"

    symbol: str
    order_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class OrderCanceled(Event):
    """
    Venue/execution layer confirmed the order is canceled.
    """
    event_type: ClassVar[str] = "order.canceled"

    symbol: str
    order_id: str


@dataclass(frozen=True, slots=True)
class Fill(Event):
    """
    A fill (partial or full) against an existing order.
    """
    event_type: ClassVar[str] = "order.fill"

    symbol: str
    order_id: str

    side: OrderSide  # required for inventory/PnL correctness

    fill_price: float
    fill_quantity: float

    # Remaining open quantity AFTER this fill (key for reservation release)
    remaining_quantity: float

    # Optional but founder-grade (keep default so it doesn't break paper adapters)
    fee: float = 0.0  # absolute fee paid in quote currency
    liquidity: Literal["maker", "taker"] | None = None
