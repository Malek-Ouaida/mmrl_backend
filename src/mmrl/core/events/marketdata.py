# src/mmrl/core/events/marketdata.py
from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Literal

from mmrl.core.events.base import Event

Side = Literal["bid", "ask"]


@dataclass(frozen=True, slots=True)
class BestBidAskUpdate(Event):
    """
    Level 1 market data update (best bid / ask).
    """
    event_type: ClassVar[str] = "market.best_bid_ask"

    symbol: str

    bid_price: float
    bid_size: float

    ask_price: float
    ask_size: float


@dataclass(frozen=True, slots=True)
class OrderBookLevelUpdate(Event):
    """
    Level 2 incremental order book update.
    """
    event_type: ClassVar[str] = "market.order_book_level"

    symbol: str

    side: Side
    price: float
    size: float


@dataclass(frozen=True, slots=True)
class TradePrint(Event):
    """
    Executed trade event.
    """
    event_type: ClassVar[str] = "market.trade"

    symbol: str

    price: float
    size: float

    aggressor_side: Side
