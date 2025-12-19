from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from mmrl.core.events.marketdata import OrderBookLevelUpdate


@dataclass(frozen=True, slots=True)
class LevelUpdate:
    price: float
    size: float  # size == 0 => delete


@dataclass(frozen=True, slots=True)
class OrderBookDelta:
    """
    Normalized L2 delta for a single symbol.

    Contains:
      - bid_updates: updates to bid side levels
      - ask_updates: updates to ask side levels

    This type is feed-agnostic (replay or live).
    """

    symbol: str
    bid_updates: tuple[LevelUpdate, ...]
    ask_updates: tuple[LevelUpdate, ...]

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol must be non-empty")

        for u in self.bid_updates:
            _validate_level(u)
        for u in self.ask_updates:
            _validate_level(u)

    def to_events(self, *, start_sequence: int) -> list[OrderBookLevelUpdate]:
        """
        Convert this delta into a deterministic sequence of OrderBookLevelUpdate events.

        We emit all bids first, then all asks, preserving input ordering.
        """
        if start_sequence < 0:
            raise ValueError("start_sequence must be >= 0")

        self.validate()

        seq = start_sequence
        out: list[OrderBookLevelUpdate] = []

        for u in self.bid_updates:
            seq += 1
            out.append(
                OrderBookLevelUpdate.create(
                    symbol=self.symbol,
                    side="bid",
                    price=u.price,
                    size=u.size,
                    sequence=seq,
                )
            )

        for u in self.ask_updates:
            seq += 1
            out.append(
                OrderBookLevelUpdate.create(
                    symbol=self.symbol,
                    side="ask",
                    price=u.price,
                    size=u.size,
                    sequence=seq,
                )
            )

        return out


def _validate_level(u: LevelUpdate) -> None:
    if u.price <= 0:
        raise ValueError("price must be > 0")
    if u.size < 0:
        raise ValueError("size must be >= 0")
