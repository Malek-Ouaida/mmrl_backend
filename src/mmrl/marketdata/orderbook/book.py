from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from mmrl.core.events.marketdata import OrderBookLevelUpdate


@dataclass(frozen=True, slots=True)
class BestBidAsk:
    bid_price: float | None
    bid_size: float | None
    ask_price: float | None
    ask_size: float | None


class OrderBook:
    """
    Deterministic in-memory L2 order book for a single symbol.

    Maintains:
      - bids: price -> size
      - asks: price -> size

    Conventions:
      - size == 0 => remove level
      - bid is max price on bid side
      - ask is min price on ask side
    """

    def __init__(self, *, symbol: str) -> None:
        self.symbol = symbol
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}

        self._best_bid: float | None = None
        self._best_ask: float | None = None

    # ---------- Public API ----------

    def apply_level_update(self, u: OrderBookLevelUpdate) -> None:
        if u.symbol != self.symbol:
            raise ValueError(f"update symbol mismatch: {u.symbol} != {self.symbol}")
        if u.price <= 0:
            raise ValueError("price must be > 0")
        if u.size < 0:
            raise ValueError("size must be >= 0")

        side = u.side
        price = u.price
        size = u.size

        if side == "bid":
            self._apply(self._bids, price, size, is_bid=True)
        else:
            self._apply(self._asks, price, size, is_bid=False)

        # Optional: enforce no-cross (best_bid <= best_ask) if both exist.
        # Real feeds can momentarily cross depending on update ordering.
        # We'll keep it permissive for now; strategy/execution can handle.
        # (We will add optional strict mode later.)

    def best(self) -> BestBidAsk:
        bid = self._best_bid
        ask = self._best_ask
        return BestBidAsk(
            bid_price=bid,
            bid_size=self._bids.get(bid) if bid is not None else None,
            ask_price=ask,
            ask_size=self._asks.get(ask) if ask is not None else None,
        )

    def top_levels(self, *, side: str, depth: int) -> list[tuple[float, float]]:
        """
        Return top-N levels as (price, size), best-first.
        """
        if depth <= 0:
            raise ValueError("depth must be > 0")
        if side not in ("bid", "ask"):
            raise ValueError("side must be 'bid' or 'ask'")

        book = self._bids if side == "bid" else self._asks
        if not book:
            return []

        prices = sorted(book.keys(), reverse=(side == "bid"))
        out: list[tuple[float, float]] = []
        for p in prices[:depth]:
            out.append((p, book[p]))
        return out

    def levels(self, *, side: str) -> Iterable[tuple[float, float]]:
        """
        Iterate all levels best-first.
        """
        if side not in ("bid", "ask"):
            raise ValueError("side must be 'bid' or 'ask'")
        book = self._bids if side == "bid" else self._asks
        prices = sorted(book.keys(), reverse=(side == "bid"))
        for p in prices:
            yield p, book[p]

    # ---------- Internal helpers ----------

    def _apply(self, book: dict[float, float], price: float, size: float, *, is_bid: bool) -> None:
        # update/remove
        if size == 0.0:
            if price in book:
                del book[price]
        else:
            book[price] = size

        # refresh best price only when needed (simple + correct)
        if is_bid:
            self._best_bid = max(book.keys()) if book else None
        else:
            self._best_ask = min(book.keys()) if book else None
