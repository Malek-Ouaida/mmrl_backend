from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Protocol, Sequence

from mmrl.marketdata.orderbook.delta import OrderBookDelta


class ReplayDataSource(Protocol):
    """
    A deterministic stream of OrderBookDelta items for replay/backtest.
    """

    def __iter__(self) -> Iterator[OrderBookDelta]:
        ...


@dataclass(frozen=True)
class InMemoryReplayDataSource:
    """
    Simple in-memory datasource for tests and early demos.

    This is intentionally strict and deterministic.
    """

    items: Sequence[OrderBookDelta]

    def __iter__(self) -> Iterator[OrderBookDelta]:
        yield from self.items
