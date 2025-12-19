from __future__ import annotations

import structlog

from mmrl.core.engine.state import EngineState
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.events.marketdata import BestBidAskUpdate, OrderBookLevelUpdate
from mmrl.marketdata.orderbook.book import OrderBook

log = structlog.get_logger()


class OrderBookComponent:
    """
    Stateful order book component.

    Consumes L2 OrderBookLevelUpdate events and emits L1 BestBidAskUpdate
    when the top-of-book changes.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        state: EngineState,
        symbol: str,
    ) -> None:
        self._bus = bus
        self._state = state
        self._book = OrderBook(symbol=symbol)

        # Track last emitted BBO to avoid noisy duplicates
        self._last_best: tuple[
            float | None,
            float | None,
            float | None,
            float | None,
        ] | None = None

    @property
    def book(self) -> OrderBook:
        return self._book

    def subscriptions(self):
        return [("market.order_book_level", self._on_l2)]

    def _on_l2(self, e: Event) -> None:
        if not isinstance(e, OrderBookLevelUpdate):
            return
        if e.symbol != self._book.symbol:
            return

        # Apply update to book
        self._book.apply_level_update(e)

        best = self._book.best()
        now = (
            best.bid_price,
            best.bid_size,
            best.ask_price,
            best.ask_size,
        )

        # Emit only if top-of-book changed
        if self._last_best == now:
            return
        self._last_best = now

        seq = self._state.next_sequence()
        bbo = BestBidAskUpdate.create(
            symbol=self._book.symbol,
            bid_price=best.bid_price or 0.0,
            bid_size=best.bid_size or 0.0,
            ask_price=best.ask_price or 0.0,
            ask_size=best.ask_size or 0.0,
            sequence=seq,
        )
        self._bus.publish(bbo)

        log.debug(
            "orderbook.bbo_emitted",
            run_id=self._state.run_id,
            symbol=self._book.symbol,
            sequence=seq,
        )
