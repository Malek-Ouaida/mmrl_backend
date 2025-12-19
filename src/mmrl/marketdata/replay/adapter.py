from __future__ import annotations

import structlog

from mmrl.core.engine.state import EngineState
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.events.system import EngineTick
from mmrl.marketdata.orderbook.delta import OrderBookDelta
from mmrl.marketdata.replay.datasource import ReplayDataSource
from mmrl.core.events.marketdata import OrderBookLevelUpdate


log = structlog.get_logger()


class ReplayMarketDataAdapter:
    """
    Market data replay adapter.

    Drives order book deltas into the event system deterministically by:
    - consuming one delta per engine tick
    - converting delta -> OrderBookLevelUpdate events
    - emitting events with globally monotonic sequence numbers
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        state: EngineState,
        datasource: ReplayDataSource,
    ) -> None:
        self._bus = bus
        self._state = state
        self._it = iter(datasource)
        self._exhausted = False

    def subscriptions(self):
        return [("system.engine_tick", self._on_tick)]

    def _on_tick(self, e: Event) -> None:
        if self._exhausted:
            return

        tick = e.tick if isinstance(e, EngineTick) else None
        if tick is None:
            return

        try:
            delta: OrderBookDelta = next(self._it)
        except StopIteration:
            self._exhausted = True
            log.info("replay.exhausted", run_id=self._state.run_id, tick=tick)
            return

        # Use engine's global sequence to ensure deterministic ordering across all events
        # Allocate deterministic sequences through EngineState
        events = []
        for u in delta.bid_updates:
            events.append(
                OrderBookLevelUpdate.create(
                    symbol=delta.symbol,
                    side="bid",
                    price=u.price,
                    size=u.size,
                    sequence=self._state.next_sequence(),
                )
            )
        for u in delta.ask_updates:
            events.append(
                OrderBookLevelUpdate.create(
                    symbol=delta.symbol,
                    side="ask",
                    price=u.price,
                    size=u.size,
                    sequence=self._state.next_sequence(),
                )
            )

        for evt in events:
            self._bus.publish(evt)

        log.debug(
            "replay.delta_published",
            run_id=self._state.run_id,
            tick=tick,
            symbol=delta.symbol,
            updates=len(events),
        )
