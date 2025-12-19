from __future__ import annotations

from dataclasses import dataclass, field

from mmrl.core.engine.engine import Engine
from mmrl.core.engine.router import EngineRouter
from mmrl.core.engine.state import EngineState
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.events.marketdata import BestBidAskUpdate
from mmrl.core.events.orders import (
    Fill,
    OrderCanceled,
    OrderCancelRequested,
    OrderSubmitted,
)
from mmrl.execution.paper.adapter import PaperExecutionAdapter
from mmrl.strategies.baselines.fixed_spread import FixedSpreadConfig, FixedSpreadMarketMaker


@dataclass(slots=True)
class BBOReplayAdapter:
    """
    Deterministic BBO stream for integration tests.

    On each engine tick:
      - emits one BestBidAskUpdate (bid/ask from steps[i])
    """
    bus: EventBus
    state: EngineState
    steps: list[tuple[float, float]]
    bid_size: float = 10.0
    ask_size: float = 10.0
    _i: int = 0

    def subscriptions(self):
        return [("system.engine_tick", self._on_tick)]

    def _on_tick(self, e: Event) -> None:
        if self._i >= len(self.steps):
            return

        bid, ask = self.steps[self._i]
        self._i += 1

        self.bus.publish(
            BestBidAskUpdate.create(
                symbol="BTCUSDT",
                bid_price=bid,
                bid_size=self.bid_size,
                ask_price=ask,
                ask_size=self.ask_size,
                sequence=self.state.next_sequence(),
            )
        )


@dataclass(slots=True)
class CancelOnlyExecutionAdapter:
    """
    Deterministic execution stub for cancel/replace tests.

    - Tracks open orders on OrderSubmitted (no fills, ever).
    - On OrderCancelRequested emits OrderCanceled immediately.
    """
    bus: EventBus
    state: EngineState
    _open: set[str] = field(default_factory=set)

    def subscriptions(self):
        return [
            ("order.submitted", self._on_submitted),
            ("order.cancel_requested", self._on_cancel_requested),
        ]

    def _on_submitted(self, e: Event) -> None:
        if isinstance(e, OrderSubmitted):
            self._open.add(e.order_id)

    def _on_cancel_requested(self, e: Event) -> None:
        if not isinstance(e, OrderCancelRequested):
            return
        if e.order_id not in self._open:
            return

        self._open.remove(e.order_id)

        self.bus.publish(
            OrderCanceled.create(
                symbol=e.symbol,
                order_id=e.order_id,
                sequence=self.state.next_sequence(),
            )
        )


class Collector:
    def __init__(self) -> None:
        self.submitted: list[OrderSubmitted] = []
        self.cancel_requested: list[OrderCancelRequested] = []
        self.canceled: list[OrderCanceled] = []
        self.fills: list[Fill] = []

    def subscriptions(self):
        return [
            ("order.submitted", self._on_submitted),
            ("order.cancel_requested", self._on_cancel_requested),
            ("order.canceled", self._on_canceled),
            ("order.fill", self._on_fill),
        ]

    def _on_submitted(self, e: Event) -> None:
        if isinstance(e, OrderSubmitted):
            self.submitted.append(e)

    def _on_cancel_requested(self, e: Event) -> None:
        if isinstance(e, OrderCancelRequested):
            self.cancel_requested.append(e)

    def _on_canceled(self, e: Event) -> None:
        if isinstance(e, OrderCanceled):
            self.canceled.append(e)

    def _on_fill(self, e: Event) -> None:
        if isinstance(e, Fill):
            self.fills.append(e)


def _run(
    steps: list[tuple[float, float]],
    *,
    spread: float,
    use_cancel_only_execution: bool,
) -> Collector:
    bus = EventBus()
    engine = Engine(run_id="test", bus=bus)

    # feed BBO directly (no orderbook accumulation => no crossed-book artifact)
    md = BBOReplayAdapter(bus=bus, state=engine.state, steps=steps)

    if use_cancel_only_execution:
        execution = CancelOnlyExecutionAdapter(bus=bus, state=engine.state)
    else:
        execution = PaperExecutionAdapter(bus=bus, state=engine.state)

    # IMPORTANT: disable throttling for tests
    strat = FixedSpreadMarketMaker(
        bus=bus,
        state=engine.state,
        cfg=FixedSpreadConfig(
            symbol="BTCUSDT",
            spread=spread,
            order_size=0.1,
            max_inventory=10.0,
            inventory_skew_k=0.0,
            min_mid_move=0.0,
            min_ticks_between_quotes=0,
        ),
    )

    col = Collector()
    EngineRouter(bus=bus).register([md, strat, execution, col])

    engine.run(max_ticks=len(steps))
    return col


def test_cancel_replace_emits_cancels_and_replacements() -> None:
    """
    Pure cancel/replace behavior, no fills.

    We use CancelOnlyExecutionAdapter so we only test:
      quote -> cancel -> canceled -> replacement submit
    """
    steps = [
        (100.0, 101.0),
        (110.0, 111.0),
        (120.0, 121.0),
        (130.0, 131.0),
    ]

    col = _run(steps, spread=0.6, use_cancel_only_execution=True)

    # Tick1: 2 submits (bid+ask)
    # Tick2+: cancel+replace cycles -> more submits + cancels
    assert len(col.submitted) >= 4, f"submitted={len(col.submitted)}"
    assert len(col.cancel_requested) >= 2, f"cancel_requested={len(col.cancel_requested)}"
    assert len(col.canceled) >= 2, f"canceled={len(col.canceled)}"

    # no fills by design
    assert len(col.fills) == 0

    submitted_ids = {o.order_id for o in col.submitted}
    assert all(cr.order_id in submitted_ids for cr in col.cancel_requested)
    assert all(c.order_id in submitted_ids for c in col.canceled)


def test_paper_execution_can_fill() -> None:
    steps = [
        (100.0, 101.0),
        (100.0, 101.0),
        (100.0, 101.0),
        (100.0, 101.0),
    ]

    # more aggressive => marketable on both sides
    col = _run(steps, spread=-2.0, use_cancel_only_execution=False)

    assert len(col.fills) >= 1, "expected at least one fill"
    submitted_ids = {o.order_id for o in col.submitted}
    assert all(f.order_id in submitted_ids for f in col.fills)

