from __future__ import annotations

from dataclasses import dataclass

from mmrl.core.engine.engine import Engine
from mmrl.core.engine.router import EngineRouter
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.events.orders import OrderRejected, OrderSubmitted
from mmrl.execution.oms.risk import RiskLimits, RiskManager
from mmrl.execution.paper.adapter import PaperExecutionAdapter


@dataclass(slots=True)
class Submitter:
    """
    On first tick, submit a single order (deterministic).
    """
    bus: EventBus
    engine: Engine

    def subscriptions(self):
        return [("system.engine_tick", self._on_tick)]

    def _on_tick(self, e: Event) -> None:
        # submit only once
        if self.engine.state.tick != 1:
            return

        self.bus.publish(
            OrderSubmitted.create(
                symbol="BTCUSDT",
                order_id="risk_test_order",
                side="buy",
                order_type="limit",
                time_in_force="GTC",
                price=100.0,
                quantity=10.0,  # this should violate max_order_qty below
                sequence=self.engine.state.next_sequence(),
            )
        )


class Collector:
    def __init__(self) -> None:
        self.rejected: list[OrderRejected] = []

    def subscriptions(self):
        return [("order.rejected", self._on_rejected)]

    def _on_rejected(self, e: Event) -> None:
        if isinstance(e, OrderRejected):
            self.rejected.append(e)


def test_risk_rejects_large_order() -> None:
    bus = EventBus()
    engine = Engine(run_id="test", bus=bus)

    # strict limits: reject qty > 1
    risk = RiskManager(limits=RiskLimits(max_order_qty=1.0, max_abs_inventory=100.0))

    execution = PaperExecutionAdapter(bus=bus, state=engine.state, risk=risk)
    submitter = Submitter(bus=bus, engine=engine)
    col = Collector()

    EngineRouter(bus=bus).register([submitter, execution, col])

    engine.run(max_ticks=2)

    assert len(col.rejected) == 1
    assert col.rejected[0].order_id == "risk_test_order"
    assert col.rejected[0].reason == "qty_exceeds_max_order_qty"
