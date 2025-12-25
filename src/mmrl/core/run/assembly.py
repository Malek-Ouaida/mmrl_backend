from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Sequence

import structlog

from mmrl.core.engine.lifecycle import EngineLifecycle
from mmrl.core.engine.router import EngineRouter, RouterWiring
from mmrl.core.engine.state import EngineState
from mmrl.core.engine.tick_driver import TickDriverComponent  # ✅ NEW
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.run.artifacts import RunArtifacts, artifacts_for
from mmrl.storage.jsonl import JsonlEventStore

from mmrl.execution.paper.adapter import PaperExecutionAdapter
from mmrl.execution.oms.risk import RiskLimits
from mmrl.execution.oms.risk_component import RiskInventoryComponent
from mmrl.marketdata.orderbook.adapter import OrderBookComponent
from mmrl.marketdata.replay.adapter import ReplayMarketDataAdapter
from mmrl.marketdata.replay.datasource import ReplayDataSource
from mmrl.strategies.baselines.fixed_spread import FixedSpreadConfig, FixedSpreadMarketMaker

log = structlog.get_logger()

RunMode = Literal["paper_replay_l2", "paper_external_bbo", "paper_no_marketdata"]


@dataclass(slots=True)
class EventLogComponent:
    """
    Deterministic, append-only event persistence to events.jsonl.
    """
    store: JsonlEventStore

    event_types: tuple[str, ...] = (
        "system.run_started",
        "system.run_stopped",
        "system.engine_tick",
        "system.engine_error",
        "market.best_bid_ask",
        "market.order_book_level",
        "market.trade",
        "order.submitted",
        "order.accepted",
        "order.rejected",
        "order.cancel_requested",
        "order.canceled",
        "order.fill",
    )

    def subscriptions(self) -> Sequence[tuple[str, callable]]:
        return [(et, self._on_event) for et in self.event_types]

    def _on_event(self, e: Event) -> None:
        self.store.append(e)


@dataclass(frozen=True, slots=True)
class RunHandle:
    """
    Canonical handle for a wired run in this process.

    - risk_component is exposed so API can persist risk_inventory artifacts on stop
    - max_inventory is a stable, deterministic threshold for summary metrics
    """
    run_id: str
    artifacts: RunArtifacts
    bus: EventBus
    state: EngineState
    lifecycle: EngineLifecycle
    wiring: RouterWiring
    components: tuple[object, ...]
    risk_component: RiskInventoryComponent
    max_inventory: float


def build_run(
    *,
    runs_dir: Path,
    run_id: str,
    mode: RunMode,
    symbol: str,
    strategy_cfg: FixedSpreadConfig,
    replay_l2: ReplayDataSource | None = None,
    extra_components: Iterable[object] = (),
) -> RunHandle:
    art = artifacts_for(runs_dir=runs_dir, run_id=run_id)
    art.ensure_dirs()
    art.events_jsonl.touch(exist_ok=True)

    bus = EventBus()
    state = EngineState(run_id=run_id)
    lifecycle = EngineLifecycle(bus=bus, state=state)

    event_store = JsonlEventStore(path=art.events_jsonl, fsync=True)
    eventlog = EventLogComponent(store=event_store)

    if strategy_cfg.symbol != symbol:
        raise ValueError(f"strategy_cfg.symbol={strategy_cfg.symbol!r} must match symbol={symbol!r}")

    strat = FixedSpreadMarketMaker(bus=bus, state=state, cfg=strategy_cfg)
    execution = PaperExecutionAdapter(bus=bus, state=state)

    # ✅ Institutional risk/inventory collector
    risk_component = RiskInventoryComponent(
        limits=RiskLimits(
            max_order_qty=strategy_cfg.order_size,
            max_abs_inventory=strategy_cfg.max_inventory,
            max_order_notional=None,
        )
    )

    components: list[object] = [eventlog]

    if mode == "paper_replay_l2":
        if replay_l2 is None:
            raise ValueError("replay_l2 datasource is required for mode='paper_replay_l2'")

        # ✅ NEW: tick driver so replay/OB/strategy actually runs
        tick_driver = TickDriverComponent(bus=bus, state=state, max_ticks=500)

        md_replay = ReplayMarketDataAdapter(bus=bus, state=state, datasource=replay_l2)
        ob = OrderBookComponent(bus=bus, state=state, symbol=symbol)

        # Deterministic chain:
        # RunStarted -> TickDriver emits EngineTick -> md_replay emits L2 -> ob emits BBO
        # -> strategy quotes -> execution fills -> risk records series
        components.extend([tick_driver, md_replay, ob, strat, execution, risk_component])

    elif mode == "paper_external_bbo":
        components.extend([strat, execution, risk_component])

    elif mode == "paper_no_marketdata":
        components.extend([strat, execution, risk_component])

    else:
        raise ValueError(f"unknown mode: {mode!r}")

    for c in extra_components:
        components.append(c)

    wiring = EngineRouter(bus=bus).register(components)

    log.info(
        "run.assembled",
        run_id=run_id,
        mode=mode,
        symbol=symbol,
        components=[type(c).__name__ for c in components],
        artifacts_dir=str(art.run_dir),
    )

    return RunHandle(
        run_id=run_id,
        artifacts=art,
        bus=bus,
        state=state,
        lifecycle=lifecycle,
        wiring=wiring,
        components=tuple(components),
        risk_component=risk_component,
        max_inventory=strategy_cfg.max_inventory,
    )
