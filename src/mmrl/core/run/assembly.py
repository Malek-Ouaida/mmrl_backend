from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Sequence

import structlog

from mmrl.core.engine.lifecycle import EngineLifecycle
from mmrl.core.engine.router import EngineRouter, RouterWiring
from mmrl.core.engine.state import EngineState
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.run.artifacts import RunArtifacts, artifacts_for
from mmrl.storage.jsonl import JsonlEventStore

from mmrl.execution.paper.adapter import PaperExecutionAdapter
from mmrl.marketdata.orderbook.adapter import OrderBookComponent
from mmrl.marketdata.replay.adapter import ReplayMarketDataAdapter
from mmrl.marketdata.replay.datasource import ReplayDataSource
from mmrl.strategies.baselines.fixed_spread import FixedSpreadConfig, FixedSpreadMarketMaker

log = structlog.get_logger()


RunMode = Literal["paper_replay_l2", "paper_external_bbo", "paper_no_marketdata"]
# - paper_replay_l2: EngineTick -> ReplayMarketDataAdapter -> L2 events -> OrderBookComponent -> BBO
# - paper_external_bbo: you provide a component that emits market.best_bid_ask (e.g., in tests)
# - paper_no_marketdata: strategy/execution can still be wired for non-md tests


# ---------------------------
# Event persistence component
# ---------------------------

@dataclass(slots=True)
class EventLogComponent:
    """
    Deterministic, append-only event persistence.

    Writes Event.to_dict()-equivalent content via JsonlEventStore's dataclass serialization,
    ensuring events.jsonl is the durable truth log for replay/evaluation.

    We subscribe to explicit event types (bus has no wildcard support).
    """
    store: JsonlEventStore

    # Keep this explicit and stable (artifact contract)
    event_types: tuple[str, ...] = (
        # system
        "system.run_started",
        "system.run_stopped",
        "system.engine_tick",
        "system.engine_error",
        # market
        "market.best_bid_ask",
        "market.order_book_level",
        "market.trade",
        # orders
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


# ---------------------------
# Assembly result handle
# ---------------------------

@dataclass(frozen=True, slots=True)
class RunHandle:
    """
    Canonical handle for a wired run in this process.

    - artifacts: stable run folder contract
    - bus/state/lifecycle: core engine control plane
    - wiring: debug view of what was wired + in what order
    - components: the instantiated components (useful for tests/introspection)
    """
    run_id: str
    artifacts: RunArtifacts
    bus: EventBus
    state: EngineState
    lifecycle: EngineLifecycle
    wiring: RouterWiring
    components: tuple[object, ...]


# ---------------------------
# Assembly builder
# ---------------------------

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
    """
    Build and wire a run deterministically.

    This is intended to be the *single source of truth* for:
      - API start/stop
      - test wiring
      - future CLI/backtest harness

    Parameters
    ----------
    runs_dir:
        Root runs directory (settings.runs_dir)
    run_id:
        Existing run_id that already has artifacts folder (created by RunManager)
    mode:
        See RunMode docs above.
    symbol:
        Trading symbol (e.g., "BTCUSDT")
    strategy_cfg:
        FixedSpreadConfig for baseline strategy (can be swapped later for other strategies)
    replay_l2:
        Required for mode="paper_replay_l2" (feeds OrderBook deltas)
    extra_components:
        Optional additional components to wire last (collectors, telemetry, etc.)

    Returns
    -------
    RunHandle
    """
    art = artifacts_for(runs_dir=runs_dir, run_id=run_id)
    art.ensure_dirs()
    art.events_jsonl.touch(exist_ok=True)

    bus = EventBus()
    state = EngineState(run_id=run_id)
    lifecycle = EngineLifecycle(bus=bus, state=state)

    # Single-source event log (truth)
    event_store = JsonlEventStore(path=art.events_jsonl, fsync=True)
    eventlog = EventLogComponent(store=event_store)

    # Strategy
    if strategy_cfg.symbol != symbol:
        # keep explicit; avoids subtle mismatch bugs
        raise ValueError(f"strategy_cfg.symbol={strategy_cfg.symbol!r} must match symbol={symbol!r}")

    strat = FixedSpreadMarketMaker(
        bus=bus,
        state=state,
        cfg=strategy_cfg,
    )

    # Execution (paper)
    execution = PaperExecutionAdapter(bus=bus, state=state)

    components: list[object] = [eventlog]

    # Marketdata wiring by mode
    if mode == "paper_replay_l2":
        if replay_l2 is None:
            raise ValueError("replay_l2 datasource is required for mode='paper_replay_l2'")

        md_replay = ReplayMarketDataAdapter(bus=bus, state=state, datasource=replay_l2)
        ob = OrderBookComponent(bus=bus, state=state, symbol=symbol)

        # Deterministic chain:
        # EngineTick -> md_replay emits L2 -> ob emits BBO -> strategy quotes -> execution fills
        components.extend([md_replay, ob, strat, execution])

    elif mode == "paper_external_bbo":
        # You will provide an extra component that emits market.best_bid_ask
        # (e.g., your BBOReplayAdapter in integration tests).
        components.extend([strat, execution])

    elif mode == "paper_no_marketdata":
        # Useful for order-flow/unit wiring tests (no quoting expected)
        components.extend([strat, execution])

    else:
        raise ValueError(f"unknown mode: {mode!r}")

    # Append extra components LAST (collectors, debugging, etc.)
    for c in extra_components:
        components.append(c)

    # Wire deterministically in listed order
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
    )
