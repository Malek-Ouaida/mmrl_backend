from __future__ import annotations

import structlog

from mmrl.core.engine.state import EngineState
from mmrl.core.events.bus import EventBus
from mmrl.core.events.system import RunStarted, RunStopped
from mmrl.core.logging.setup import bind_context

log = structlog.get_logger()


class EngineLifecycle:
    """
    Explicit engine lifecycle controller.

    Ensures start/stop transitions are correct and audited via events.
    """

    def __init__(self, *, bus: EventBus, state: EngineState) -> None:
        self._bus = bus
        self._state = state

    @property
    def state(self) -> EngineState:
        return self._state

    def start(self) -> None:
        if self._state.is_running:
            raise RuntimeError("engine already running")

        # Bind run_id to structured log context
        bind_context(run_id=self._state.run_id, component="engine")

        self._state.is_running = True
        self._state.tick = 0
        self._state.sequence = 0

        # Emit RunStarted event (sequence-free on purpose; it is a boundary marker)
        event = RunStarted.create(run_id=self._state.run_id)
        self._bus.publish(event)

        log.info("engine.started", run_id=self._state.run_id)

    def stop(self) -> None:
        if not self._state.is_running:
            raise RuntimeError("engine not running")

        self._state.is_running = False

        event = RunStopped.create(run_id=self._state.run_id)
        self._bus.publish(event)

        log.info("engine.stopped", run_id=self._state.run_id)
