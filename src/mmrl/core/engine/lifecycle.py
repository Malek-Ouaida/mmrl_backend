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

        bind_context(run_id=self._state.run_id, component="engine")

        # Reset deterministic counters
        self._state.tick = 0
        self._state.sequence = 0

        # Enter running state FIRST (sequence/tick guards depend on this)
        self._state.is_running = True

        # Allocate a sequence for RunStarted (required by Event schema)
        seq = self._state.next_sequence()

        self._bus.publish(
            RunStarted.create(
                run_id=self._state.run_id,
                sequence=seq,
            )
        )

        log.info("engine.started", run_id=self._state.run_id)

    def stop(self) -> None:
        if not self._state.is_running:
            raise RuntimeError("engine not running")

        # Allocate sequence while still running
        seq = self._state.next_sequence()

        # Transition to stopped
        self._state.is_running = False

        self._bus.publish(
            RunStopped.create(
                run_id=self._state.run_id,
                sequence=seq,
            )
        )

        log.info("engine.stopped", run_id=self._state.run_id)
