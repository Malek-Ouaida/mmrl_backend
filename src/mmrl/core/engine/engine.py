from __future__ import annotations

import structlog

from mmrl.core.engine.lifecycle import EngineLifecycle
from mmrl.core.events.bus import EventBus
from mmrl.core.events.system import EngineError, EngineTick
from mmrl.core.engine.state import EngineState

log = structlog.get_logger()


class Engine:
    """
    Deterministic event-driven engine.

    Runs a tick loop and emits EngineTick events.
    Other components attach via the EventBus.
    """

    def __init__(self, *, run_id: str, bus: EventBus) -> None:
        self._bus = bus
        self._state = EngineState(run_id=run_id)
        self._lifecycle = EngineLifecycle(bus=bus, state=self._state)

    @property
    def bus(self) -> EventBus:
        return self._bus

    @property
    def state(self) -> EngineState:
        return self._state

    def run(self, *, max_ticks: int) -> None:
        if max_ticks <= 0:
            raise ValueError("max_ticks must be > 0")

        self._lifecycle.start()

        try:
            while self._state.is_running and self._state.tick < max_ticks:
                tick = self._state.next_tick()

                self._bus.publish(
                    EngineTick.create(
                        run_id=self._state.run_id,
                        tick=tick,
                        sequence=self._state.next_sequence(),
                    )
                )

        except Exception as exc:
            # Fail-fast with explicit event emission (must include sequence)
            self._bus.publish(
                EngineError.create(
                    run_id=self._state.run_id,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    sequence=self._state.next_sequence(),
                )
            )
            log.exception("engine.crashed", run_id=self._state.run_id)
            raise

        finally:
            if self._state.is_running:
                self._lifecycle.stop()
