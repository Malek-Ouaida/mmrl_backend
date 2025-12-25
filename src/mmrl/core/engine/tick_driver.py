from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from mmrl.core.engine.state import EngineState
from mmrl.core.events.system import EngineTick, RunStarted
from mmrl.core.events.bus import EventBus


@dataclass(slots=True)
class TickDriverComponent:
    """
    Dev driver: emits EngineTick events after RunStarted.

    This lets replay adapters and strategies actually run without a separate loop.
    Deterministic because it uses EngineState.next_tick/next_sequence.
    """
    bus: EventBus
    state: EngineState
    max_ticks: int = 200

    def subscriptions(self) -> Sequence[tuple[str, callable]]:
        return [("system.run_started", self._on_run_started)]

    def _on_run_started(self, e: RunStarted) -> None:
        # emit ticks synchronously
        for _ in range(self.max_ticks):
            tick = self.state.next_tick()
            self.bus.publish(
                EngineTick.create(
                    run_id=self.state.run_id,
                    tick=tick,
                    sequence=self.state.next_sequence(),
                )
            )
