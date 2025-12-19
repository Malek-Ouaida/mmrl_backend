from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class EngineState:
    """
    Engine state that must remain deterministic across runs.

    - tick: engine step counter
    - sequence: monotonic sequence used for event ordering (replay correctness)
    """

    run_id: str
    tick: int = 0
    sequence: int = 0
    is_running: bool = False

    def next_tick(self) -> int:
        self.tick += 1
        return self.tick

    def next_sequence(self) -> int:
        self.sequence += 1
        return self.sequence
