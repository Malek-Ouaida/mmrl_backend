from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class EngineState:
    """
    Engine state that must remain deterministic across runs.

    - tick: engine step counter
    - sequence: monotonic sequence used for event ordering (replay correctness)

    Guardrails:
      - next_tick / next_sequence only valid while engine is running
        (prevents "events after stop" bugs and makes lifecycle explicit)
    """

    run_id: str
    tick: int = 0
    sequence: int = 0
    is_running: bool = False

    def next_tick(self) -> int:
        if not self.is_running:
            raise RuntimeError("cannot advance tick when engine is not running")
        self.tick += 1
        return self.tick

    def next_sequence(self) -> int:
        if not self.is_running:
            raise RuntimeError("cannot advance sequence when engine is not running")
        self.sequence += 1
        return self.sequence
