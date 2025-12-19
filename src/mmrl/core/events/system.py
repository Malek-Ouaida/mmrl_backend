from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from mmrl.core.events.base import Event


@dataclass(frozen=True, slots=True)
class RunStarted(Event):
    """
    Emitted when a run begins.
    """

    event_type: ClassVar[str] = "system.run_started"

    run_id: str


@dataclass(frozen=True, slots=True)
class RunStopped(Event):
    """
    Emitted when a run ends normally.
    """

    event_type: ClassVar[str] = "system.run_stopped"

    run_id: str


@dataclass(frozen=True, slots=True)
class EngineTick(Event):
    """
    Emitted on each engine step / iteration.
    Useful for deterministic replay and diagnostics.
    """

    event_type: ClassVar[str] = "system.engine_tick"

    run_id: str
    tick: int


@dataclass(frozen=True, slots=True)
class EngineError(Event):
    """
    Emitted when the engine encounters an unrecoverable error.
    """

    event_type: ClassVar[str] = "system.engine_error"

    run_id: str

    error_type: str
    error_message: str
