from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Protocol, Sequence

from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus, Subscription


EventHandler = Callable[[Event], None]


class EventComponent(Protocol):
    """
    A component that can register event handlers to an EventBus.

    Examples:
    - replay marketdata adapter
    - live Binance adapter
    - strategy
    - execution adapter
    - metrics/evaluation collector
    """

    def subscriptions(self) -> Sequence[tuple[str, EventHandler]]:
        """
        Return (event_type, handler) tuples.
        """
        ...


@dataclass(frozen=True)
class RouterWiring:
    """
    Captures the wiring produced when components are registered.
    Useful for debugging / observability.
    """

    subscriptions: tuple[Subscription, ...]


class EngineRouter:
    """
    Registers components onto an EventBus deterministically.
    """

    def __init__(self, *, bus: EventBus) -> None:
        self._bus = bus

    def register(self, components: Iterable[EventComponent]) -> RouterWiring:
        subs: list[Subscription] = []

        for component in components:
            for event_type, handler in component.subscriptions():
                subs.append(self._bus.subscribe(event_type=event_type, handler=handler))

        return RouterWiring(subscriptions=tuple(subs))
