from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Protocol, Sequence

from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus, Subscription


EventHandler = Callable[[Event], None]


class EventComponent(Protocol):
    """
    A component that can register event handlers to an EventBus.
    """

    def subscriptions(self) -> Sequence[tuple[str, EventHandler]]:
        """
        Return (event_type, handler) tuples.
        """
        ...


@dataclass(frozen=True, slots=True)
class WiredSubscription:
    """
    A subscription + the component that produced it (debuggable wiring).
    """

    component: str
    subscription: Subscription


@dataclass(frozen=True, slots=True)
class RouterWiring:
    """
    Captures the wiring produced when components are registered.
    Useful for debugging / observability.
    """

    subscriptions: tuple[WiredSubscription, ...]


class EngineRouter:
    """
    Registers components onto an EventBus deterministically.

    Determinism rules:
      - components are wired in the order provided
      - each component's subscriptions() order is preserved
    """

    def __init__(self, *, bus: EventBus) -> None:
        self._bus = bus

    @staticmethod
    def _component_name(component: object) -> str:
        return type(component).__name__

    def register(self, components: Iterable[EventComponent]) -> RouterWiring:
        wired: list[WiredSubscription] = []
        seen: set[tuple[str, int]] = set()
        # key: (event_type, id(handler)) to detect accidental double wiring

        for component in components:
            cname = self._component_name(component)

            subs = component.subscriptions()
            if not isinstance(subs, Sequence):
                raise TypeError(f"{cname}.subscriptions() must return a Sequence")

            for event_type, handler in subs:
                if not event_type:
                    raise ValueError(f"{cname} produced empty event_type")

                key = (event_type, id(handler))
                if key in seen:
                    raise RuntimeError(f"duplicate subscription detected: component={cname} event_type={event_type}")
                seen.add(key)

                s = self._bus.subscribe(event_type=event_type, handler=handler)
                wired.append(WiredSubscription(component=cname, subscription=s))

        return RouterWiring(subscriptions=tuple(wired))
