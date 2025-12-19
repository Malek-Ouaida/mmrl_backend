from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, DefaultDict, Iterable, TypeAlias

import structlog

from mmrl.core.events.base import Event

log = structlog.get_logger()

EventHandler: TypeAlias = Callable[[Event], None]


@dataclass(frozen=True)
class Subscription:
    """
    Represents a subscription of a handler to a specific event_type.
    """

    event_type: str
    handler: EventHandler


class EventBus:
    """
    Deterministic synchronous event bus.

    - publish(event) dispatches to handlers subscribed to event.event_type
    - dispatch order is subscription order
    - failures are fail-fast by default (raises)
    """

    def __init__(self) -> None:
        self._handlers: DefaultDict[str, list[EventHandler]] = defaultdict(list)

    def subscribe(self, *, event_type: str, handler: EventHandler) -> Subscription:
        if not event_type:
            raise ValueError("event_type must be non-empty")
        self._handlers[event_type].append(handler)
        log.debug("bus.subscribed", event_type=event_type, handler=getattr(handler, "__name__", "handler"))
        return Subscription(event_type=event_type, handler=handler)

    def publish(self, event: Event) -> None:
        handlers = self._handlers.get(event.event_type, [])
        log.debug(
            "bus.publish",
            event_type=event.event_type,
            event_id=str(event.event_id),
            handlers=len(handlers),
        )
        for handler in handlers:
            handler(event)

    def subscribers_for(self, event_type: str) -> Iterable[EventHandler]:
        return tuple(self._handlers.get(event_type, []))
