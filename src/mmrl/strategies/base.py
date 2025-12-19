from __future__ import annotations

from typing import Callable, Protocol, Sequence

from mmrl.core.events.base import Event


EventHandler = Callable[[Event], None]


class Strategy(Protocol):
    """
    Strategy interface.

    Strategies:
    - subscribe to market/system events
    - produce order intent events
    """

    def subscriptions(self) -> Sequence[tuple[str, EventHandler]]:
        ...
