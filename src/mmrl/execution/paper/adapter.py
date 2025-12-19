from __future__ import annotations

import structlog

from mmrl.core.engine.state import EngineState
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.events.marketdata import BestBidAskUpdate
from mmrl.core.events.orders import (
    Fill,
    OrderAccepted,
    OrderCanceled,
    OrderCancelRequested,
    OrderSubmitted,
)
from mmrl.execution.oms.orders import OrderRecord
from mmrl.execution.oms.positions import Position

log = structlog.get_logger()


class PaperExecutionAdapter:
    """
    Paper execution adapter (deterministic, event-driven).

    Consumes:
      - order.submitted
      - order.cancel_requested
      - market.best_bid_ask

    Emits:
      - order.accepted
      - order.canceled
      - order.fill

    Notes:
      - Fill model is top-of-book touch/cross for limit orders
      - Cancel is immediate (deterministic). Later we can add latency.
    """

    def __init__(self, *, bus: EventBus, state: EngineState) -> None:
        self._bus = bus
        self._state = state

        self._bbo: BestBidAskUpdate | None = None

        # OMS
        self._orders: dict[str, OrderRecord] = {}

        # Positions
        self._positions: dict[str, Position] = {}

    def subscriptions(self):
        return [
            ("order.submitted", self._on_order_submitted),
            ("order.cancel_requested", self._on_cancel_requested),
            ("market.best_bid_ask", self._on_bbo),
        ]

    # ---------- Handlers ----------

    def _on_bbo(self, e: Event) -> None:
        if not isinstance(e, BestBidAskUpdate):
            return
        self._bbo = e

        # try fill any resting orders for this symbol
        for order in list(self._orders.values()):
            if order.status != "open":
                continue
            if order.symbol != e.symbol:
                continue
            self._try_fill(order)

    def _on_order_submitted(self, e: Event) -> None:
        if not isinstance(e, OrderSubmitted):
            return

        # accept immediately
        self._orders[e.order_id] = OrderRecord(
            symbol=e.symbol,
            order_id=e.order_id,
            side=e.side,
            price=e.price,
            quantity=e.quantity,
            remaining=e.quantity,
            status="open",
        )

        self._bus.publish(
            OrderAccepted.create(
                symbol=e.symbol,
                order_id=e.order_id,
                sequence=self._state.next_sequence(),
            )
        )

        # attempt immediate fill if we have bbo
        if self._bbo is not None and self._bbo.symbol == e.symbol:
            self._try_fill(self._orders[e.order_id])

    def _on_cancel_requested(self, e: Event) -> None:
        if not isinstance(e, OrderCancelRequested):
            return

        rec = self._orders.get(e.order_id)
        if rec is None:
            return
        if rec.symbol != e.symbol:
            return
        if rec.status != "open":
            return

        rec.cancel()

        self._bus.publish(
            OrderCanceled.create(
                symbol=e.symbol,
                order_id=e.order_id,
                sequence=self._state.next_sequence(),
            )
        )

        log.info(
            "paper.canceled",
            run_id=self._state.run_id,
            symbol=e.symbol,
            order_id=e.order_id,
        )

    # ---------- Fill model ----------

    def _try_fill(self, order: OrderRecord) -> None:
        bbo = self._bbo
        if bbo is None:
            return

        bid = bbo.bid_price
        ask = bbo.ask_price
        if bid <= 0 or ask <= 0:
            return

        # Only limit supported right now (market orders later)
        if order.price is None:
            return

        # Determine if the order crosses/touches
        if order.side == "buy":
            executable = order.price >= ask
            fill_price = ask
        else:
            executable = order.price <= bid
            fill_price = bid

        if not executable:
            return

        fill_qty = order.remaining
        order.apply_fill(fill_qty=fill_qty)

        # Update position
        pos = self._positions.get(order.symbol)
        if pos is None:
            pos = Position(symbol=order.symbol)
            self._positions[order.symbol] = pos
        pos.on_fill(side=order.side, qty=fill_qty, price=fill_price)

        # Emit fill event
        self._bus.publish(
            Fill.create(
                symbol=order.symbol,
                order_id=order.order_id,
                side=order.side,
                fill_price=fill_price,
                fill_quantity=fill_qty,
                remaining_quantity=order.remaining,
                sequence=self._state.next_sequence(),
            )
        )

        log.info(
            "paper.fill",
            run_id=self._state.run_id,
            symbol=order.symbol,
            order_id=order.order_id,
            side=order.side,
            fill_price=fill_price,
            fill_qty=fill_qty,
            remaining=order.remaining,
            inventory=pos.inventory,
        )
