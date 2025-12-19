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

    Institutional-grade behavior (for a simulator):
      - Symbol-scoped BBO tracking (not a single global BBO)
      - Deterministic sequence allocation via EngineState.next_sequence()
      - Deterministic order state transitions (open -> filled/canceled)
      - Immediate acceptance on submit
      - Fill checks on BOTH:
          (1) BBO updates (resting orders become executable)
          (2) Order submits (new orders may cross immediately)
      - Cancels are idempotent and safe:
          canceling a missing/non-open order is a no-op (logged at debug)
    """

    _EPS: float = 1e-12

    def __init__(self, *, bus: EventBus, state: EngineState) -> None:
        self._bus = bus
        self._state = state

        # Latest BBO per symbol
        self._bbo_by_symbol: dict[str, BestBidAskUpdate] = {}

        # OMS: order_id -> OrderRecord
        self._orders: dict[str, OrderRecord] = {}

        # Fast index: symbol -> set(order_id)
        self._orders_by_symbol: dict[str, set[str]] = {}

        # Positions: symbol -> Position
        self._positions: dict[str, Position] = {}

    def subscriptions(self):
        return [
            ("order.submitted", self._on_order_submitted),
            ("order.cancel_requested", self._on_cancel_requested),
            ("market.best_bid_ask", self._on_bbo),
        ]

    # ---------------- Handlers ----------------

    def _on_bbo(self, e: Event) -> None:
        if not isinstance(e, BestBidAskUpdate):
            return

        # Store latest BBO for that symbol
        self._bbo_by_symbol[e.symbol] = e

        # Try fill any resting orders for this symbol
        for oid in tuple(self._orders_by_symbol.get(e.symbol, ())):
            rec = self._orders.get(oid)
            if rec is None or rec.status != "open":
                continue
            self._try_fill(rec)

    def _on_order_submitted(self, e: Event) -> None:
        if not isinstance(e, OrderSubmitted):
            return

        # Accept immediately (paper venue)
        rec = OrderRecord(
            symbol=e.symbol,
            order_id=e.order_id,
            side=e.side,
            price=e.price,
            quantity=e.quantity,
            remaining=e.quantity,
            status="open",
        )
        self._orders[e.order_id] = rec
        self._orders_by_symbol.setdefault(e.symbol, set()).add(e.order_id)

        self._bus.publish(
            OrderAccepted.create(
                symbol=e.symbol,
                order_id=e.order_id,
                sequence=self._state.next_sequence(),
            )
        )

        # Attempt immediate fill against latest known BBO for this symbol
        bbo = self._bbo_by_symbol.get(e.symbol)
        if bbo is not None:
            self._try_fill(rec)

    def _on_cancel_requested(self, e: Event) -> None:
        if not isinstance(e, OrderCancelRequested):
            return

        rec = self._orders.get(e.order_id)
        if rec is None:
            log.debug(
                "paper.cancel_ignored_missing",
                run_id=self._state.run_id,
                symbol=e.symbol,
                order_id=e.order_id,
            )
            return

        # Safety: ignore mismatched symbol cancels
        if rec.symbol != e.symbol:
            log.debug(
                "paper.cancel_ignored_symbol_mismatch",
                run_id=self._state.run_id,
                event_symbol=e.symbol,
                order_symbol=rec.symbol,
                order_id=e.order_id,
            )
            return

        if rec.status != "open":
            log.debug(
                "paper.cancel_ignored_not_open",
                run_id=self._state.run_id,
                symbol=e.symbol,
                order_id=e.order_id,
                status=rec.status,
            )
            return

        rec.cancel()
        self._orders_by_symbol.get(rec.symbol, set()).discard(rec.order_id)

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

    # ---------------- Fill model ----------------

    def _try_fill(self, order: OrderRecord) -> None:
        """
        Deterministic fill model:
          - Limit orders execute if they touch/cross top-of-book
          - Buy executes at ask when order.price >= ask
          - Sell executes at bid when order.price <= bid
          - Full fill (for now): fill_qty = remaining
        """
        if order.status != "open":
            return

        bbo = self._bbo_by_symbol.get(order.symbol)
        if bbo is None:
            return

        bid = bbo.bid_price
        ask = bbo.ask_price
        if bid <= 0 or ask <= 0:
            return

        # Only limit supported right now (market orders later)
        if order.price is None:
            return

        # Determine if executable and choose fill price
        if order.side == "buy":
            executable = order.price + self._EPS >= ask
            fill_price = ask
        else:
            executable = order.price - self._EPS <= bid
            fill_price = bid

        if not executable:
            return

        fill_qty = order.remaining
        if fill_qty <= self._EPS:
            return

        order.apply_fill(fill_qty=fill_qty)

        # Update position
        pos = self._positions.get(order.symbol)
        if pos is None:
            pos = Position(symbol=order.symbol)
            self._positions[order.symbol] = pos
        pos.on_fill(side=order.side, qty=fill_qty, price=fill_price)

        # If filled, remove from symbol index
        if order.status != "open":
            self._orders_by_symbol.get(order.symbol, set()).discard(order.order_id)

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
