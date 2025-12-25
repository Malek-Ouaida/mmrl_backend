# src/mmrl/execution/paper/adapter.py
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
    OrderRejected,
    OrderSubmitted,
)
from mmrl.execution.model.fill_model import FillModel, TopOfBookFullFillModel
from mmrl.execution.oms.orders import OrderRecord
from mmrl.execution.oms.positions import Position
from mmrl.execution.oms.risk import RiskLimits, RiskManager

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
      - order.rejected
      - order.canceled
      - order.fill
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        state: EngineState,
        fill_model: FillModel | None = None,
        risk: RiskManager | None = None,
    ) -> None:
        self._bus = bus
        self._state = state

        # Deterministic fill model (pluggable)
        self._fill_model: FillModel = fill_model if fill_model is not None else TopOfBookFullFillModel()

        # Deterministic risk gate (pluggable)
        self._risk: RiskManager = risk if risk is not None else RiskManager(
            limits=RiskLimits(
                max_order_qty=1e9,
                max_abs_inventory=1e9,
                max_order_notional=None,
            )
        )

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

        self._bbo_by_symbol[e.symbol] = e

        # Try fill any open orders for this symbol on each BBO update
        for oid in tuple(self._orders_by_symbol.get(e.symbol, ())):
            rec = self._orders.get(oid)
            if rec is None or rec.status != "open":
                continue
            self._try_fill(rec)

    def _on_order_submitted(self, e: Event) -> None:
        if not isinstance(e, OrderSubmitted):
            return

        # Risk gate first (reserve exposure deterministically)
        rc = self._risk.check_new_order(
            symbol=e.symbol,
            side=e.side,
            qty=e.quantity,
            price=e.price,
            order_id=e.order_id,
        )
        if not rc.ok:
            self._bus.publish(
                OrderRejected.create(
                    symbol=e.symbol,
                    order_id=e.order_id,
                    reason=rc.reason,
                    sequence=self._state.next_sequence(),
                )
            )
            log.info(
                "paper.rejected",
                run_id=self._state.run_id,
                symbol=e.symbol,
                order_id=e.order_id,
                reason=rc.reason,
            )
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

        # ✅ FIX: OrderAccepted must include side/price/quantity
        self._bus.publish(
            OrderAccepted.create(
                symbol=e.symbol,
                order_id=e.order_id,
                side=e.side,
                price=e.price,
                quantity=e.quantity,
                sequence=self._state.next_sequence(),
            )
        )

        # Attempt immediate fill against latest known BBO for this symbol
        if e.symbol in self._bbo_by_symbol:
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

        # Release reserved exposure
        self._risk.on_cancel(order_id=e.order_id)

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
        if order.status != "open":
            return

        bbo = self._bbo_by_symbol.get(order.symbol)
        if bbo is None:
            return

        decision = self._fill_model.decide(order=order, bbo=bbo)
        if not decision.executable:
            return

        assert decision.fill_price is not None, "FillModel returned executable without fill_price"
        assert decision.fill_qty is not None, "FillModel returned executable without fill_qty"

        fill_price = float(decision.fill_price)
        fill_qty = float(decision.fill_qty)

        if fill_qty <= 0:
            return

        order.apply_fill(fill_qty=fill_qty)

        # Update position
        pos = self._positions.get(order.symbol)
        if pos is None:
            pos = Position(symbol=order.symbol)
            self._positions[order.symbol] = pos
        pos.on_fill(side=order.side, qty=fill_qty, price=fill_price)

        # Update risk inventory (tie to order_id so reservations are released)
        self._risk.on_fill(
            symbol=order.symbol,
            side=order.side,
            qty=fill_qty,
            order_id=order.order_id,
        )

        if order.status != "open":
            self._orders_by_symbol.get(order.symbol, set()).discard(order.order_id)

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
