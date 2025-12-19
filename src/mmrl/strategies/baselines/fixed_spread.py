from __future__ import annotations

import hashlib
from dataclasses import dataclass

import structlog

from mmrl.core.engine.state import EngineState
from mmrl.core.events.base import Event
from mmrl.core.events.bus import EventBus
from mmrl.core.events.marketdata import BestBidAskUpdate
from mmrl.core.events.orders import Fill, OrderCanceled, OrderCancelRequested, OrderSubmitted
from mmrl.strategies.base import Strategy

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class FixedSpreadConfig:
    symbol: str

    # Quoting parameters
    spread: float              # absolute spread in price units
    order_size: float          # base quote size

    # Inventory control
    max_inventory: float       # hard cap; beyond this we stop quoting one side
    inventory_skew_k: float    # skew factor in price units per inventory unit

    # Re-quote throttling
    min_mid_move: float = 0.0  # only requote if mid moved more than this
    min_ticks_between_quotes: int = 1


class FixedSpreadMarketMaker(Strategy):
    """
    Event-driven fixed-spread market maker baseline.

    Design goals:
      - Deterministic: order IDs and event sequences are reproducible.
      - Correct-by-construction cancel/replace: never more than 1 live quote per side.
      - Conservative state machine: replacements are submitted only after cancel ack.
      - Inventory-aware quoting: skew quotes based on current inventory.
      - Minimal assumptions about the execution layer: cancel acks and fills may arrive
        in any order in the future once latency/partial fills are introduced.

    Event flow:
      market.best_bid_ask -> quote decision -> (optional cancel) -> order.submitted
      order.canceled -> submit staged replacement (if any)
      order.fill -> update inventory + clear state for that order
    """

    _EPS: float = 1e-12

    def __init__(
        self,
        *,
        bus: EventBus,
        state: EngineState,
        cfg: FixedSpreadConfig,
    ) -> None:
        self._bus = bus
        self._state = state
        self._cfg = cfg

        self._inventory: float = 0.0
        self._last_mid: float | None = None
        self._last_quote_tick: int = -10**9

        # Active quote tracking (cancel/replace)
        self._active_bid_id: str | None = None
        self._active_ask_id: str | None = None
        self._active_bid_price: float | None = None
        self._active_ask_price: float | None = None

        # Pending replacements (submit only after cancel ack).
        # Latest-intent-wins: if mid moves while cancel in flight, update pending.
        self._pending_bid: tuple[float, float] | None = None  # (price, qty)
        self._pending_ask: tuple[float, float] | None = None  # (price, qty)

    def subscriptions(self):
        return [
            ("market.best_bid_ask", self._on_bbo),
            ("order.fill", self._on_fill),
            ("order.canceled", self._on_canceled),
        ]

    # ---------------- Invariants ----------------

    def _assert_invariants(self) -> None:
        """
        Brutal correctness checks.

        Invariants:
          - At most one active order per side is tracked.
          - A pending replacement may only exist if there is something to cancel.
        """
        if self._pending_bid is not None:
            assert self._active_bid_id is not None, "pending bid without active bid"
        if self._pending_ask is not None:
            assert self._active_ask_id is not None, "pending ask without active ask"

    @staticmethod
    def _same_price(a: float | None, b: float | None, eps: float) -> bool:
        if a is None or b is None:
            return False
        return abs(a - b) <= eps

    # ---------------- Event handlers ----------------

    def _on_fill(self, e: Event) -> None:
        if not isinstance(e, Fill):
            return
        if e.symbol != self._cfg.symbol:
            return

        signed = e.fill_quantity if e.side == "buy" else -e.fill_quantity
        self._inventory += signed

        # Clear active IDs if the filled order was one of our active quotes.
        # Also clear any staged replacement; it's stale once the order has filled.
        if self._active_bid_id == e.order_id:
            self._active_bid_id = None
            self._active_bid_price = None
            self._pending_bid = None

        if self._active_ask_id == e.order_id:
            self._active_ask_id = None
            self._active_ask_price = None
            self._pending_ask = None

        log.info(
            "strategy.inventory_updated",
            run_id=self._state.run_id,
            symbol=e.symbol,
            side=e.side,
            fill_qty=e.fill_quantity,
            fill_price=e.fill_price,
            inventory=self._inventory,
        )

        self._assert_invariants()

    def _on_canceled(self, e: Event) -> None:
        if not isinstance(e, OrderCanceled):
            return
        if e.symbol != self._cfg.symbol:
            return

        # Bid cancel ack → submit pending bid replacement (if any)
        if self._active_bid_id == e.order_id:
            self._active_bid_id = None
            self._active_bid_price = None

            if self._pending_bid is not None:
                price, qty = self._pending_bid
                self._pending_bid = None

                o = self._make_order(side="buy", price=price, qty=qty)
                self._active_bid_id = o.order_id
                self._active_bid_price = price
                self._bus.publish(o)

        # Ask cancel ack → submit pending ask replacement (if any)
        if self._active_ask_id == e.order_id:
            self._active_ask_id = None
            self._active_ask_price = None

            if self._pending_ask is not None:
                price, qty = self._pending_ask
                self._pending_ask = None

                o = self._make_order(side="sell", price=price, qty=qty)
                self._active_ask_id = o.order_id
                self._active_ask_price = price
                self._bus.publish(o)

        log.debug(
            "strategy.cancel_ack",
            run_id=self._state.run_id,
            symbol=e.symbol,
            order_id=e.order_id,
        )

        self._assert_invariants()

    def _on_bbo(self, e: Event) -> None:
        if not isinstance(e, BestBidAskUpdate):
            return
        if e.symbol != self._cfg.symbol:
            return

        bid = e.bid_price
        ask = e.ask_price
        if bid <= 0 or ask <= 0 or ask <= bid:
            return

        mid = (bid + ask) / 2.0

        # Throttle quotes to avoid spamming
        if (self._state.tick - self._last_quote_tick) < self._cfg.min_ticks_between_quotes:
            return
        if self._last_mid is not None and abs(mid - self._last_mid) < self._cfg.min_mid_move:
            return

        self._last_mid = mid
        self._last_quote_tick = self._state.tick

        # Inventory skew: positive inventory pushes quotes down to encourage selling
        skew = self._cfg.inventory_skew_k * self._inventory

        half = self._cfg.spread / 2.0
        bid_quote = mid - half - skew
        ask_quote = mid + half - skew

        # Clamp sizes based on inventory limits
        buy_size = self._cfg.order_size
        sell_size = self._cfg.order_size

        if self._inventory >= self._cfg.max_inventory:
            buy_size = 0.0
        if self._inventory <= -self._cfg.max_inventory:
            sell_size = 0.0

        # --- Bid quote (buy) ---
        if buy_size > 0:
            need_new_bid = (
                self._active_bid_id is None
                or self._active_bid_price is None
                or not self._same_price(bid_quote, self._active_bid_price, self._EPS)
            )

            if need_new_bid:
                # Latest-intent-wins while cancel is in flight:
                # if the market moves again, update the pending replacement.
                if self._pending_bid is not None:
                    self._pending_bid = (bid_quote, buy_size)
                elif self._active_bid_id is not None:
                    self._pending_bid = (bid_quote, buy_size)
                    self._bus.publish(
                        OrderCancelRequested.create(
                            symbol=self._cfg.symbol,
                            order_id=self._active_bid_id,
                            sequence=self._state.next_sequence(),
                        )
                    )
                else:
                    o = self._make_order(side="buy", price=bid_quote, qty=buy_size)
                    self._active_bid_id = o.order_id
                    self._active_bid_price = bid_quote
                    self._bus.publish(o)

        # --- Ask quote (sell) ---
        if sell_size > 0:
            need_new_ask = (
                self._active_ask_id is None
                or self._active_ask_price is None
                or not self._same_price(ask_quote, self._active_ask_price, self._EPS)
            )

            if need_new_ask:
                # Latest-intent-wins while cancel is in flight:
                if self._pending_ask is not None:
                    self._pending_ask = (ask_quote, sell_size)
                elif self._active_ask_id is not None:
                    self._pending_ask = (ask_quote, sell_size)
                    self._bus.publish(
                        OrderCancelRequested.create(
                            symbol=self._cfg.symbol,
                            order_id=self._active_ask_id,
                            sequence=self._state.next_sequence(),
                        )
                    )
                else:
                    o = self._make_order(side="sell", price=ask_quote, qty=sell_size)
                    self._active_ask_id = o.order_id
                    self._active_ask_price = ask_quote
                    self._bus.publish(o)

        log.info(
            "strategy.quoted",
            run_id=self._state.run_id,
            symbol=self._cfg.symbol,
            mid=mid,
            bid_quote=bid_quote,
            ask_quote=ask_quote,
            inventory=self._inventory,
            active_bid_id=self._active_bid_id,
            active_ask_id=self._active_ask_id,
            pending_bid=self._pending_bid is not None,
            pending_ask=self._pending_ask is not None,
        )

        self._assert_invariants()

    # ---------------- Helpers ----------------

    def _make_order(self, *, side: str, price: float, qty: float) -> OrderSubmitted:
        """
        Build a deterministic OrderSubmitted event.

        Order IDs are stable across replays:
          sha1(run_id | tick | side | price | qty)[:16]
        """
        payload = f"{self._state.run_id}|{self._state.tick}|{side}|{price:.8f}|{qty:.8f}"
        oid = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]

        return OrderSubmitted.create(
            symbol=self._cfg.symbol,
            order_id=oid,
            side=side,  # type: ignore[arg-type]
            order_type="limit",
            time_in_force="GTC",
            price=price,
            quantity=qty,
            sequence=self._state.next_sequence(),
        )
