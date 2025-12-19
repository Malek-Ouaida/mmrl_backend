from __future__ import annotations

from typing import Any, Iterable, Sequence

from mmrl.marketdata.orderbook.delta import LevelUpdate, OrderBookDelta


def normalize_l2_delta(
    *,
    symbol: str,
    bids: Sequence[Any],
    asks: Sequence[Any],
) -> OrderBookDelta:
    """
    Normalize raw bids/asks updates into an OrderBookDelta.

    Accepts rows like:
      - ["123.4", "0.56"]  (binance style)
      - [123.4, 0.56]
      - {"price": 123.4, "size": 0.56}

    Returns:
      OrderBookDelta(symbol=..., bid_updates=(...), ask_updates=(...))
    """
    if not symbol:
        raise ValueError("symbol must be non-empty")

    bid_updates = tuple(_parse_levels(bids))
    ask_updates = tuple(_parse_levels(asks))

    d = OrderBookDelta(symbol=symbol, bid_updates=bid_updates, ask_updates=ask_updates)
    d.validate()
    return d


def _parse_levels(rows: Sequence[Any]) -> Iterable[LevelUpdate]:
    for r in rows:
        price, size = _parse_row(r)
        yield LevelUpdate(price=price, size=size)


def _parse_row(row: Any) -> tuple[float, float]:
    # Dict form
    if isinstance(row, dict):
        if "price" not in row or "size" not in row:
            raise ValueError("dict row must contain 'price' and 'size'")
        return float(row["price"]), float(row["size"])

    # List/tuple form
    if isinstance(row, (list, tuple)) and len(row) == 2:
        return float(row[0]), float(row[1])

    raise ValueError("row must be [price, size] or {'price':..., 'size':...}")
