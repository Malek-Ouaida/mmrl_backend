from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from mmrl.marketdata.orderbook.delta import LevelUpdate, OrderBookDelta


@dataclass(frozen=True, slots=True)
class JsonlReplayDataSource:
    """
    Deterministic JSONL datasource for OrderBookDelta replay.

    Each line:
      {"symbol":"BTCUSDT","bid_updates":[[price,size],...],"ask_updates":[[price,size],...]}

    Order preserved. Blank lines ignored.
    """
    path: Path

    def __iter__(self) -> Iterator[OrderBookDelta]:
        with self.path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                s = line.strip()
                if not s:
                    continue

                try:
                    obj = json.loads(s)
                except json.JSONDecodeError as e:
                    raise ValueError(f"invalid JSON on line {line_no}: {e}") from e

                symbol = obj.get("symbol")
                bids = obj.get("bid_updates", [])
                asks = obj.get("ask_updates", [])

                if not isinstance(symbol, str) or not symbol:
                    raise ValueError(f"missing/invalid 'symbol' on line {line_no}")
                if not isinstance(bids, list) or not isinstance(asks, list):
                    raise ValueError(f"'bid_updates'/'ask_updates' must be lists on line {line_no}")

                bid_updates = tuple(LevelUpdate(price=float(p), size=float(sz)) for p, sz in bids)
                ask_updates = tuple(LevelUpdate(price=float(p), size=float(sz)) for p, sz in asks)

                delta = OrderBookDelta(symbol=symbol, bid_updates=bid_updates, ask_updates=ask_updates)
                delta.validate()
                yield delta
