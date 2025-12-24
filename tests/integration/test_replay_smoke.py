from __future__ import annotations

import json
from pathlib import Path

from mmrl.core.events.system import EngineTick
from mmrl.core.run.factory import RunFactory
from mmrl.core.run.manager import RunManager
from mmrl.core.run.spec import RunSpec


def test_replay_smoke_produces_book_and_bbo(tmp_path: Path) -> None:
    # --- Replay JSONL (3 deterministic deltas) ---
    replay_path = tmp_path / "replay.jsonl"
    replay_lines = [
        {
            "symbol": "BTCUSDT",
            "bid_updates": [[43000, 1.0], [42999.5, 1.0]],
            "ask_updates": [[43001, 1.0], [43001.5, 1.0]],
        },
        {
            "symbol": "BTCUSDT",
            "bid_updates": [[43000, 1.2]],
            "ask_updates": [[43001, 0.8]],
        },
        {
            "symbol": "BTCUSDT",
            "bid_updates": [[42999.5, 0.0]],   # delete
            "ask_updates": [[43001.5, 0.0]],   # delete
        },
    ]
    replay_path.write_text("\n".join(json.dumps(x) for x in replay_lines) + "\n", encoding="utf-8")

    # --- Create run directory + spec ---
    manager = RunManager(tmp_path)
    run = manager.create_run(seed=1, config_snapshot={"test": True})

    spec = RunSpec.model_validate(
        {
            "symbol": "BTCUSDT",
            "marketdata": {
                "mode": "paper_replay_l2",
                "replay_l2": {"path": str(replay_path), "format": "jsonl"},
            },
            # strategy/execution use defaults from RunSpec
        }
    )

    # --- Build through factory (canonical assembly) ---
    factory = RunFactory(runs_dir=tmp_path)
    handle = factory.build(run_id=run.run_id, spec=spec)

    # --- Start lifecycle ---
    handle.lifecycle.start()

    # --- Drive engine ticks (ReplayMarketDataAdapter consumes 1 delta per tick) ---
    for _ in range(10):
        t = handle.state.next_tick()
        handle.bus.publish(
            EngineTick.create(
                run_id=handle.run_id,
                tick=t,
                sequence=handle.state.next_sequence(),
            )
        )

    # --- Stop lifecycle ---
    handle.lifecycle.stop()

    # --- Verify event log contains orderbook updates + BBO ---
    events_path = handle.artifacts.events_jsonl
    assert events_path.exists()

    seen = set()
    lines = 0
    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            lines += 1
            obj = json.loads(line)
            et = obj.get("event_type")
            if et:
                seen.add(et)

    assert lines > 0, "events.jsonl should not be empty"
    assert "market.order_book_level" in seen
    assert "market.best_bid_ask" in seen
