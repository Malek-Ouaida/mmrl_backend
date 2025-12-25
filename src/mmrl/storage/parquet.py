from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Mapping, Sequence

import structlog

log = structlog.get_logger()


class ParquetUnavailable(RuntimeError):
    pass


def _atomic_replace_write(path: Path, write_fn) -> None:
    """
    Atomic file write: write to tmp then replace.
    Readers never see partial files.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        write_fn(tmp)
        tmp.replace(path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass


def write_records_parquet(
    *,
    path: Path,
    records: Sequence[Mapping[str, Any]],
    compression: str = "zstd",
) -> None:
    """
    Write a list[dict] to Parquet atomically.

    Institutional properties:
      - deterministic schema: derived from keys present in records
      - atomic replace
      - compression enabled
    """
    if not records:
        # Create an empty parquet with no rows but stable schema? We choose:
        # do nothing (or create a marker file). For now: log + return.
        log.info("parquet.write_skipped_empty", path=str(path))
        return

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception as exc:
        raise ParquetUnavailable(
            "pyarrow is required for parquet output. Install: pip install pyarrow"
        ) from exc

    # Ensure parent directory exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # Deterministic column order: sort keys
    keys = sorted({k for r in records for k in r.keys()})

    # Build columns (missing keys become None)
    columns: dict[str, list[Any]] = {k: [] for k in keys}
    for r in records:
        for k in keys:
            columns[k].append(r.get(k, None))

    table = pa.table(columns)

    def _write(tmp_path: Path) -> None:
        pq.write_table(
            table,
            tmp_path,
            compression=compression,
            use_dictionary=True,
            write_statistics=True,
        )

    _atomic_replace_write(path, _write)

    log.info(
        "parquet.written",
        path=str(path),
        rows=len(records),
        cols=len(keys),
        compression=compression,
    )


def write_series_parquet(
    *,
    path: Path,
    series: Any,
    compression: str = "zstd",
) -> None:
    """
    Write a dataclass-like 'series' (with list fields) to Parquet.

    Expected shape (your RiskInventorySeries):
      series.seq: list[int]
      series.inv: list[float]
      ... etc
    """
    # Accept dataclass or plain object
    if hasattr(series, "__dataclass_fields__"):
        data = asdict(series)
    else:
        # Best-effort: use __dict__
        data = dict(series.__dict__)

    # Convert columnar dict -> list of row dicts (arrow can do either, but rows are simplest + stable)
    # Validate equal lengths
    lengths = {k: len(v) for k, v in data.items() if isinstance(v, list)}
    if not lengths:
        log.info("parquet.write_skipped_no_lists", path=str(path))
        return

    n = max(lengths.values())
    for k, ln in lengths.items():
        if ln != n:
            raise ValueError(f"series column length mismatch: {k} has {ln} != {n}")

    records = []
    keys = sorted(data.keys())
    for i in range(n):
        row = {}
        for k in keys:
            v = data[k]
            row[k] = v[i] if isinstance(v, list) else v
        records.append(row)

    write_records_parquet(path=path, records=records, compression=compression)
