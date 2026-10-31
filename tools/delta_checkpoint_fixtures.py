"""Derive the checkpoint-shape fixtures Lake.Delta.Tests needs from DAT's with_checkpoint table.

    uv run --project interop python -I tools/delta_checkpoint_fixtures.py

DAT ships only classic checkpoints, so the multi-part and V2 sidecar shapes are cut out of its real checkpoint
(tests/Lake.Delta.Tests/fixtures/dat/with_checkpoint/delta/_delta_log/00000000000000000002.checkpoint.parquet) with
pyarrow, an independent Parquet writer (ParquetSharp's Arrow writer cannot re-export imported nested batches, Arrow
C# 18). The rows are unchanged; only how they are split differs:

    checkpoint-shapes/multi-part/part1.parquet   the checkpoint's file-action rows (remove, add)
    checkpoint-shapes/multi-part/part2.parquet   its other rows (protocol, metaData)
    checkpoint-shapes/sidecar.parquet            the file-action rows' add and remove columns only (a V2 sidecar)

Outputs are committed; regenerate them with this script, never edit them.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "Lake.Delta.Tests" / "fixtures"
SOURCE = (
    FIXTURES
    / "dat"
    / "with_checkpoint"
    / "delta"
    / "_delta_log"
    / "00000000000000000002.checkpoint.parquet"
)
OUT = FIXTURES / "checkpoint-shapes"


def main() -> None:
    table = pq.read_table(SOURCE)
    is_file_action = pc.or_(pc.is_valid(table["add"]), pc.is_valid(table["remove"]))
    files = table.filter(is_file_action)
    others = table.filter(pc.invert(is_file_action))
    assert (
        files.num_rows + others.num_rows == table.num_rows
        and files.num_rows > 0
        and others.num_rows > 0
    )

    (OUT / "multi-part").mkdir(parents=True, exist_ok=True)
    pq.write_table(files, OUT / "multi-part" / "part1.parquet")
    pq.write_table(others, OUT / "multi-part" / "part2.parquet")
    pq.write_table(files.select(["add", "remove"]), OUT / "sidecar.parquet")
    print(
        f"{files.num_rows} file-action rows, {others.num_rows} other rows -> {OUT.relative_to(ROOT)}"
    )


if __name__ == "__main__":
    main()
