"""Per-package line-coverage ratchet (docs/lake/goal.md G10).

Reads a merged Cobertura report (ReportGenerator's Cobertura.xml) and compares each package's line coverage with
docs/lake/verification/coverage-baseline.json. Fails when a package drops by more than the tolerance, or when a
package in the report has no baseline (so a new library is not unmeasured by accident). --update rewrites the
baseline from the report; commit it in the same pull request that explains the change.

  python3 tools/coverage_ratchet.py <Cobertura.xml> [--suite unit] [--update] [--summary <markdown file>]
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

BASELINE = Path(__file__).resolve().parent.parent / "docs" / "lake" / "verification" / "coverage-baseline.json"
TOLERANCE = 0.5  # percentage points


def read(report: Path) -> dict[str, float]:
    root = ET.parse(report).getroot()
    return {p.get("name"): round(float(p.get("line-rate")) * 100, 1) for p in root.iter("package")}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--suite", default="unit")
    parser.add_argument("--update", action="store_true")
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()

    current = read(args.report)
    data = json.loads(BASELINE.read_text()) if BASELINE.exists() else {}
    if args.update:
        data[args.suite] = dict(sorted(current.items()))
        BASELINE.write_text(json.dumps(data, indent=2) + "\n")
        print(f"baseline for '{args.suite}' updated: {len(current)} packages")
        return 0

    baseline = data.get(args.suite, {})
    rows, failures = [], []
    for name in sorted(set(current) | set(baseline)):
        now, was = current.get(name), baseline.get(name)
        if now is None:
            rows.append((name, was, None, "not in this report"))
            continue
        if was is None:
            failures.append(f"{name}: {now}% has no baseline (run with --update and commit the baseline)")
            rows.append((name, None, now, "no baseline"))
            continue
        status = "ok" if now >= was - TOLERANCE else "DROPPED"
        if status == "DROPPED":
            failures.append(f"{name}: {now}% < baseline {was}% - {TOLERANCE}")
        rows.append((name, was, now, status))

    lines = [f"### Line coverage ({args.suite} suite)", "", "| Package | Baseline | Now | |", "|---|---|---|---|"]
    lines += [f"| {n} | {'' if b is None else f'{b}%'} | {'' if c is None else f'{c}%'} | {s} |" for n, b, c, s in rows]
    text = "\n".join(lines) + "\n"
    print(text)
    if args.summary:
        with args.summary.open("a") as f:
            f.write(text)
    for failure in failures:
        print(f"::error::coverage {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
