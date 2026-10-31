"""Print the failed tests recorded in the TRX files of a directory (tools/test-projects.sh calls it after a red run).

Each failure is one GitHub Actions error annotation plus the first lines of its message, so the cause of a red suite is
at the end of the log and on the run page.
Usage: python3 -I tools/trx_failures.py <dir-with-trx-files>
"""

from __future__ import annotations

import pathlib
import sys
import xml.etree.ElementTree as ET

NS = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}
MAX_LINES = 12


def main(directory: str) -> int:
    count = 0
    for trx in sorted(pathlib.Path(directory).glob("*.trx")):
        root = ET.parse(trx).getroot()
        for result in root.iterfind(".//t:UnitTestResult", NS):
            if result.get("outcome") != "Failed":
                continue
            count += 1
            name = result.get("testName", "?")
            message = (result.findtext("t:Output/t:ErrorInfo/t:Message", default="", namespaces=NS) or "").strip()
            first = message.splitlines()[0] if message else "(no message)"
            print(f"::error title={trx.stem}::{name}: {first[:300]}")
            for line in message.splitlines()[1:MAX_LINES]:
                print(f"    {line}")
    print(f"{count} failed test(s) in {directory}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
