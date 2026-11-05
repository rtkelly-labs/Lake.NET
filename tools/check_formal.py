"""Check the bounded commit model, its negative controls, and an executable witness.

Run: python3 tools/check_formal.py [--update-trace]
Requires Java 11+ and curl. No Python packages. All downloaded/generated files go in temp/formal.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODEL = ROOT / "formal/commit-retry"
WORK = ROOT / "temp/formal"
VERSION = "1.7.4"
JAR_SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
TRACE = MODEL / "traces/append-conflict.json"
SAFETY = "TypeOK RowsMatchSerialIntent AtMostOnce ReachableFilesExist UnknownFilesPreserved HeadRetained"
FENCE_SAFETY = "TypeOK PublishedSchemaCorrect PublishedSpecCorrect"


def java_binary() -> str:
    if binary := os.environ.get("ICEBERG_TLC_JAVA"):
        return binary
    if home := os.environ.get("JAVA_HOME"):
        return str(Path(home) / "bin/java")
    if binary := shutil.which("java"):
        return binary
    raise RuntimeError("Java 11+ required; set JAVA_HOME or ICEBERG_TLC_JAVA")


def tools_jar() -> Path:
    jar = WORK / f"tla2tools-{VERSION}.jar"
    if not jar.exists():
        download = jar.with_suffix(".download")
        try:
            subprocess.run(
                [
                    "curl",
                    "--fail",
                    "--location",
                    "--retry",
                    "2",
                    "--max-time",
                    "120",
                    "--output",
                    str(download),
                    f"https://github.com/tlaplus/tlaplus/releases/download/v{VERSION}/tla2tools.jar",
                ],
                check=True,
            )
            if hashlib.sha256(download.read_bytes()).hexdigest() != JAR_SHA256:
                raise RuntimeError("Downloaded TLC checksum mismatch")
            download.replace(jar)
        finally:
            download.unlink(missing_ok=True)
    if hashlib.sha256(jar.read_bytes()).hexdigest() != JAR_SHA256:
        raise RuntimeError(f"TLC checksum mismatch: {jar}")
    return jar


def check(
    java: str,
    jar: Path,
    name: str,
    modes: str = "AppendAppend",
    bug: str | None = None,
    violation: str | None = None,
    module: str = "CommitRetry",
    safety: str = SAFETY,
) -> str:
    config = (
        (MODEL / f"{module}.cfg")
        .read_text()
        .replace("Modes <- AppendAppend", f"Modes <- {modes}")
    )
    if bug:
        config = config.replace(f"{bug} = FALSE", f"{bug} = TRUE")
    if violation:
        # Isolate the property this negative control is meant to falsify.
        config = config.replace(
            f"INVARIANTS {safety}", f"INVARIANTS TypeOK {violation}"
        )
    cfg = WORK / f"{name}.cfg"
    cfg.write_text(config)
    result = subprocess.run(
        [
            java,
            "-XX:+UseParallelGC",
            "-Xmx1g",
            "-Djava.io.tmpdir=" + str(WORK),
            "-jar",
            str(jar),
            "-workers",
            "1",
            "-fp",
            "0",
            "-seed",
            "1",
            "-metadir",
            str(WORK / f"states-{name}"),
            "-config",
            str(cfg.relative_to(ROOT)),
            str((MODEL / f"{module}.tla").relative_to(ROOT)),
        ],
        check=False,
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60,
    )
    (WORK / f"{name}.log").write_text(result.stdout)
    if violation:
        passed = (
            result.returncode == 12
            and f"Invariant {violation} is violated" in result.stdout
        )
    else:
        passed = (
            result.returncode == 0
            and "Model checking completed. No error has been found." in result.stdout
        )
    if not passed:
        raise RuntimeError(
            f"Unexpected TLC result for {name} (exit {result.returncode}):\n{result.stdout}"
        )
    stats = re.search(
        r"([\d,]+ states generated, [\d,]+ distinct states found[^\n]*)", result.stdout
    )
    print(
        f"{name}: {'expected counterexample' if violation else 'PASS'}; {stats[1] if stats else 'see log'}"
    )
    return result.stdout


def witness(output: str) -> dict:
    steps = []
    for state in re.split(r"\nState \d+:", output)[1:]:
        action = re.search(r'/\\ lastAction = "([a-z-]+)"', state)
        actor = re.search(r"/\\ actor = (\d+)", state)
        if not action or not actor:
            raise RuntimeError("Cannot extract action/actor from TLC witness")
        if action[1] != "init":
            steps.append({"action": action[1], "writer": int(actor[1])})
    if not steps:
        raise RuntimeError("TLC produced no witness steps")
    return {
        "model": "CommitRetry",
        "configuration": "AppendAppend; MaxAttempts=2; all bugs FALSE",
        "property": "NoAppendRetryWitness",
        "steps": steps,
        "expectedRows": [0, 1, 2],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update-trace",
        action="store_true",
        help="Regenerate the committed C# replay witness",
    )
    args = parser.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    java, jar = java_binary(), tools_jar()
    for modes in ("AppendAppend", "AppendReplace", "ReplaceReplace"):
        check(java, jar, modes, modes)
    check(java, jar, "broken-cas", bug="BugCAS", violation="RowsMatchSerialIntent")
    check(
        java,
        jar,
        "stale-sources",
        "ReplaceReplace",
        "BugStaleSources",
        "RowsMatchSerialIntent",
    )
    check(java, jar, "unknown-retry", bug="BugUnknownRetry", violation="AtMostOnce")
    check(
        java,
        jar,
        "unknown-cleanup",
        bug="BugCleanup",
        violation="UnknownFilesPreserved",
    )
    trace = witness(
        check(java, jar, "append-witness", violation="NoAppendRetryWitness")
    )
    if args.update_trace:
        TRACE.parent.mkdir(parents=True, exist_ok=True)
        TRACE.write_text(json.dumps(trace, indent=2) + "\n")
        print(f"Updated {TRACE.relative_to(ROOT)}")
    elif not TRACE.exists() or json.loads(TRACE.read_text()) != trace:
        raise RuntimeError(
            "Replay witness differs; inspect the model change and run --update-trace"
        )
    else:
        print("Committed C# replay witness matches TLC")

    check(java, jar, "write-plan", module="WritePlan", safety=FENCE_SAFETY)
    for name, bug in (
        ("missing-schema-requirement", "BugAuthority"),
        ("stale-schema-rebind", "BugRetryRebind"),
    ):
        check(
            java,
            jar,
            name,
            bug=bug,
            violation="PublishedSchemaCorrect",
            module="WritePlan",
            safety=FENCE_SAFETY,
        )
    check(
        java,
        jar,
        "archived-spec-witness",
        violation="NoArchivedSpecWitness",
        module="WritePlan",
        safety=FENCE_SAFETY,
    )


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
