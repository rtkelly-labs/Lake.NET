"""Smoke a published server executable, including durable state across a restart (stdlib only)."""

from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request


def start(assembly: Path, root: Path):
    env = {k: v for k, v in os.environ.items() if not k.lower().startswith("iceberg__")}
    env.update(
        Iceberg__Store="sqlite",
        Iceberg__SqlitePath=str(root / "catalog.db"),
        Iceberg__Warehouse=str(root / "warehouse"),
    )
    process = subprocess.Popen(
        ["dotnet", str(assembly), "--urls", "http://127.0.0.1:0"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines: queue.Queue[str] = queue.Queue()
    log: list[str] = []

    def read():
        assert process.stdout
        for line in process.stdout:
            log.append(line)
            lines.put(line)

    threading.Thread(target=read, daemon=True).start()
    try:
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                line = lines.get(timeout=0.2)
            except queue.Empty:
                if process.poll() is not None:
                    raise RuntimeError(
                        "Server exited before listening: " + "".join(log)
                    )
                continue
            if "Now listening on: http://127.0.0.1:" in line:
                return process, line.split("Now listening on:", 1)[1].strip(), log
        raise TimeoutError("Server did not listen: " + "".join(log))
    except BaseException:
        stop(process)
        raise


def stop(process):
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    if process.stdout:
        process.stdout.close()


def request(base: str, path: str, body=None, key=None):
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    data = None if body is None else json.dumps(body).encode()
    with urllib.request.urlopen(
        urllib.request.Request(base + path, data=data, headers=headers), timeout=15
    ) as response:
        content = response.read()
        return (json.loads(content) if content else None), response.headers


def main():
    if len(sys.argv) not in (3, 4):
        raise SystemExit(
            "Usage: server-smoke.py <published-server-directory> <temporary-directory> [consumer-project]"
        )
    assembly = Path(sys.argv[1]).resolve() / "Iceberg.Net.RestServer.dll"
    if not assembly.is_file():
        raise SystemExit(f"Missing published server: {assembly}")
    Path(sys.argv[2]).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=sys.argv[2], prefix="server-smoke-") as work:
        root = Path(work).resolve()
        process, base, log = start(assembly, root)
        namespace = {"namespace": ["smoke"]}
        try:
            config, _ = request(base, "/v1/config")
            assert "endpoints" in config
            request(base, "/v1/namespaces", namespace, key="restart-key")
            created, _ = request(
                base,
                "/v1/namespaces/smoke/tables",
                {
                    "name": "events",
                    "schema": {
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [
                            {"id": 1, "name": "id", "required": True, "type": "long"}
                        ],
                    },
                },
            )
            uuid = created["metadata"]["table-uuid"]
            if len(sys.argv) == 4:
                subprocess.run(
                    [
                        "dotnet",
                        "run",
                        "--project",
                        sys.argv[3],
                        "--no-build",
                        "--",
                        base,
                    ],
                    check=True,
                )
            document, _ = request(base, "/openapi/v1.json")
            assert document["paths"]
        except BaseException:
            print("".join(log), file=sys.stderr)
            raise
        finally:
            stop(process)
        process, base, log = start(assembly, root)
        try:
            loaded, _ = request(base, "/v1/namespaces/smoke/tables/events")
            assert loaded["metadata"]["table-uuid"] == uuid
            if len(sys.argv) == 4:
                consumer_table, _ = request(base, "/v1/namespaces/smoke/tables/orders")
                assert len(consumer_table["metadata"]["snapshots"]) == 1
            _, headers = request(base, "/v1/namespaces", namespace, key="restart-key")
            assert headers.get("Idempotent-Replayed") == "true"
        except BaseException:
            print("".join(log), file=sys.stderr)
            raise
        finally:
            stop(process)
    print(
        "Published server OK: OpenAPI, SQLite table persistence and idempotent replay across restart"
    )


if __name__ == "__main__":
    main()
