"""Refresh docs/spec/open-api/rest-catalog-open-api.json: the Iceberg REST OpenAPI at the pinned ref, as JSON.

OpenApiConformanceTests diff the REST server's generated document against this copy. The ref is the one
docs/spec/sections.json pins, so the spec traceability matrix and the conformance test read the same spec.

    uv run --with pyyaml --no-project python -I tools/openapi_reference.py
"""

import hashlib
import json
import pathlib
import urllib.request

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
REF = json.loads((ROOT / "docs/spec/sections.json").read_text(encoding="utf-8"))["ref"]
URL = f"https://raw.githubusercontent.com/apache/iceberg/{REF}/open-api/rest-catalog-open-api.yaml"
OUT = ROOT / "docs/spec/open-api/rest-catalog-open-api.json"


def main() -> None:
    raw = urllib.request.urlopen(URL, timeout=60).read()  # noqa: S310 - fixed https URL
    spec = yaml.safe_load(raw)
    spec["x-iceberg-net-source"] = {"url": URL, "ref": REF, "sha256": hashlib.sha256(raw).hexdigest()}
    OUT.write_text(json.dumps(spec, indent=1, sort_keys=False, default=str) + "\n", encoding="utf-8")
    print(f"{OUT.relative_to(ROOT)} <- {URL}")


if __name__ == "__main__":
    main()
