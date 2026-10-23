"""Spec traceability and drift for iceberg-dotnet.

    python tools/spec_trace.py snapshot [--ref TAG]   pin upstream spec sections (docs/spec/sections.json)
    python tools/spec_trace.py matrix [--check]        tests x sections -> docs/spec-traceability.md
    python tools/spec_trace.py drift [--ref main] [--acknowledge]
                                                      what changed upstream since the pinned snapshot

Sections are the headings of format/spec.md and format/puffin-spec.md, and the operationIds of the REST OpenAPI
document, each with a hash of its own text. Tests link to them with [Spec("Row Lineage")] (C#) or @spec:row-lineage
(Gherkin). `matrix --check` fails on links to sections that do not exist; `drift` exits 1 when pinned sections
changed, disappeared or were added upstream and nobody has reviewed the change yet (docs/spec/acknowledged-drift.json),
naming the tests that cover each changed section.
Standard library only: runs anywhere Python 3.10+ does.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = ROOT / "docs" / "spec" / "sections.json"
MATRIX = ROOT / "docs" / "spec-traceability.md"
ACKNOWLEDGED = ROOT / "docs" / "spec" / "acknowledged-drift.json"
RAW = "https://raw.githubusercontent.com/apache/iceberg/{ref}/{path}"
DOCS = {
    "spec": "format/spec.md",
    "puffin": "format/puffin-spec.md",
    "rest": "open-api/rest-catalog-open-api.yaml",
}
PINNED_REF = "apache-iceberg-1.12.0"  # the release the Spark oracle runs (emulation/up.sh --spark)

# Container and prose headings: nothing to test directly (their subsections carry the requirements).
INFORMATIVE = {
    "iceberg-table-spec",
    "goals",
    "overview",
    "specification",
    "terms",
    "appendix-a-format-specific-requirements",
    "appendix-c-json-serialization",
    "appendix-e-format-version-changes",
    "appendix-f-implementation-notes",
    "other-fields",
    "puffin:puffin-file-format",
    "puffin:format-specification",
    "puffin:blob-types",
}

# Sections deliberately not implemented (rendered as such instead of as gaps). Keep in step with docs/spec-coverage.md.
NOT_IMPLEMENTED = {
    "version-4-metadata-structure-and-representation": "v4 is in development upstream",
    "semi-structured-types": "variant type not implemented",
    "crs": "geometry/geography not implemented",
    "edge-interpolation-algorithm": "geometry/geography not implemented",
    "bounds-for-geometry-and-geography": "geometry/geography not implemented",
    "bounds-for-variant": "variant type not implemented",
    "encryption-keys": "table encryption not implemented (keys round-trip in metadata only)",
    "manifest-deletion-vectors": "v4 proposal",
    "content-stats": "v4 proposal",
    "field-statistics": "v4 proposal",
    "content-stats-in-manifests": "v4 proposal",
    "version-4": "v4 is in development upstream",
    "path-construction": "v4 relative paths",
    "orc": "ORC data files not implemented (Parquet only)",
    "appendix-g-geospatial-notes": "geometry/geography not implemented",
    "position-delete-files-with-row-data": "optional row column not written (deprecated upstream)",
    "partition-statistics-file": "partition statistics files not written",
    **{
        f"rest:{op}": "REST operation not implemented by RestCatalog"
        for op in (
            "updateproperties",
            "listfunctions",
            "loadfunction",
            "plantablescan",
            "fetchplanningresult",
            "cancelplanning",
            "fetchscantasks",
            "unregistertable",
            "loadcredentials",
            "signrequest",
            "reportmetrics",
            "committransaction",
            "listviews",
            "createview",
            "loadview",
            "replaceview",
            "dropview",
            "viewexists",
            "renameview",
            "registerview",
        )
    },
}


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "-", text.lower()).strip("-")


def key(doc: str, heading: str) -> str:
    return slug(heading) if doc == "spec" else f"{doc}:{slug(heading)}"


@dataclass
class Section:
    key: str
    title: str
    level: int
    sha: str


def digest(lines: list[str]) -> str:
    # Whitespace-insensitive: markdown reflows and trailing spaces are not spec changes.
    text = " ".join(" ".join(lines).split())
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def markdown_sections(doc: str, text: str) -> list[Section]:
    out: list[Section] = []
    current: tuple[str, int] | None = None
    body: list[str] = []
    in_code = False
    seen: dict[str, int] = {}

    def flush() -> None:
        if current:
            k = key(doc, current[0])
            seen[k] = seen.get(k, 0) + 1
            if seen[k] > 1:
                k = f"{k}-{seen[k]}"
            out.append(Section(k, current[0], current[1], digest(body)))

    for line in text.splitlines():
        if line.startswith("```"):
            in_code = not in_code
        m = None if in_code else re.match(r"^(#{1,6})\s+(.*?)\s*$", line)
        if m:
            flush()
            current, body = (m.group(2).replace("`", ""), len(m.group(1))), []
        else:
            body.append(line)
    flush()
    return out


def openapi_sections(text: str) -> list[Section]:
    """One section per operationId: the lines from its HTTP verb to the next verb or path."""
    lines = text.splitlines()
    out: list[Section] = []
    starts = [
        i
        for i, line in enumerate(lines)
        if re.match(r"^    (get|put|post|delete|head|patch):", line)
    ]
    for n, start in enumerate(starts):
        end = next(
            (
                i
                for i in range(start + 1, len(lines))
                if re.match(r"^ {2}\S|^ {4}\S", lines[i])
            ),
            len(lines),
        )
        if n + 1 < len(starts):
            end = min(end, starts[n + 1])
        block = lines[start:end]
        op = next(
            (
                re.search(r"operationId:\s*(\S+)", line)
                for line in block
                if "operationId:" in line
            ),
            None,
        )
        if op:
            out.append(Section(key("rest", op.group(1)), op.group(1), 3, digest(block)))
    return out


def fetch(ref: str, source: str | None) -> dict[str, list[Section]]:
    result = {}
    for doc, path in DOCS.items():
        if source:
            text = (Path(source) / Path(path).name).read_text()
        else:
            with urllib.request.urlopen(
                RAW.format(ref=ref, path=path), timeout=60
            ) as r:  # noqa: S310 - fixed https URL
                text = r.read().decode()
        result[doc] = (
            openapi_sections(text) if doc == "rest" else markdown_sections(doc, text)
        )
    return result


def load_snapshot() -> dict:
    return json.loads(SNAPSHOT.read_text())


def cmd_snapshot(ref: str, source: str | None) -> int:
    docs = fetch(ref, source)
    SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "ref": ref,
        "documents": {doc: DOCS[doc] for doc in DOCS},
        "sections": {doc: [s.__dict__ for s in secs] for doc, secs in docs.items()},
    }
    SNAPSHOT.write_text(json.dumps(data, indent=1) + "\n")
    print(
        f"pinned {sum(len(s) for s in docs.values())} sections at {ref} -> {SNAPSHOT.relative_to(ROOT)}"
    )
    return 0


SPEC_ATTR = re.compile(r'\[Spec\("((?:[^"\\]|\\.)*)"\)\]')
TYPE_DECL = re.compile(r"\b(?:class|record)\s+(\w+)")
METHOD_DECL = re.compile(r"\bpublic\s+(?:async\s+)?[\w<>\[\],.? ]+\s+(\w+)\s*\(")
FEATURE_TAG = re.compile(r"@spec:([\w:-]+)")


def links() -> dict[str, list[str]]:
    """section key -> test names, from [Spec] attributes and @spec: Gherkin tags."""
    found: dict[str, list[str]] = {}
    for cs in sorted((ROOT / "tests").rglob("*.cs")):
        if "/obj/" in str(cs) or "/bin/" in str(cs):
            continue
        lines = cs.read_text().splitlines()
        pending: list[str] = []
        for line in lines:
            if line.lstrip().startswith("//"):
                continue  # doc comments may show [Spec(...)] as an example
            pending += SPEC_ATTR.findall(line)
            if not pending:
                continue
            decl = TYPE_DECL.search(line) or METHOD_DECL.search(line)
            if decl and not line.strip().startswith("["):
                name = f"{cs.stem}" + (
                    "" if TYPE_DECL.search(line) else f".{decl.group(1)}"
                )
                for section in pending:
                    doc, _, heading = (
                        section.partition(":")
                        if section.split(":")[0] in DOCS
                        else ("spec", "", section)
                    )
                    found.setdefault(key(doc, heading or section), []).append(name)
                pending = []
    for feature in sorted((ROOT / "tests").rglob("*.feature")):
        scenario = None
        tags: list[str] = []
        for line in feature.read_text().splitlines():
            tags += FEATURE_TAG.findall(line)
            m = re.match(r"\s*(Feature|Scenario(?: Outline)?):\s*(.*)", line)
            if m:
                scenario = (
                    f"{feature.stem}: {m.group(2)}"
                    if m.group(1) != "Feature"
                    else f"{feature.stem}"
                )
                for t in tags:
                    found.setdefault(t, []).append(scenario)
                tags = []
    return found


def cmd_matrix(check: bool) -> int:
    snap = load_snapshot()
    known = {s["key"]: (doc, s) for doc, secs in snap["sections"].items() for s in secs}
    found = links()
    unknown = sorted(k for k in found if k not in known)
    covered = [k for k in known if k in found]
    impl = [
        k
        for k in known
        if k not in NOT_IMPLEMENTED
        and k not in INFORMATIVE
        and not k.startswith("rest:")
    ]
    rest = [k for k in known if k.startswith("rest:") and k not in NOT_IMPLEMENTED]
    out = [
        "# Spec traceability",
        "",
        f"Generated by `python tools/spec_trace.py matrix` from `[Spec(...)]` attributes and `@spec:` tags, against the "
        f"upstream sections pinned at **{snap['ref']}** (`docs/spec/sections.json`). Do not edit by hand.",
        "",
        f"- Table + Puffin spec sections linked to at least one test: **{sum(1 for k in impl if k in found)} / {len(impl)}** "
        "(excluding informative headings and sections marked not implemented)",
        f"- Implemented REST operations linked: **{sum(1 for k in rest if k in found)} / {len(rest)}**",
        f"- Tests and scenarios carrying links: {len({t for ts in found.values() for t in ts})}",
        "",
        "Status: ✅ linked tests · ⛔ deliberately not implemented · · informative heading · ⬜ no linked test yet.",
        "",
    ]
    titles = {
        "spec": "Table spec (format/spec.md)",
        "puffin": "Puffin (format/puffin-spec.md)",
        "rest": "REST catalog operations (OpenAPI)",
    }
    for doc, secs in snap["sections"].items():
        out += [
            f"## {titles[doc]}",
            "",
            "| Section | Status | Tests |",
            "|---|---|---|",
        ]
        for s in secs:
            k = s["key"]
            indent = "&nbsp;&nbsp;" * max(0, s["level"] - 2)
            tests = found.get(k, [])
            status = (
                "✅"
                if tests
                else "⛔ " + NOT_IMPLEMENTED[k]
                if k in NOT_IMPLEMENTED
                else "·"
                if k in INFORMATIVE
                else "⬜"
            )
            shown = ", ".join(f"`{t}`" for t in sorted(set(tests))[:6]) + (
                f" +{len(set(tests)) - 6}" if len(set(tests)) > 6 else ""
            )
            out.append(f"| {indent}{s['title']} | {status} | {shown} |")
        out.append("")
    if unknown:
        out += [
            "## Links to unknown sections",
            "",
            *[f"- `{k}` from {', '.join(found[k])}" for k in unknown],
            "",
        ]
    MATRIX.write_text("\n".join(out))
    print(
        f"{len(covered)} sections linked, {len(unknown)} unknown links -> {MATRIX.relative_to(ROOT)}"
    )
    for k in unknown:
        print(
            f"unknown section '{k}' referenced by {', '.join(found[k])}",
            file=sys.stderr,
        )
    return 1 if check and unknown else 0


def cmd_drift(ref: str, source: str | None, acknowledge: bool) -> int:
    """Report upstream changes since the pinned snapshot. Changes already reviewed (recorded in ACKNOWLEDGED with
    --acknowledge) are listed but do not fail; a section that changes again after review fails again."""
    snap = load_snapshot()
    upstream = fetch(ref, source)
    found = links()
    acked: dict[str, str] = (
        json.loads(ACKNOWLEDGED.read_text())["sections"]
        if ACKNOWLEDGED.exists()
        else {}
    )
    current: dict[str, str] = {}
    report = [f"# Iceberg spec drift: pinned `{snap['ref']}` → `{ref}`", ""]
    unreviewed = 0
    for doc in DOCS:
        old = {s["key"]: s for s in snap["sections"][doc]}
        new = {s.key: s for s in upstream[doc]}
        added = [k for k in new if k not in old]
        removed = [k for k in old if k not in new]
        changed = [k for k in old if k in new and old[k]["sha"] != new[k].sha]
        for k in added + changed:
            current[k] = new[k].sha
        for k in removed:
            current[k] = "removed"
        if not (added or removed or changed):
            continue
        report += [f"## {DOCS[doc]}", ""]
        for label, keys, title in (
            ("Changed", changed, lambda k: new[k].title),
            ("Added", added, lambda k: new[k].title),
            ("Removed", removed, lambda k: old[k]["title"]),
        ):
            if keys:
                report.append(f"**{label}**")
                for k in keys:
                    tests = sorted(set(found.get(k, [])))
                    reviewed = acked.get(k) == current[k]
                    unreviewed += not reviewed
                    report.append(
                        f"- {'' if reviewed else '🆕 '}{title(k)} (`{k}`)"
                        + (" — reviewed" if reviewed else "")
                        + (f" — covered by {', '.join(tests)}" if tests else "")
                    )
                report.append("")
    if acknowledge:
        ACKNOWLEDGED.write_text(
            json.dumps({"ref": ref, "sections": current}, indent=1, sort_keys=True)
            + "\n"
        )
        report.append(
            f"Acknowledged {len(current)} upstream changes -> {ACKNOWLEDGED.relative_to(ROOT)}."
        )
        unreviewed = 0
    elif not current:
        report.append("No upstream changes since the pinned snapshot.")
    elif unreviewed:
        report += [
            f"{unreviewed} change(s) marked 🆕 have not been reviewed. Assess each against the implementation and "
            "tests (docs/roadmap.md), then either re-pin to a release that contains them "
            "(`python tools/spec_trace.py snapshot --ref <tag>` + `matrix`) or record the review with "
            "`python tools/spec_trace.py drift --acknowledge`."
        ]
    else:
        report.append("Every upstream change has been reviewed.")
    text = "\n".join(report) + "\n"
    print(text)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(text)
    return 1 if unreviewed else 0


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("--ref", default=PINNED_REF)
    s.add_argument(
        "--source", help="read the documents from this directory instead of GitHub"
    )
    m = sub.add_parser("matrix")
    m.add_argument(
        "--check", action="store_true", help="fail on links to unknown sections"
    )
    d = sub.add_parser("drift")
    d.add_argument("--ref", default="main")
    d.add_argument("--source")
    d.add_argument(
        "--acknowledge",
        action="store_true",
        help="record the current upstream changes as reviewed",
    )
    a = p.parse_args()
    if a.cmd == "snapshot":
        return cmd_snapshot(a.ref, a.source)
    if a.cmd == "matrix":
        return cmd_matrix(a.check)
    return cmd_drift(a.ref, a.source, a.acknowledge)


if __name__ == "__main__":
    sys.exit(main())
