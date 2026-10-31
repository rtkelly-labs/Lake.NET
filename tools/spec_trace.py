"""Spec traceability and drift for iceberg-dotnet.

    python tools/spec_trace.py snapshot [--ref TAG]   pin upstream spec sections (docs/spec/sections.json)
    python tools/spec_trace.py snapshot --ducklake [--ref SHA]
                                                      pin the DuckLake spec (docs/spec/ducklake-sections.json)
    python tools/spec_trace.py matrix [--check]        tests x sections -> docs/spec-traceability.md
    python tools/spec_trace.py drift [--ref main] [--acknowledge] [--ducklake]
                                                      what changed upstream since the pinned snapshot

Sections are the headings of format/spec.md and format/puffin-spec.md, and the operationIds of the REST OpenAPI
document, each with a hash of its own text. Tests link to them with [Spec("Row Lineage")] (C#) or @spec:row-lineage
(Gherkin); DuckLake sections with [Spec("ducklake: INSERT")]: the pages of the DuckLake specification (one section
per page title and heading, across pages) from duckdb/ducklake-web. `matrix --check` fails on links to sections that do not exist; `drift` exits 1 when pinned sections
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

# DuckLake (src/Iceberg.Net.DuckLake): the 1.1 specification pages ("stable" at the pinned commit), plus the two DuckDB pages that define behaviour the
# specification relies on (conflict resolution, inlined data), pinned at a duckdb/ducklake-web commit.
DUCKLAKE_SNAPSHOT = ROOT / "docs" / "spec" / "ducklake-sections.json"
DUCKLAKE_ACKNOWLEDGED = ROOT / "docs" / "spec" / "ducklake-acknowledged-drift.json"
DUCKLAKE_RAW = "https://raw.githubusercontent.com/duckdb/ducklake-web/{ref}/{path}"
DUCKLAKE_PINNED_REF = "9d095946cd3f930774171e3d397116ad504931e8"
DUCKLAKE_TABLES = [
    "ducklake_snapshot",
    "ducklake_snapshot_changes",
    "ducklake_schema",
    "ducklake_table",
    "ducklake_view",
    "ducklake_column",
    "ducklake_macro",
    "ducklake_macro_impl",
    "ducklake_macro_parameters",
    "ducklake_data_file",
    "ducklake_delete_file",
    "ducklake_files_scheduled_for_deletion",
    "ducklake_inlined_data_tables",
    "ducklake_column_mapping",
    "ducklake_name_mapping",
    "ducklake_table_stats",
    "ducklake_table_column_stats",
    "ducklake_file_column_stats",
    "ducklake_file_variant_stats",
    "ducklake_partition_info",
    "ducklake_partition_column",
    "ducklake_file_partition_value",
    "ducklake_sort_info",
    "ducklake_sort_expression",
    "ducklake_metadata",
    "ducklake_tag",
    "ducklake_column_tag",
    "ducklake_schema_versions",
    "ducklake_view_column_tag",
]
DUCKLAKE_DOCS = [
    "docs/stable/specification/introduction.md",
    "docs/stable/specification/queries.md",
    "docs/stable/specification/data_types.md",
    "docs/stable/specification/tables/overview.md",
    *[f"docs/stable/specification/tables/{t}.md" for t in DUCKLAKE_TABLES],
    "docs/stable/duckdb/advanced_features/conflict_resolution.md",
    "docs/stable/duckdb/advanced_features/data_inlining.md",
]

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

INFORMATIVE |= {
    "ducklake:introduction",
    "ducklake:building-blocks",
    "ducklake:catalog-database",
    "ducklake:data-storage",
    "ducklake:tables",
    "ducklake:queries",
    "ducklake:reading-data",
    "ducklake:writing-data",
    "ducklake:snapshots",
    "ducklake:ducklake-schema",
    "ducklake:macros",
    "ducklake:data-files-and-tables",
    "ducklake:data-file-mapping",
    "ducklake:statistics",
    "ducklake:partitioning-information",
    "ducklake:sort-information",
    "ducklake:auxiliary-tables",
    "ducklake:full-schema-creation-script",
    "ducklake:columns",
    "ducklake:examples",
    "ducklake:logical-conflicts",
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
    # The REST server implements every other operation, including those the client has no method for
    # (updateProperties, unregisterTable, reportMetrics); tests reach those over HTTP.
    **{
        f"rest:{op}": "REST operation not implemented (client or server)"
        for op in (
            "listfunctions",
            "loadfunction",
            "registerview",
        )
    },
    # DuckLake: keep in step with docs/ducklake.md ("What is not implemented").
    **{
        f"ducklake:{k}": reason
        for k, reason in {
            "ducklake_file_variant_stats": "variant type not implemented",
            "semi-structured-types": "variant type not implemented",
            "geometry-types": "geometry type not implemented",
            "extra-statistics": "geometry and variant statistics not implemented",
            "geometry": "geometry type not implemented",
            "variant": "variant type not implemented",
            # DuckDB's own inlining settings and maintenance calls: extension behaviour, not format.
            **{
                k: "DuckDB extension setting or function, not part of the format"
                for k in (
                    "global-default",
                    "per-connection-override",
                    "per-attach-override",
                    "return-values",
                    "sorted-flush",
                    "interaction-with-auto_compact",
                    "metadata-catalog-support",
                )
            },
        }.items()
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


def markdown_sections(
    doc: str, text: str, seen: dict[str, int] | None = None
) -> list[Section]:
    out: list[Section] = []
    current: tuple[str, int] | None = None
    body: list[str] = []
    in_code = False
    seen = {} if seen is None else seen

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


def fetch_ducklake(ref: str, source: str | None) -> list[Section]:
    """Every DuckLake page as one document: its front-matter title is a level-1 section, keys are unique across pages."""
    seen: dict[str, int] = {}
    out: list[Section] = []
    for path in DUCKLAKE_DOCS:
        if source:
            text = (Path(source) / path).read_text()
        else:
            with urllib.request.urlopen(
                DUCKLAKE_RAW.format(ref=ref, path=path), timeout=60
            ) as r:  # noqa: S310 - fixed https URL
                text = r.read().decode()
        title = re.search(r"^title:\s*(.+?)\s*$", text, re.MULTILINE)
        body = re.sub(r"\A---\n.*?\n---\n", "", text, flags=re.DOTALL)
        out += markdown_sections(
            "ducklake", f"# {title.group(1) if title else path}\n{body}", seen
        )
    return out


def load_snapshot() -> dict:
    snap = json.loads(SNAPSHOT.read_text())
    if DUCKLAKE_SNAPSHOT.exists():
        ducklake = json.loads(DUCKLAKE_SNAPSHOT.read_text())
        snap["sections"]["ducklake"] = ducklake["sections"]["ducklake"]
        snap["ducklake_ref"] = ducklake["ref"]
    return snap


def cmd_snapshot_ducklake(ref: str, source: str | None) -> int:
    sections = fetch_ducklake(ref, source)
    data = {
        "ref": ref,
        "repository": "duckdb/ducklake-web",
        "documents": DUCKLAKE_DOCS,
        "sections": {"ducklake": [s.__dict__ for s in sections]},
    }
    DUCKLAKE_SNAPSHOT.write_text(json.dumps(data, indent=1) + "\n")
    print(
        f"pinned {len(sections)} DuckLake sections at {ref} -> {DUCKLAKE_SNAPSHOT.relative_to(ROOT)}"
    )
    return 0


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
                        if section.split(":")[0] in (*DOCS, "ducklake")
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
        and not k.startswith(("rest:", "ducklake:"))
    ]
    ducklake = [
        k
        for k in known
        if k.startswith("ducklake:")
        and k not in NOT_IMPLEMENTED
        and k not in INFORMATIVE
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
        (
            f"- DuckLake specification sections linked (pinned at `{snap.get('ducklake_ref', '-')}`, "
            f"`docs/spec/ducklake-sections.json`): **{sum(1 for k in ducklake if k in found)} / {len(ducklake)}**"
        ),
        f"- Tests and scenarios carrying links: {len({t for ts in found.values() for t in ts})}",
        "",
        "Status: ✅ linked tests · ⛔ deliberately not implemented · · informative heading · ⬜ no linked test yet.",
        "",
    ]
    titles = {
        "spec": "Table spec (format/spec.md)",
        "puffin": "Puffin (format/puffin-spec.md)",
        "rest": "REST catalog operations (OpenAPI)",
        "ducklake": "DuckLake 1.1 (duckdb/ducklake-web docs/stable/specification, conflict resolution, data inlining)",
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


def cmd_drift(
    ref: str, source: str | None, acknowledge: bool, ducklake: bool = False
) -> int:
    """Report upstream changes since the pinned snapshot. Changes already reviewed (recorded in ACKNOWLEDGED with
    --acknowledge) are listed but do not fail; a section that changes again after review fails again."""
    snap = load_snapshot()
    if ducklake:
        docs = {"ducklake": "duckdb/ducklake-web docs/stable"}
        upstream = {"ducklake": fetch_ducklake(ref, source)}
        acknowledged, pinned, name = (
            DUCKLAKE_ACKNOWLEDGED,
            snap.get("ducklake_ref"),
            "DuckLake",
        )
    else:
        docs, upstream, acknowledged, pinned, name = (
            DOCS,
            fetch(ref, source),
            ACKNOWLEDGED,
            snap["ref"],
            "Iceberg",
        )
    found = links()
    acked: dict[str, str] = (
        json.loads(acknowledged.read_text())["sections"]
        if acknowledged.exists()
        else {}
    )
    current: dict[str, str] = {}
    report = [f"# {name} spec drift: pinned `{pinned}` → `{ref}`", ""]
    unreviewed = 0
    for doc in docs:
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
        report += [f"## {docs[doc]}", ""]
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
        acknowledged.write_text(
            json.dumps({"ref": ref, "sections": current}, indent=1, sort_keys=True)
            + "\n"
        )
        report.append(
            f"Acknowledged {len(current)} upstream changes -> {acknowledged.relative_to(ROOT)}."
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
    s.add_argument(
        "--ducklake", action="store_true", help="pin the DuckLake specification instead"
    )
    m = sub.add_parser("matrix")
    m.add_argument(
        "--check", action="store_true", help="fail on links to unknown sections"
    )
    d = sub.add_parser("drift")
    d.add_argument("--ref", default="main")
    d.add_argument("--source")
    d.add_argument(
        "--ducklake", action="store_true", help="the DuckLake specification instead"
    )
    d.add_argument(
        "--acknowledge",
        action="store_true",
        help="record the current upstream changes as reviewed",
    )
    a = p.parse_args()
    if a.cmd == "snapshot" and a.ducklake:
        return cmd_snapshot_ducklake(
            DUCKLAKE_PINNED_REF if a.ref == PINNED_REF else a.ref, a.source
        )
    if a.cmd == "snapshot":
        return cmd_snapshot(a.ref, a.source)
    if a.cmd == "matrix":
        return cmd_matrix(a.check)
    return cmd_drift(a.ref, a.source, a.acknowledge, a.ducklake)


if __name__ == "__main__":
    sys.exit(main())
