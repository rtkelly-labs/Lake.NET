# Documentation standards

What every document in this repository has to do. It applies to people and agents alike. A pull request that
changes behaviour, a public API, a command or a decision updates the docs that describe it, in the same pull
request.

## Progressive disclosure

A reader starts at the top and goes down only as far as they need. Each level answers its own question
completely and links to the next level for detail. It does not repeat that detail.

| Level | Where | Answers | Length |
|---|---|---|---|
| 0 | `README.md`, `VISION.md` | What is this, where is it going, how do I start | One screen of essentials; ~150 lines at most |
| 1 | `AGENTS.md`, `docs/README.md` | Where is the page for my task | Tables of links, one line each |
| 2 | A topic page or a topic folder's `README.md` (`docs/read-path.md`, `docs/lake/README.md`) | How does this area work, and what are its rules | Up to ~300 lines |
| 3 | Detail pages (`docs/lake/decisions.md`, `docs/upstream/*.md`, `docs/research/*.md`) | Every case, the evidence, the history | As long as it needs, with headings that can be linked to |

Rules:

- **Start with a summary.** The first paragraph says what the page covers, its status if it isn't
  describing what ships today, and what to read before it.
- **Link down, don't copy.** If a fact is explained in a lower-level page, summarise it in a line and link
  there. Every fact lives in exactly one place.
- **Split when a page passes ~300 lines** or covers two topics. A topic that needs several pages becomes a folder
  with a `README.md` as its level-2 page (`docs/lake/` is the example).
- **Every page is reachable** from `docs/README.md`. Pages you'd go to for a task also get a row in the
  `AGENTS.md` table.

## Kinds of document

| Kind | Describes | Where | Must have |
|---|---|---|---|
| Reference | How the code works now | `docs/<topic>.md` | Matches the code on `main`; names the tests that pin each rule |
| Target state | Where the code is going | `VISION.md`, `docs/lake/` | A status line saying it is not yet true; decisions with recommendations; the phase each change lands in |
| Runbook | How to run, test or release | `docs/regression-suite.md`, `docs/handover.md` | Commands you can copy and run from the repository root; the environment variables they need |
| Research | What was found outside the repository | `docs/research/<topic>-<yyyy-mm>.md` | The date, and a source for every claim; never edited later to change what was found, only superseded |
| Upstream notes | Tickets and patches for other projects | `docs/upstream/` | Whether it has been filed; drafts to file by hand where the project refuses LLM-written submissions |
| Generated | Output of a tool | Not committed (e.g. `docs/spec-traceability.md`) | The command that generates it, in the page that refers to it |

Once something on a target-state page ships, move the text into the reference page and leave a link behind.
A target-state page must not end up describing the current behaviour.

## Writing

- **Claims need evidence.** Name the test, fixture, command or source that shows it. Leave out a claim that
  hasn't been measured or checked, or mark it as an assumption (for example, Delta facts that come from the
  protocol and haven't been verified here).
- **Absolute dates** (`2026-10-28`), never "last week" or "recently". Versions are pinned (`DuckDB 1.5.5`).
- **Plain and specific.** Short sentences, active voice, no marketing. Say what happens, not that it's
  "robust" or "seamless".
- **Use the code's names**, in backticks: `TableScan.ReadRowsAsync`, `IFileIO.WriteAsync(overwrite: false)`. Link
  to a file path when the reader will want to open it.
- **Tables** for comparisons, mappings and inventories. **Mermaid** for dependency and sequence diagrams.
  A plain-text block for a pipeline that fits on a few lines.
- **Record decisions** with an ID that other pages can link to. Give each one its options, a recommendation and
  an **Outcome** (`Open` until it is decided). When it is decided, put what was chosen and the date in
  **Outcome** ([lake/decisions.md](lake/decisions.md) is the example).
- **Spec terms** match the spec's wording and link to its section. Tests use the same section names (`[Spec("…")]`).

## Names and files

- File names are lower-case kebab-case (`read-path.md`). The only exceptions are the root files GitHub
  recognises: `README.md`, `VISION.md`, `AGENTS.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`.
- Use relative links (`../AGENTS.md`, `lake/decisions.md#d1`), never `github.com` URLs to files in this
  repository.
- Images and diagram sources go next to the page that uses them.

## Review checklist

- [ ] Does the first paragraph say what the page covers and its status?
- [ ] Is it reachable from `docs/README.md`, and from `AGENTS.md` if it is a task page?
- [ ] Is every fact in one place only, with everything else linking to it?
- [ ] Does each claim name its evidence?
- [ ] Are dates absolute and versions pinned?
- [ ] Do the commands run from the repository root as written?
- [ ] If a change shipped, did its text move from the target-state page to the reference page?
