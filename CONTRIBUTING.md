# Contributing to Iceberg.NET

Thank you for your interest in contributing to **Iceberg.NET**! We welcome bug reports, feature suggestions, documentation improvements, and code contributions.

---

## Code of Conduct

All contributors and maintainers are expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions.

---

## Development Workflow

We follow a strict, modern GitHub workflow across our repositories:

1. **Fork or Branch**:
   - Create a feature or bugfix branch using clear naming:
     - `feat/<feature-name>`
     - `fix/<bug-description>`
     - `docs/<doc-update>`
     - `chore/<maintenance-task>`

2. **Linear History & Rebase**:
   - We enforce a strictly linear history on `main`.
   - Before opening a PR or marking it ready for review, rebase your branch on latest `main`:
     ```bash
     git fetch origin
     git rebase origin/main
     ```

3. **Code Quality & Testing**:
   - Ensure your code passes all local linting, type checks, and automated tests.
   - Spec tests (no network):
     ```bash
     ./run-tests.sh spec
     ```
   - The full suite, as CI runs it (needs Docker; starts the emulators, Spark, Trino and Polaris):
     ```bash
     ./run-tests.sh emulation
     ```

4. **Pull Requests**:
   - Fill out the PR template completely with linked issues and verification steps.
   - All pull requests are merged exclusively via **Squash and Merge**.

---

## Reporting Issues

- **Bug Reports**: Please use the [Bug Report](.github/ISSUE_TEMPLATE/bug_report.yml) form. Provide exact reproduction steps and system versions.
- **Feature Requests**: Please use the [Feature Request](.github/ISSUE_TEMPLATE/feature_request.yml) form.

Thank you for helping make Iceberg.NET better!
