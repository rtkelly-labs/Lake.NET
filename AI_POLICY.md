# Generative AI & AI-Assisted Contribution Policy

This repository adheres to standard open-source community guidelines for the ethical, transparent, and accountable utilization of Artificial Intelligence (AI) and Machine Learning (ML) tools.

---

## 1. Scope & Core Principle

This policy applies to all contributions, code, documentation, and issues submitted to `rtkelly-labs/Iceberg.NET`.

### 🔑 The Core Principle: Human Accountability
**The human contributor or maintainer is 100% accountable for all submitted work.** 

Using AI tools (such as GitHub Copilot, Anthropic Claude, OpenAI ChatGPT, Cursor, Google Antigravity, or other Large Language Models) is welcomed as an assistive productivity tool, but an AI tool cannot be listed as an author or bear responsibility for a pull request.

---

## 2. Guidelines for Contributors

### A. Permitted Use
Contributors are permitted to use generative AI tools to assist with:
* Drafting code implementations, refactoring, and optimizations.
* Generating boilerplate, unit tests, and mock datasets.
* Writing or polishing technical documentation and XML comments.
* Researching API specifications and type mappings.

### B. Prohibited Use
The following practices are strictly prohibited:
* **Autonomous PR Submissions**: Opening issues or pull requests generated autonomously by automated bots or agents without active human curation and verification.
* **Unreviewed Code ("AI Slop")**: Submitting code or explanations that the contributor has not personally read, understood, and tested.
* **Hallucinated References**: Submitting PRs with non-existent dependencies, invalid type signatures, or unverified performance claims.

---

## 3. Review & Verification Requirements

Every AI-assisted contribution must meet the project's standard quality gates:

1. **Full Comprehension**: You must be able to explain the logic, design decisions, and trade-offs in code review.
2. **Automated Test Coverage**: New features or bug fixes must include unit or integration tests verifying the behavior on all supported runtimes (.NET 8 & .NET 9).
3. **Build & Formatting Compliance**: All code must compile/lint cleanly without warnings and pass automated code style checks.

---

## 4. Disclosure & Attribution

In alignment with open-source transparency best practices:

* **Pull Request Descriptions**: If a substantial portion of a pull request was generated with AI assistance, please include a brief disclosure in the PR description (e.g., *"Assisted by Claude / Copilot / Antigravity"*).
* **Git Commit Trailers**: Contributors are encouraged to include git trailers when applicable:
  ```git
  Assisted-by: AI Assistant <noreply@anthropic.com>
  Co-authored-by: AI Assistant <noreply@google.com>
  ```

---

## 5. Intellectual Property & Licensing

* Contributors must ensure that any AI-generated content does not violate third-party intellectual property rights or incorporate proprietary/copyleft source code inconsistent with this project's **MIT License**.
* By submitting a pull request, you certify that you have the right to license the contribution under the project's [MIT License](LICENSE).
