# Contributing to Sluice

Sluice is on a path toward donation to the Apache Software Foundation (ASF)
Incubator. This document explains how to contribute today, and how
contribution mechanics will work once the project enters incubation.

## Ways to contribute

- Bug reports and feature requests (JIRA, see below)
- Code contributions (readers/writers/transforms, DAG factory, Autosys
  generator, docs, tests)
- Documentation improvements
- Triage / code review on open pull requests

## Before your first contribution

### 1. Sign the Individual Contributor License Agreement (ICLA)

The ASF requires every contributor whose changes will be merged to have a
signed ICLA on file once the project is under ASF governance. This is a
one-time, per-person agreement (not per-project) that grants the ASF the
rights it needs to distribute your contribution under the Apache License.

- Template: https://www.apache.org/licenses/icla.pdf
- Submit per instructions at https://www.apache.org/licenses/#clas
- See `docs/APACHE_ONBOARDING.md` for the full step-by-step process.

Until the project is accepted into incubation, contributions are still
governed by the repository's Apache License, Version 2.0 (`LICENSE`); the
ICLA becomes a hard requirement once ASF infrastructure (JIRA, Git-over-ASF,
mailing lists) is in use.

### 2. Get a JIRA account

Apache projects track work in ASF JIRA (or the project's chosen tracker
during incubation). See `docs/APACHE_ONBOARDING.md` for account setup and
project-key request steps.

### 3. Subscribe to the mailing list

Design discussions, release votes, and roadmap decisions for ASF projects
happen on public mailing lists, not private channels — this is the ASF's
"community over code" principle: decisions must be visible and archived.
Once the dev list exists (`dev@sluice.apache.org` during incubation), the
subscribe address is `dev-subscribe@sluice.apache.org`.

## Development workflow

1. **Find or file an issue.** Every non-trivial change should have a JIRA
   ticket (e.g. `SLUICE-123`) describing the problem before code is written.
2. **Branch naming:** `feature/SLUICE-123-short-description` or
   `fix/SLUICE-123-short-description`.
3. **Commit messages:** reference the ticket key, e.g.
   `SLUICE-123: Add Kafka reader schema-registry support`. See
   `DEVELOPER_GUIDELINES.md` for the full commit convention.
4. **Tests required.** New readers/writers/transforms need unit tests
   (`tests/unit/...`); pipeline-level changes need an integration or e2e
   test. Run `pytest --cov=src --cov-report=term` before opening a PR —
   don't drop overall coverage.
5. **License headers.** Every new source file must carry the standard
   Apache license header (see any existing file under `src/` for the exact
   block, or `docs/APACHE_ONBOARDING.md`).
6. **Open a pull request** against `main`. Link the JIRA ticket in the PR
   description. At least one existing committer/PPMC member must approve
   before merge (see `DEVELOPER_GUIDELINES.md` for the review/merge model).
7. **Discuss non-trivial design changes on the dev mailing list first**
   (once it exists) — PRs that introduce new source/target connectors,
   change the config schema, or alter the pipeline's public API should have
   a `[DISCUSS]` thread before implementation, per ASF norms.

## Code style

- Python 3.11+, type hints on public functions.
- No commented-out code, no unused imports.
- Prefer composition via the existing factory/registry patterns
  (`ReaderFactory`, `WriterFactory`) over new conditionals — see
  `src/ingestion/README.md`.

## Reporting security issues

Do not open a public JIRA ticket or PR for a security vulnerability. Once
under ASF governance, report privately to `security@sluice.apache.org` per
https://apache.org/security/. Until then, contact the project owners
directly (see `docs/APACHE_ONBOARDING.md`).

## Code of Conduct

All contributors are expected to follow `CODE_OF_CONDUCT.md`, which adopts
the ASF Code of Conduct (https://www.apache.org/foundation/policies/conduct).
