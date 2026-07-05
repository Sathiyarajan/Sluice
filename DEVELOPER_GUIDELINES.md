# Developer Guidelines

Practical rules for anyone writing code in this repo, aligned with how
Apache projects run day to day. Read `CONTRIBUTING.md` first for the
onboarding steps (ICLA, JIRA, mailing list).

## Coordination model

Apache projects run on three coordination channels — use the right one:

| Channel | Use for |
|---|---|
| **JIRA** (`SLUICE-<n>`) | Tracking a specific bug/feature/task. Every PR references one. |
| **dev mailing list** (`dev@sluice.apache.org` once it exists) | Design discussion, roadmap, release votes, anything the whole community should see and archive. Use `[DISCUSS]`, `[PROPOSAL]`, `[VOTE]` subject prefixes. |
| **Pull request comments** | Line-level code review on an already-agreed-upon change. |

Rule of thumb: if the change affects the public config schema
(`DatasetConfig` and friends), adds a new reader/writer type, or changes how
Airflow/Autosys schedule jobs, start a `[DISCUSS]` thread on the dev list
before writing code, not after.

## Branching and commits

- Branch: `feature/SLUICE-<n>-short-slug` or `fix/SLUICE-<n>-short-slug`.
- Commit subject: `SLUICE-<n>: <imperative summary>` (e.g. `SLUICE-142: Add
  schema-registry support to KafkaReader`).
- Commit body: explain *why*, not what — the diff already shows what.
- Squash trivial fixups before requesting review; keep meaningful commit
  boundaries (e.g. "add tests" as a separate commit from "implement feature"
  is fine, "fix typo" x5 is not).

## Code review and merge

- At least one approval from an existing committer or PPMC member (once the
  project has one) is required before merge — this is the ASF's
  meritocracy model: code review is how trust and eventually committer
  status are earned.
- Reviewers check: correctness, test coverage, license header presence,
  and whether the change needed a `[DISCUSS]` thread it didn't get.
- CI (tests + coverage + lint) must be green before merge.

## Testing requirements

- New reader/writer/transform: unit test under the matching `tests/unit/...`
  subfolder, following the existing pattern (see `tests/README.md`).
- Change to `IngestionPipeline` behavior: integration test under
  `tests/integration/`.
- Change to scheduling (`dags/dag_factory.py`, `autosys/jil_generator.py`):
  unit test asserting the generated DAG/JIL structure.
- Run before every PR:
  ```bash
  pytest --cov=src --cov-report=term --cov-report=xml
  ```
  Don't drop total coverage below the current baseline (see coverage output
  in CI or `coverage.xml`).

## License headers

Every `.py` file under `src/`, `dags/`, `autosys/`, and `tests/` must start
with the standard ASF header:

```python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the
# License.
```

New files copy this verbatim. CI should eventually enforce this via
Apache RAT (Release Audit Tool) once the project has a release process.

## Release process (target state, once incubating)

1. Cut a release branch, run the full test suite + RAT license check.
2. Build source and binary distributions, sign with a committer's PGP key,
   upload to the ASF distribution area.
3. Call a `[VOTE]` thread on the dev list (72-hour minimum, 3 +1 PMC votes
   required).
4. On passing vote, promote artifacts to the official ASF release area and
   announce on `announce@apache.org`.

This repo doesn't yet have ASF release infrastructure — see
`docs/APACHE_ONBOARDING.md` for what's needed to get there and
`docs/INCUBATOR_PROPOSAL.md` for the donation plan.

## Coordinating across the three "front doors"

This project has one engine (`src/ingestion/`) with two schedulers
(`dags/`, `autosys/`). Any change to `config/models.py` (the schema both
schedulers read) must keep both in sync — update `dags/dag_factory.py` and
`autosys/jil_generator.py` together, and update `tests/unit/test_dag_factory.py`
and `tests/unit/test_jil_generator.py` in the same PR.
