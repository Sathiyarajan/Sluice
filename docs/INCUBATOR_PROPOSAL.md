# Sluice Incubator Proposal (Draft)

Status: **DRAFT — not yet submitted.** Needs a Champion and Mentors before
posting to `general@incubator.apache.org`. See `docs/APACHE_ONBOARDING.md`
step 7 for the submission process.

## Abstract

Sluice is a config-driven, metadata-first ETL/ingestion framework built on
Apache Spark. A single YAML file per dataset describes source, target(s),
transforms, data-quality rules, retry/SLA policy, and scheduling — and
Sluice generates both the Spark ingestion job and its orchestration
(Apache Airflow DAGs, or Autosys JIL for teams on that scheduler) from it.

## Proposal

Donate Sluice to the ASF Incubator as a new podling, to be developed under
ASF governance with an open, meritocratic community, reusing the existing
Apache ecosystem it already integrates with (Spark, Airflow, Delta Lake).

## Background

Most organizations doing batch ETL on Spark hand-write a bespoke ingestion
job per dataset/source/target combination, then hand-write a matching
Airflow DAG (and, in enterprises still on legacy schedulers, a separate
Autosys job graph) — two artifacts that drift out of sync over time. Sluice
collapses this into one declarative config per dataset, with pluggable
reader/writer/transform strategies (factory + registry pattern) so adding a
new source or destination system doesn't require touching the orchestration
layer at all.

## Rationale

ETL/ingestion tooling is a recurring, cross-industry need; existing OSS in
this space (e.g. Airbyte, dlt) is either not Spark-native or not
config/schema driven at the level Sluice targets (Pydantic-validated
dataset configs with DQ rules and SLA/retry policy as first-class fields,
not bolted on). Bringing this under the ASF:

- Gives users license clarity and IP assurance (Apache License 2.0 today;
  ASF governance guarantees this doesn't change under a single vendor's
  control).
- Attracts contributors already active in the Spark/Airflow/Delta
  ecosystems the project depends on.
- Provides a neutral home so the project isn't tied to one company's
  roadmap — critical for a data-infrastructure dependency enterprises will
  build ETL pipelines on top of.

## Initial Goals

1. Stabilize the current reader/writer/transform interfaces
   (`BaseReader`, `BaseWriter`, `Transform`) as the extension API.
2. Expand source/target coverage (current: JDBC, S3, Hive, Kafka, Delta ->
   Databricks Delta, Snowflake, Hive, S3) based on community demand.
3. Formalize the config schema (`DatasetConfig` and friends) as a
   versioned, documented public contract.
4. Establish CI, RAT license-header checks, and a first ASF-compliant
   release.
5. Grow a community beyond the initial contributor set — the standard
   Incubator graduation bar.

## Current Status

- Working PySpark-based ingestion engine with pluggable readers
  (`src/ingestion/readers/`) and writers (`src/ingestion/writers/`).
- Config-driven Airflow DAG generation (`dags/dag_factory.py`) and Autosys
  JIL generation (`autosys/jil_generator.py`) from the same YAML configs.
- Data quality checks, dead-letter handling, checkpointed incremental
  loads, retry/backoff, structured logging and metrics
  (`src/ingestion/utils/`).
- Test suite: unit, integration (real local Delta + moto-mocked S3), and
  synthetic end-to-end (`tests/`) — see `tests/README.md`.
- Local Docker Compose dev stack (MinIO + Hive metastore) for running the
  full pipeline without cloud credentials.
- No ASF infrastructure yet (see `docs/APACHE_ONBOARDING.md`).

### Meritocracy

Initial committers will grant commit access to new contributors based on
sustained, quality contributions (patches, reviews, documentation),
following the standard ASF meritocracy model — not proposed here as a fixed
list, since it should grow via the same process every ASF project uses.

### Community

Pre-incubation, this is a single-organization codebase. A primary goal of
incubation is to grow an independent, multi-organization contributor base —
the standard Incubator graduation criterion of "diversity of contributors
and decision-makers."

### Core Developers

- Sathiyarajan (shathi.ece@gmail.com) — original author, proposed initial
  PMC chair

Sole contributor at proposal time. Growing this list is the top priority of
the incubation period (see Known Risks — Homogenous developers, below).

### Alignment

Sluice depends on and extends: Apache Spark, Apache Airflow, Delta Lake
(Linux Foundation, Spark-adjacent). Natural fit alongside other
ASF data-engineering podlings/projects (e.g. Apache Airflow itself, Apache
Iceberg, Apache Hop) — potential for cross-project collaboration on
connector interfaces.

### Known Risks

- **Orphaned project risk**: mitigated by seeking Mentors from existing
  ASF data-engineering PMCs and by this proposal explicitly prioritizing
  community growth over feature velocity during incubation.
- **Inexperience with Open Source**: initial contributors are new to ASF
  process; Mentors' guidance is relied on heavily for the first 6–12
  months.
- **Homogenous developers**: current contributor base is a single
  organization — flagged above as the top incubation-period goal to fix.
- **Reliance on Salaried Developers**: acknowledged; incubation should aim
  to attract volunteer contributors independent of any one employer.
- **Relationships with other Apache products**: complementary, not
  competitive, with Airflow/Spark/Iceberg — no conflict anticipated.
- **An excessive fascination with the infrastructure**: none anticipated;
  scope is the ingestion engine and its two scheduler integrations, not
  infrastructure tooling itself.

### Documentation

See `README.md`, `src/ingestion/README.md`, `dags/README.md`,
`autosys/README.md`, `tests/README.md` in this repository.

## Required Resources

- Mailing lists: `dev@sluice.apache.org`, `private@sluice.apache.org` (PPMC).
- Git repository: `sluice.git` under ASF Git hosting (or GitHub mirror per
  current ASF policy).
- Issue tracking: ASF JIRA project `SLUICE` (see
  `docs/APACHE_ONBOARDING.md` step 4).
- Continuous Integration: ASF-provided GitHub Actions / Jenkins per current
  INFRA offerings.

## Initial Committers

- Sathiyarajan (shathi.ece@gmail.com) — ASF id pending, ICLA to be filed
  per `docs/APACHE_ONBOARDING.md` steps 2–3.

## Sponsors

- **Champion**: _TBD — must be an existing ASF Member or officer._
- **Mentors**: _TBD — minimum 3, at least one experienced with Incubator
  process._
- **Sponsoring Entity**: Apache Incubator PMC.
