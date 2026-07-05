# Tests (`tests/`)

## What this folder does, in plain English

This folder proves the ingestion engine (`src/ingestion/`) and its two
schedulers (`dags/`, `autosys/`) actually work, at three levels of
confidence:

- **unit/** — smallest possible slice. One class or function tested in
  isolation, with a real local Spark session but no real cloud services.
- **integration/** — a few pieces wired together (e.g. the full pipeline
  against a real Delta table, or a mocked S3 bucket via `moto`).
- **e2e/** — "end to end": the whole pipeline run start to finish on
  synthetic data, closest to what happens in production.

If you're new to the codebase, read tests bottom-up: unit tests tell you what
one function is supposed to do; e2e tests tell you what the whole system is
supposed to do.

## Shared setup: `conftest.py`

Runs before any test. Two jobs:

1. **Environment bootstrap** (Windows-specific): sets `JAVA_HOME`,
   `PYSPARK_PYTHON`, `HADOOP_HOME`, and a local, file-based Airflow home
   (`.airflow_home/`) with SQLite metadata DB — so Airflow's DAG-parsing
   tests don't need a real Airflow deployment.
2. **`spark` fixture** (session-scoped, i.e. built once and reused by every
   test that asks for it): a local Spark session (`local[2]`) with Delta Lake
   support enabled, UI disabled, and shuffle partitions turned down to 2 for
   speed.

## `tests/unit/` — one thing at a time

| File | What it verifies |
|---|---|
| `test_checkpoint.py` | `FileCheckpointStore` round-trips a watermark to disk (`test_checkpoint_roundtrip`) and correctly overwrites an existing watermark (`test_checkpoint_overwrite`). |
| `test_config_loader.py` | `load_config` parses a YAML file into a `DatasetConfig` (`test_load_yaml_config`); `load_config_dir` picks up every config file in a directory (`test_load_config_dir`). |
| `test_config_models.py` | Pydantic validation rules: `merge`/`upsert` write mode requires `merge_keys` and rejects a config without them (`test_target_config_merge_requires_keys`) but accepts one with them (`test_target_config_merge_with_keys_ok`); invalid `dataset_name` is rejected (`test_dataset_config_rejects_invalid_name`); default values populate correctly (`test_dataset_config_defaults`). |
| `test_dag_factory.py` | `build_dag` produces a correct Airflow `DAG` for a sample dataset config (`test_build_dag_for_customer_orders`); `generate_dags` discovers every config file (`test_generate_dags_discovers_all_configs`). |
| `test_jil_generator.py` | Generated JIL text contains both a BOX job and its CMD job(s) (`test_render_dataset_jil_contains_box_and_command_jobs`); upstream dependencies produce a `condition` line (`test_render_dataset_jil_includes_upstream_condition`); files actually get written to disk (`test_generate_jil_files_writes_to_output_dir`). |
| `test_quality.py` | `run_quality_checks` passes clean data (`test_quality_pass`), catches too-few rows (`test_quality_min_row_count_violation`), catches too many nulls in a column (`test_quality_null_fraction_violation`), catches a missing expected column (`test_quality_schema_violation`), and is a no-op when disabled (`test_quality_disabled`). |
| `test_retry.py` | `with_retry` retries transient failures and eventually succeeds (`test_retry_succeeds_after_transient_failures`); re-raises the original exception once attempts are exhausted (`test_retry_raises_after_exhausting_attempts`). |
| `test_secrets.py` | `resolve_secret` reads an `env:VAR` reference (`test_resolve_env_secret`), raises a clear error if the env var is missing (`test_resolve_env_secret_missing`), raises if a `vault:` reference is used with no Vault client configured (`test_resolve_vault_without_client`), and rejects unknown reference formats (`test_unsupported_reference_format`). |
| `unit/readers/test_file_reader.py` | `FileReader` reads Parquet (`test_read_parquet`) and CSV (`test_read_csv`); `read_incremental` correctly filters rows above a watermark (`test_read_incremental_with_watermark`). |
| `unit/readers/test_hive_reader.py` | `HiveReader` reads a Hive table (`test_read_hive_table`). |
| `unit/readers/test_reader_factory.py` | `ReaderFactory` dispatches JDBC configs to `JdbcReader` (`test_factory_dispatches_jdbc`); custom reader types can be registered at runtime (`test_factory_register_custom_reader`). |
| `unit/transforms/test_column_transforms.py` | Each transform in isolation: rename, cast, add ingestion metadata columns, drop columns, deduplicate without an order column, deduplicate keeping the most recent row per key. |
| `unit/writers/test_databricks_writer.py` | `DatabricksWriter` appends then merges correctly (`test_databricks_writer_append_and_merge`); overwrite mode replaces data and evolves schema (`test_databricks_writer_overwrite`). |
| `unit/writers/test_hive_writer.py` | `HiveWriter` overwrites a table then performs a merge/upsert via the anti-join-and-swap strategy (`test_hive_writer_overwrite_then_merge`). |
| `unit/writers/test_s3_writer.py` | `S3Writer` append (`test_s3_writer_append`), overwrite (`test_s3_writer_overwrite`), merge/upsert (`test_s3_writer_merge_upsert`), and partitioned writes (`test_s3_writer_partitioned`). |
| `unit/writers/test_writer_factory.py` | `WriterFactory` dispatches Snowflake configs correctly (`test_factory_dispatches_snowflake`); an unsupported write mode raises a clear error (`test_unsupported_write_mode_raises`). |

## `tests/integration/` — a few pieces together

| File | What it verifies |
|---|---|
| `test_pipeline_delta.py` | Runs the *full* `IngestionPipeline` against a real local Delta table: first an initial load, then a second incremental run that only picks up new rows and merges them (`test_pipeline_initial_load_then_incremental_merge`); also verifies the pipeline raises `DataQualityFailure` when quality checks fail (`test_pipeline_raises_on_dq_violation`). |
| `test_s3_moto.py` | Uses `moto` (an S3 mocking library — no real AWS account or network calls needed) to verify a bucket can be created and used (`test_moto_s3_bucket_created`, `test_moto_s3_put_and_list_objects`), and that the reader/writer factories correctly recognize S3-flavored source/target types end to end (`test_reader_writer_factories_recognize_s3_types`). |

## `tests/e2e/` — the whole system

| File | What it verifies |
|---|---|
| `test_synthetic_pipeline.py` | `test_synthetic_end_to_end_pipeline` — generates synthetic input data, runs it through the complete pipeline (extract, transform, quality check, write), and asserts on the final output. This is the closest thing in the repo to "does this actually work in production." |

## Running the tests

```bash
pip install -e ".[dev]"
pytest                       # everything
pytest tests/unit            # fast feedback loop
pytest tests/integration      # slower, still no cloud creds needed (moto + local Delta)
pytest tests/e2e               # full pipeline smoke test
```

See the root `README.md` (and `docs/`) for coverage reporting via
`pytest-cov`, which is wired into Sonar for code-coverage tracking.
