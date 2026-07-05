# On-Call Runbook

## Alerting paths

Each dataset config declares `sla.alert_channels` (e.g. `slack:#data-eng-alerts`,
`email:oncall@example.com`). Two orchestrators fan out from the same list:

- **Airflow**: `on_failure_callback` in `dags/dag_factory.py` logs a structured
  `task_failed` event; wire this to `SlackWebhookOperator`/`EmailOperator` in
  the deployment's Airflow connections.
- **Autosys**: JIL jobs set `alarm_if_fail: 1`; the `-- alert_channels:` comment
  in generated `.jil` files documents which channels the paging integration
  should notify (Autosys itself pages via its own event server config).

## Pipeline failed — where to look first

1. **Structured logs** — every pipeline run emits JSON logs
   (`ingestion.pipeline`, `ingestion.retry`, `ingestion.dead_letter`) with a
   `context` object. Grep by `dataset` name.
2. **Data quality failure** (`DataQualityFailure` exception) — the log context
   lists every violated check (`min_row_count`, `max_null_fraction`,
   `schema_validation`). Rows failing DQ are (optionally) routed to the
   dead-letter path configured via `DeadLetterHandler`.
3. **Write failure after retries exhausted** — `ingestion.retry` logs each
   attempt with the underlying exception. Check target-system availability
   (Delta table lock contention, Snowflake connector auth, Hive metastore).
4. **Watermark stuck / no new rows ingested** — inspect the checkpoint file at
   `.checkpoints/<dataset_name>.json` (or the configured `CheckpointStore`
   backend). A stuck watermark usually means the upstream source stopped
   producing rows past that value, not a pipeline bug.

## Common remediations

| Symptom | Likely cause | Action |
|---|---|---|
| SLA miss, task still running | Large backlog after an outage | Let it finish once; if it recurs, consider chunking the source read by watermark range |
| `DataQualityFailure: max_null_fraction` | Upstream schema/data change | Confirm with source owner; raise `max_null_fraction` only with data-eng sign-off |
| Merge writer raises `UNSUPPORTED_OVERWRITE` | Hive/S3 merge implementation regression | These writers stage to a temp table/path and swap — verify staging cleanup didn't fail mid-run |
| Autosys job stuck in `RUNNING` after process died | Orphaned JVM | Kill via Autosys `sendevent -e FORCE_STARTJOB`/manual kill, then rerun the box job |

## Re-running a dataset manually

```bash
python -m ingestion.run_pipeline \
  --dataset customer_orders \
  --config-path config/datasets/customer_orders.yaml
```

Pass `--target-type <type>` to re-run a single target if only one of several
targets failed.
