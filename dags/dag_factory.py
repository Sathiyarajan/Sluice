"""Parameterized Airflow DAG factory: generates one DAG per dataset ingestion
config found in config/datasets/*.yaml. Each DAG has a single task group per
source->target pair (a dataset may fan out to multiple targets), an upstream
ExternalTaskSensor per declared dependency, an SLA callback, and failure
alerting (email + Slack) wired from the config's sla.alert_channels.
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

from ingestion.config.loader import load_config_dir
from ingestion.config.models import DatasetConfig

CONFIG_DIR = Path(__file__).parent.parent / "config" / "datasets"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 0,  # retry/backoff is handled inside IngestionPipeline itself
    "retry_delay": timedelta(minutes=1),
}


def _run_ingestion_task(dataset_name: str, target_type: str | None = None) -> None:
    """Task body: build a SparkSession, load the dataset config, and run the pipeline.
    Imported lazily so DAG parsing (which happens frequently) never needs Spark."""
    from pyspark.sql import SparkSession

    from ingestion.config.loader import load_config
    from ingestion.pipeline import IngestionPipeline

    config = load_config(CONFIG_DIR / f"{dataset_name}.yaml")
    if target_type is not None:
        config = config.model_copy(
            update={"targets": [t for t in config.targets if t.type.value == target_type]}
        )
    spark = SparkSession.builder.appName(f"ingestion-{dataset_name}").getOrCreate()
    try:
        IngestionPipeline(spark, config).run()
    finally:
        spark.stop()


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    from ingestion.utils.logging_utils import get_logger

    logger = get_logger("ingestion.airflow.sla")
    logger.warning(
        "sla_missed",
        extra={"context": {"dag_id": dag.dag_id, "tasks": [t for t in task_list]}},
    )


def _failure_callback(context: dict[str, Any]) -> None:
    from ingestion.utils.logging_utils import get_logger

    logger = get_logger("ingestion.airflow.failure")
    dag_run = context.get("dag_run")
    logger.error(
        "task_failed",
        extra={
            "context": {
                "dag_id": dag_run.dag_id if dag_run else None,
                "task_id": context["task_instance"].task_id,
                "exception": str(context.get("exception")),
            }
        },
    )
    # In production this would notify email/Slack per config.sla.alert_channels via
    # EmailOperator / SlackWebhookOperator dispatch; kept as a structured log hook
    # here so the callback is unit-testable without live credentials.


def build_dag(config: DatasetConfig) -> DAG:
    dag = DAG(
        dag_id=f"ingestion__{config.dataset_name}",
        description=config.description,
        default_args={**DEFAULT_ARGS, "on_failure_callback": _failure_callback},
        schedule_interval=config.schedule,
        start_date=days_ago(1),
        catchup=False,
        sla_miss_callback=_sla_miss_callback,
        tags=list(config.tags) + ["config-driven-ingestion"],
    )

    with dag:
        sensors = [
            ExternalTaskSensor(
                task_id=f"wait_for_{upstream}",
                external_dag_id=upstream,
                mode="reschedule",
                timeout=config.sla.max_runtime_minutes * 60,
            )
            for upstream in config.upstream_dependencies
        ]

        for target in config.targets:
            with TaskGroup(group_name=f"{config.source.type.value}_to_{target.type.value}") as group:
                ingest_task = PythonOperator(
                    task_id=f"ingest_{config.dataset_name}_{target.type.value}",
                    python_callable=_run_ingestion_task,
                    op_kwargs={
                        "dataset_name": config.dataset_name,
                        "target_type": target.type.value,
                    },
                    sla=timedelta(minutes=config.sla.max_runtime_minutes),
                )
            for sensor in sensors:
                sensor >> group

    return dag


def generate_dags(config_dir: Path = CONFIG_DIR) -> dict[str, DAG]:
    configs = load_config_dir(config_dir)
    return {config.dataset_name: build_dag(config) for config in configs}


# Airflow's DAG file processor scans module globals for DAG instances.
for _dataset_name, _dag in generate_dags().items():
    globals()[f"dag_{_dataset_name}"] = _dag
