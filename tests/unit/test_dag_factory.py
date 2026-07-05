import sys
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).parent.parent.parent / "dags"
sys.path.insert(0, str(DAGS_DIR))

try:
    from dag_factory import build_dag, generate_dags  # noqa: E402
except ModuleNotFoundError as exc:
    # Airflow's core operators import fcntl, a POSIX-only stdlib module. Airflow is
    # officially unsupported on native Windows (only WSL2/Linux); this framework's
    # DAG factory targets a Linux Airflow deployment, so skip here rather than fail.
    pytest.skip(f"Airflow unavailable on this platform: {exc}", allow_module_level=True)

from ingestion.config.loader import load_config  # noqa: E402

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "datasets" / "customer_orders.yaml"


def test_build_dag_for_customer_orders():
    config = load_config(CONFIG_PATH)
    dag = build_dag(config)
    assert dag.dag_id == "ingestion__customer_orders"
    task_ids = [t.task_id for t in dag.tasks]
    assert any("ingest_customer_orders_databricks_delta" in t for t in task_ids)


def test_generate_dags_discovers_all_configs():
    dags = generate_dags()
    assert "customer_orders" in dags
    assert dags["customer_orders"].schedule_interval == "0 6 * * *"
