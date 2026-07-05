import sys
from pathlib import Path

AUTOSYS_DIR = Path(__file__).parent.parent.parent / "autosys"
sys.path.insert(0, str(AUTOSYS_DIR))

from jil_generator import generate_jil_files, render_dataset_jil  # noqa: E402
from ingestion.config.loader import load_config  # noqa: E402

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "datasets" / "customer_orders.yaml"


def test_render_dataset_jil_contains_box_and_command_jobs():
    config = load_config(CONFIG_PATH)
    jil = render_dataset_jil(config)
    assert "insert_job: BOX_CUSTOMER_ORDERS   job_type: BOX" in jil
    assert "insert_job: CMD_CUSTOMER_ORDERS_TO_DATABRICKS_DELTA   job_type: CMD" in jil
    assert "box_name: BOX_CUSTOMER_ORDERS" in jil
    assert "n_retrys: 2" in jil


def test_render_dataset_jil_includes_upstream_condition():
    config = load_config(CONFIG_PATH)
    jil = render_dataset_jil(config)
    assert "condition: success(BOX_RAW_CUSTOMER_ORDERS_LANDED)" in jil


def test_generate_jil_files_writes_to_output_dir(tmp_path):
    written = generate_jil_files(
        config_dir=CONFIG_PATH.parent, output_dir=tmp_path / "generated"
    )
    assert "customer_orders" in written
    output_file = Path(written["customer_orders"])
    assert output_file.exists()
    assert "BOX_CUSTOMER_ORDERS" in output_file.read_text(encoding="utf-8")
