from pathlib import Path

from ingestion.config.loader import load_config, load_config_dir

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_load_yaml_config():
    config = load_config(FIXTURES / "sample_dataset.yaml")
    assert config.dataset_name == "customer_orders"
    assert config.source.type.value == "s3_parquet"
    assert config.targets[0].write_mode.value == "merge"


def test_load_config_dir():
    configs = load_config_dir(FIXTURES)
    names = {c.dataset_name for c in configs}
    assert "customer_orders" in names
