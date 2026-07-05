import pytest
from pydantic import ValidationError

from ingestion.config.models import (
    DatasetConfig,
    SourceConfig,
    SourceType,
    TargetConfig,
    TargetType,
    WriteMode,
)


def _base_source():
    return SourceConfig(type=SourceType.S3_PARQUET, options={"path": "s3://bucket/path"})


def test_target_config_merge_requires_keys():
    with pytest.raises(ValidationError):
        TargetConfig(type=TargetType.HIVE, write_mode=WriteMode.MERGE, merge_keys=[])


def test_target_config_merge_with_keys_ok():
    target = TargetConfig(
        type=TargetType.HIVE, write_mode=WriteMode.MERGE, merge_keys=["id"]
    )
    assert target.write_mode == WriteMode.MERGE


def test_dataset_config_rejects_invalid_name():
    with pytest.raises(ValidationError):
        DatasetConfig(
            dataset_name="bad name!",
            source=_base_source(),
            targets=[TargetConfig(type=TargetType.S3)],
        )


def test_dataset_config_defaults():
    config = DatasetConfig(
        dataset_name="customer_orders",
        source=_base_source(),
        targets=[TargetConfig(type=TargetType.S3)],
    )
    assert config.retry.max_attempts == 3
    assert config.sla.max_runtime_minutes == 60
    assert config.data_quality.enabled is True
