from __future__ import annotations

from pyspark.sql import SparkSession

from ingestion.config.models import TargetConfig, TargetType
from ingestion.writers.base import BaseWriter
from ingestion.writers.databricks_writer import DatabricksWriter
from ingestion.writers.hive_writer import HiveWriter
from ingestion.writers.s3_writer import S3Writer
from ingestion.writers.snowflake_writer import SnowflakeWriter

_WRITER_REGISTRY: dict[TargetType, type[BaseWriter]] = {
    TargetType.DATABRICKS_DELTA: DatabricksWriter,
    TargetType.SNOWFLAKE: SnowflakeWriter,
    TargetType.HIVE: HiveWriter,
    TargetType.S3: S3Writer,
}


class WriterFactory:
    @staticmethod
    def create(spark: SparkSession, target_config: TargetConfig) -> BaseWriter:
        writer_cls = _WRITER_REGISTRY.get(target_config.type)
        if writer_cls is None:
            raise ValueError(f"No writer registered for target type {target_config.type!r}")
        return writer_cls(spark, target_config)

    @staticmethod
    def register(target_type: TargetType, writer_cls: type[BaseWriter]) -> None:
        _WRITER_REGISTRY[target_type] = writer_cls
