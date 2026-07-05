from __future__ import annotations

from pyspark.sql import SparkSession

from ingestion.config.models import SourceConfig, SourceType
from ingestion.readers.base import BaseReader
from ingestion.readers.delta_reader import DeltaReader
from ingestion.readers.file_reader import FileReader
from ingestion.readers.hive_reader import HiveReader
from ingestion.readers.jdbc_reader import JdbcReader
from ingestion.readers.kafka_reader import KafkaReader

_READER_REGISTRY: dict[SourceType, type[BaseReader]] = {
    SourceType.JDBC: JdbcReader,
    SourceType.S3_PARQUET: FileReader,
    SourceType.S3_CSV: FileReader,
    SourceType.S3_JSON: FileReader,
    SourceType.HIVE: HiveReader,
    SourceType.KAFKA: KafkaReader,
    SourceType.DELTA: DeltaReader,
}


class ReaderFactory:
    @staticmethod
    def create(spark: SparkSession, source_config: SourceConfig) -> BaseReader:
        reader_cls = _READER_REGISTRY.get(source_config.type)
        if reader_cls is None:
            raise ValueError(f"No reader registered for source type {source_config.type!r}")
        return reader_cls(spark, source_config)

    @staticmethod
    def register(source_type: SourceType, reader_cls: type[BaseReader]) -> None:
        _READER_REGISTRY[source_type] = reader_cls
