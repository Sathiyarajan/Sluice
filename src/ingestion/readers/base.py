"""BaseReader strategy interface — all readers accept a SparkSession + SourceConfig
and return a DataFrame."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from ingestion.config.models import SourceConfig


class BaseReader(ABC):
    def __init__(self, spark: SparkSession, source_config: SourceConfig) -> None:
        self.spark = spark
        self.source_config = source_config

    @abstractmethod
    def read(self) -> DataFrame: ...

    def read_incremental(self, watermark: str | None) -> DataFrame:
        """Default incremental behaviour: filter on watermark_column if configured."""
        df = self.read()
        column = self.source_config.watermark_column
        if column and watermark is not None:
            df = df.filter(df[column] > watermark)
        return df
