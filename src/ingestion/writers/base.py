"""BaseWriter strategy interface. All writers accept a SparkSession + TargetConfig
and write a DataFrame, honoring write_mode (append/overwrite/merge/upsert)."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from ingestion.config.models import TargetConfig, WriteMode


class UnsupportedWriteModeError(ValueError):
    pass


class BaseWriter(ABC):
    def __init__(self, spark: SparkSession, target_config: TargetConfig) -> None:
        self.spark = spark
        self.target_config = target_config

    def write(self, df: DataFrame) -> None:
        mode = self.target_config.write_mode
        if mode == WriteMode.APPEND:
            self.append(df)
        elif mode == WriteMode.OVERWRITE:
            self.overwrite(df)
        elif mode in (WriteMode.MERGE, WriteMode.UPSERT):
            self.merge(df)
        else:
            raise UnsupportedWriteModeError(f"Unsupported write mode: {mode}")

    @abstractmethod
    def append(self, df: DataFrame) -> None: ...

    @abstractmethod
    def overwrite(self, df: DataFrame) -> None: ...

    @abstractmethod
    def merge(self, df: DataFrame) -> None: ...
