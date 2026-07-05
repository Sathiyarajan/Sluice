from __future__ import annotations

from pyspark.sql import DataFrame

from ingestion.readers.base import BaseReader


class DeltaReader(BaseReader):
    """Reads a Delta Lake table (Databricks or OSS Delta) by path or metastore name."""

    def read(self) -> DataFrame:
        options = dict(self.source_config.options)
        table = options.pop("table", None)
        path = options.pop("path", None)
        reader = self.spark.read.format("delta").options(**options)
        if table:
            return self.spark.read.format("delta").options(**options).table(table)
        if path:
            return reader.load(path)
        raise ValueError("Delta source requires 'table' or 'path' option")
