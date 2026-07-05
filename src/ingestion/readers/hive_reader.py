from __future__ import annotations

from pyspark.sql import DataFrame

from ingestion.readers.base import BaseReader


class HiveReader(BaseReader):
    def read(self) -> DataFrame:
        options = dict(self.source_config.options)
        table = options.get("table")
        if not table:
            raise ValueError("Hive source requires 'table' option (db.table)")
        df = self.spark.table(table)
        partition_filter = options.get("partition_filter")
        if partition_filter:
            df = df.filter(partition_filter)
        return df
