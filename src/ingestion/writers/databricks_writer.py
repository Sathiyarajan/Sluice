"""Databricks Delta Lake writer. Uses the DeltaTable API for MERGE/upsert and
supports schema evolution via mergeSchema."""
from __future__ import annotations

from pyspark.sql import DataFrame

from ingestion.writers.base import BaseWriter


class DatabricksWriter(BaseWriter):
    def _table_path_or_name(self) -> tuple[str | None, str | None]:
        options = self.target_config.options
        return options.get("path"), options.get("table")

    def _writer(self, df: DataFrame):
        writer = df.write.format("delta")
        if self.target_config.options.get("schema_evolution", True):
            writer = writer.option("mergeSchema", "true")
        if self.target_config.partition_columns:
            writer = writer.partitionBy(*self.target_config.partition_columns)
        return writer

    def append(self, df: DataFrame) -> None:
        path, table = self._table_path_or_name()
        writer = self._writer(df).mode("append")
        writer.saveAsTable(table) if table else writer.save(path)

    def overwrite(self, df: DataFrame) -> None:
        path, table = self._table_path_or_name()
        writer = self._writer(df).mode("overwrite").option("overwriteSchema", "true")
        writer.saveAsTable(table) if table else writer.save(path)

    def merge(self, df: DataFrame) -> None:
        from delta.tables import DeltaTable

        path, table = self._table_path_or_name()
        merge_keys = self.target_config.merge_keys
        exists = (
            DeltaTable.isDeltaTable(self.spark, path)
            if path
            else self.spark.catalog.tableExists(table)
        )
        if not exists:
            self.overwrite(df)
            return

        delta_table = (
            DeltaTable.forPath(self.spark, path) if path else DeltaTable.forName(self.spark, table)
        )
        condition = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
