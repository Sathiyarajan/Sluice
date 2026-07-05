# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific
# language governing permissions and limitations under the
# License.

"""Hive writer for managed/external tables with partitioned writes and MSCK repair."""
from __future__ import annotations

from pyspark.sql import DataFrame

from ingestion.writers.base import BaseWriter


class HiveWriter(BaseWriter):
    def _table(self) -> str:
        table = self.target_config.options.get("table")
        if not table:
            raise ValueError("Hive target requires 'table' option")
        return table

    def _writer(self, df: DataFrame, mode: str):
        writer = df.write.format(self.target_config.options.get("format", "parquet")).mode(mode)
        if self.target_config.partition_columns:
            writer = writer.partitionBy(*self.target_config.partition_columns)
        return writer

    def append(self, df: DataFrame) -> None:
        self._writer(df, "append").saveAsTable(self._table())
        self._repair()

    def overwrite(self, df: DataFrame) -> None:
        self._writer(df, "overwrite").saveAsTable(self._table())
        self._repair()

    def merge(self, df: DataFrame) -> None:
        """Hive has no native MERGE via the DataFrame writer; emulate upsert by
        anti-joining existing rows on merge_keys then swapping in the combined result
        through a staging table (Spark refuses to overwrite a table it is reading
        from in the same query, so the combined result must land elsewhere first)."""
        table = self._table()
        merge_keys = self.target_config.merge_keys
        if not self.spark.catalog.tableExists(table):
            self.overwrite(df)
            return
        existing = self.spark.table(table)
        condition = [existing[k] == df[k] for k in merge_keys]
        from functools import reduce
        import operator

        join_condition = reduce(operator.and_, condition)
        unmatched_existing = existing.join(df, join_condition, "left_anti")
        combined = unmatched_existing.unionByName(df, allowMissingColumns=True)

        staging_table = f"{table}_merge_staging"
        self._writer(combined, "overwrite").saveAsTable(staging_table)
        self.spark.sql(f"DROP TABLE IF EXISTS {table}")
        self.spark.sql(f"ALTER TABLE {staging_table} RENAME TO {table}")
        self._repair()

    def _repair(self) -> None:
        if self.target_config.partition_columns:
            self.spark.sql(f"MSCK REPAIR TABLE {self._table()}")
