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

"""Snowflake writer using the Spark Snowflake connector: stage to a Snowflake table
via the connector's write path (append/overwrite), and MERGE via a staging table +
raw SQL MERGE statement (executed through the connector's utils.runQuery)."""
from __future__ import annotations

from pyspark.sql import DataFrame

from ingestion.writers.base import BaseWriter
from ingestion.utils.secrets import resolve_secret

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"


class SnowflakeWriter(BaseWriter):
    def _connection_options(self) -> dict[str, str]:
        options = dict(self.target_config.options)
        if "sfPassword_secret" in options:
            options["sfPassword"] = resolve_secret(options.pop("sfPassword_secret"))
        options.pop("table", None)
        options.pop("staging_table", None)
        options.pop("schema_evolution", None)
        return options

    def _table(self) -> str:
        table = self.target_config.options.get("table")
        if not table:
            raise ValueError("Snowflake target requires 'table' option")
        return table

    def append(self, df: DataFrame) -> None:
        (
            df.write.format(SNOWFLAKE_SOURCE)
            .options(**self._connection_options())
            .option("dbtable", self._table())
            .mode("append")
            .save()
        )

    def overwrite(self, df: DataFrame) -> None:
        (
            df.write.format(SNOWFLAKE_SOURCE)
            .options(**self._connection_options())
            .option("dbtable", self._table())
            .mode("overwrite")
            .save()
        )

    def merge(self, df: DataFrame) -> None:
        staging_table = self.target_config.options.get(
            "staging_table", f"{self._table()}_STAGE"
        )
        connection_options = self._connection_options()
        (
            df.write.format(SNOWFLAKE_SOURCE)
            .options(**connection_options)
            .option("dbtable", staging_table)
            .mode("overwrite")
            .save()
        )

        merge_keys = self.target_config.merge_keys
        columns = df.columns
        on_clause = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
        update_clause = ", ".join(f"{c} = source.{c}" for c in columns if c not in merge_keys)
        insert_columns = ", ".join(columns)
        insert_values = ", ".join(f"source.{c}" for c in columns)
        merge_sql = f"""
            MERGE INTO {self._table()} AS target
            USING {staging_table} AS source
            ON {on_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """.strip()

        from ingestion.writers._snowflake_utils import run_snowflake_query

        run_snowflake_query(self.spark, connection_options, merge_sql)
