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

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.transforms.base import Transform


class ColumnRename(Transform):
    def __init__(self, mapping: dict[str, str]) -> None:
        self._mapping = mapping

    def apply(self, df: DataFrame) -> DataFrame:
        for old_name, new_name in self._mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df


class ColumnCast(Transform):
    def __init__(self, casts: dict[str, str]) -> None:
        self._casts = casts

    def apply(self, df: DataFrame) -> DataFrame:
        for column, spark_type in self._casts.items():
            df = df.withColumn(column, F.col(column).cast(spark_type))
        return df


class AddIngestionMetadata(Transform):
    def __init__(self, dataset_name: str) -> None:
        self._dataset_name = dataset_name

    def apply(self, df: DataFrame) -> DataFrame:
        return df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
            "_dataset_name", F.lit(self._dataset_name)
        )


class DropColumns(Transform):
    def __init__(self, columns: list[str]) -> None:
        self._columns = columns

    def apply(self, df: DataFrame) -> DataFrame:
        return df.drop(*self._columns)


class Deduplicate(Transform):
    def __init__(self, keys: list[str], order_by: str | None = None) -> None:
        self._keys = keys
        self._order_by = order_by

    def apply(self, df: DataFrame) -> DataFrame:
        if not self._order_by:
            return df.dropDuplicates(self._keys)
        from pyspark.sql import Window

        window = Window.partitionBy(*self._keys).orderBy(F.col(self._order_by).desc())
        return (
            df.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
