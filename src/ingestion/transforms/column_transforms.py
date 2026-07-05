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
