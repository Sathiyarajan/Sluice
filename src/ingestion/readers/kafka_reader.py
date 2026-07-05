from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from ingestion.readers.base import BaseReader


class KafkaReader(BaseReader):
    """Batch read of a Kafka topic between offsets (streaming variant can be layered on
    top by calling readStream instead of read in a future extension)."""

    def read(self) -> DataFrame:
        options = dict(self.source_config.options)
        value_schema_json = options.pop("value_schema", None)
        reader = self.spark.read.format("kafka").options(**options)
        raw_df = reader.load()
        if not value_schema_json:
            return raw_df
        schema = StructType.fromJson(value_schema_json)
        return raw_df.select(
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        ).select("data.*")
