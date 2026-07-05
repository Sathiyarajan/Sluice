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
