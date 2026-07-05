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

"""BaseReader strategy interface — all readers accept a SparkSession + SourceConfig
and return a DataFrame."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from ingestion.config.models import SourceConfig


class BaseReader(ABC):
    def __init__(self, spark: SparkSession, source_config: SourceConfig) -> None:
        self.spark = spark
        self.source_config = source_config

    @abstractmethod
    def read(self) -> DataFrame: ...

    def read_incremental(self, watermark: str | None) -> DataFrame:
        """Default incremental behaviour: filter on watermark_column if configured."""
        df = self.read()
        column = self.source_config.watermark_column
        if column and watermark is not None:
            df = df.filter(df[column] > watermark)
        return df
