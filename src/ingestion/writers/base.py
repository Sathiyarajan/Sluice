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

"""BaseWriter strategy interface. All writers accept a SparkSession + TargetConfig
and write a DataFrame, honoring write_mode (append/overwrite/merge/upsert)."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from ingestion.config.models import TargetConfig, WriteMode


class UnsupportedWriteModeError(ValueError):
    pass


class BaseWriter(ABC):
    def __init__(self, spark: SparkSession, target_config: TargetConfig) -> None:
        self.spark = spark
        self.target_config = target_config

    def write(self, df: DataFrame) -> None:
        mode = self.target_config.write_mode
        if mode == WriteMode.APPEND:
            self.append(df)
        elif mode == WriteMode.OVERWRITE:
            self.overwrite(df)
        elif mode in (WriteMode.MERGE, WriteMode.UPSERT):
            self.merge(df)
        else:
            raise UnsupportedWriteModeError(f"Unsupported write mode: {mode}")

    @abstractmethod
    def append(self, df: DataFrame) -> None: ...

    @abstractmethod
    def overwrite(self, df: DataFrame) -> None: ...

    @abstractmethod
    def merge(self, df: DataFrame) -> None: ...
