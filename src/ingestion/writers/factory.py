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

from pyspark.sql import SparkSession

from ingestion.config.models import TargetConfig, TargetType
from ingestion.writers.base import BaseWriter
from ingestion.writers.databricks_writer import DatabricksWriter
from ingestion.writers.hive_writer import HiveWriter
from ingestion.writers.s3_writer import S3Writer
from ingestion.writers.snowflake_writer import SnowflakeWriter

_WRITER_REGISTRY: dict[TargetType, type[BaseWriter]] = {
    TargetType.DATABRICKS_DELTA: DatabricksWriter,
    TargetType.SNOWFLAKE: SnowflakeWriter,
    TargetType.HIVE: HiveWriter,
    TargetType.S3: S3Writer,
}


class WriterFactory:
    @staticmethod
    def create(spark: SparkSession, target_config: TargetConfig) -> BaseWriter:
        writer_cls = _WRITER_REGISTRY.get(target_config.type)
        if writer_cls is None:
            raise ValueError(f"No writer registered for target type {target_config.type!r}")
        return writer_cls(spark, target_config)

    @staticmethod
    def register(target_type: TargetType, writer_cls: type[BaseWriter]) -> None:
        _WRITER_REGISTRY[target_type] = writer_cls
