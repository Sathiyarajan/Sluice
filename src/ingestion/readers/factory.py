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

from ingestion.config.models import SourceConfig, SourceType
from ingestion.readers.base import BaseReader
from ingestion.readers.delta_reader import DeltaReader
from ingestion.readers.file_reader import FileReader
from ingestion.readers.hive_reader import HiveReader
from ingestion.readers.jdbc_reader import JdbcReader
from ingestion.readers.kafka_reader import KafkaReader

_READER_REGISTRY: dict[SourceType, type[BaseReader]] = {
    SourceType.JDBC: JdbcReader,
    SourceType.S3_PARQUET: FileReader,
    SourceType.S3_CSV: FileReader,
    SourceType.S3_JSON: FileReader,
    SourceType.HIVE: HiveReader,
    SourceType.KAFKA: KafkaReader,
    SourceType.DELTA: DeltaReader,
}


class ReaderFactory:
    @staticmethod
    def create(spark: SparkSession, source_config: SourceConfig) -> BaseReader:
        reader_cls = _READER_REGISTRY.get(source_config.type)
        if reader_cls is None:
            raise ValueError(f"No reader registered for source type {source_config.type!r}")
        return reader_cls(spark, source_config)

    @staticmethod
    def register(source_type: SourceType, reader_cls: type[BaseReader]) -> None:
        _READER_REGISTRY[source_type] = reader_cls
