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

from ingestion.config.models import SourceType
from ingestion.readers.base import BaseReader

_FORMAT_BY_SOURCE_TYPE = {
    SourceType.S3_PARQUET: "parquet",
    SourceType.S3_CSV: "csv",
    SourceType.S3_JSON: "json",
}


class FileReader(BaseReader):
    """Reads Parquet/CSV/JSON from S3 (or any Hadoop-compatible filesystem, incl. local
    for tests) using the source path in options['path']."""

    def read(self) -> DataFrame:
        options = dict(self.source_config.options)
        path = options.pop("path", None)
        if not path:
            raise ValueError("File source requires 'path' option")
        fmt = _FORMAT_BY_SOURCE_TYPE[self.source_config.type]
        reader = self.spark.read.format(fmt)
        if fmt == "csv":
            options.setdefault("header", "true")
            options.setdefault("inferSchema", "true")
        reader = reader.options(**options)
        return reader.load(path)
