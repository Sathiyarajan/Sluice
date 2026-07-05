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

from ingestion.readers.base import BaseReader


class DeltaReader(BaseReader):
    """Reads a Delta Lake table (Databricks or OSS Delta) by path or metastore name."""

    def read(self) -> DataFrame:
        options = dict(self.source_config.options)
        table = options.pop("table", None)
        path = options.pop("path", None)
        reader = self.spark.read.format("delta").options(**options)
        if table:
            return self.spark.read.format("delta").options(**options).table(table)
        if path:
            return reader.load(path)
        raise ValueError("Delta source requires 'table' or 'path' option")
