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


class HiveReader(BaseReader):
    def read(self) -> DataFrame:
        options = dict(self.source_config.options)
        table = options.get("table")
        if not table:
            raise ValueError("Hive source requires 'table' option (db.table)")
        df = self.spark.table(table)
        partition_filter = options.get("partition_filter")
        if partition_filter:
            df = df.filter(partition_filter)
        return df
