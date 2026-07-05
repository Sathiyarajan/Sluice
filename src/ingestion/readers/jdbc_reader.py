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
from ingestion.utils.secrets import resolve_secret


class JdbcReader(BaseReader):
    """Reads from any JDBC-compliant source (Postgres, MySQL, Oracle, SQL Server, ...)."""

    def read(self) -> DataFrame:
        options = dict(self.source_config.options)
        if "password_secret" in options:
            options["password"] = resolve_secret(options.pop("password_secret"))
        query_or_table = options.pop("query", None) or options.pop("dbtable", None)
        if not query_or_table:
            raise ValueError("JDBC source requires 'query' or 'dbtable' option")
        reader = self.spark.read.format("jdbc").options(**options)
        reader = reader.option("dbtable", query_or_table)
        return reader.load()
