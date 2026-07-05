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

"""Thin wrapper around the Snowflake Spark connector's Utils.runQuery, isolated so it
can be monkeypatched in unit tests without a real Snowflake connection."""
from __future__ import annotations

from pyspark.sql import SparkSession


def run_snowflake_query(spark: SparkSession, connection_options: dict[str, str], sql: str) -> None:
    sfUtils = spark._jvm.net.snowflake.spark.snowflake.Utils  # noqa: SLF001
    sfUtils.runQuery(connection_options, sql)
