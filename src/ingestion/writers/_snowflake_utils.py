"""Thin wrapper around the Snowflake Spark connector's Utils.runQuery, isolated so it
can be monkeypatched in unit tests without a real Snowflake connection."""
from __future__ import annotations

from pyspark.sql import SparkSession


def run_snowflake_query(spark: SparkSession, connection_options: dict[str, str], sql: str) -> None:
    sfUtils = spark._jvm.net.snowflake.spark.snowflake.Utils  # noqa: SLF001
    sfUtils.runQuery(connection_options, sql)
