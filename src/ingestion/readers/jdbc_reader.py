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
