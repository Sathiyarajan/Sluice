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
