"""S3 writer for Parquet/Delta, partitioned, with a checksum/manifest file written
alongside the data for downstream consumers to verify completeness."""
from __future__ import annotations

import hashlib
import json
import time

from pyspark.sql import DataFrame

from ingestion.writers.base import BaseWriter


class S3Writer(BaseWriter):
    def _path(self) -> str:
        path = self.target_config.options.get("path")
        if not path:
            raise ValueError("S3 target requires 'path' option")
        return path

    def _format(self) -> str:
        return self.target_config.options.get("format", "parquet")

    def _writer(self, df: DataFrame, mode: str):
        writer = df.write.format(self._format()).mode(mode)
        if self.target_config.partition_columns:
            writer = writer.partitionBy(*self.target_config.partition_columns)
        return writer

    def append(self, df: DataFrame) -> None:
        self._writer(df, "append").save(self._path())
        self._write_manifest(df)

    def overwrite(self, df: DataFrame) -> None:
        self._writer(df, "overwrite").save(self._path())
        self._write_manifest(df)

    def merge(self, df: DataFrame) -> None:
        """S3/Parquet has no native merge; emulate upsert via anti-join + overwrite,
        same approach as HiveWriter."""
        path = self._path()
        merge_keys = self.target_config.merge_keys
        try:
            existing = self.spark.read.format(self._format()).load(path)
            existing = existing.cache()
            existing.count()  # force materialization before the source path is overwritten
        except Exception:
            self.overwrite(df)
            return
        from functools import reduce
        import operator

        condition = reduce(operator.and_, [existing[k] == df[k] for k in merge_keys])
        unmatched_existing = existing.join(df, condition, "left_anti")
        combined = unmatched_existing.unionByName(df, allowMissingColumns=True).cache()
        combined.count()
        existing.unpersist()
        self._writer(combined, "overwrite").save(path)
        self._write_manifest(combined)
        combined.unpersist()

    def _write_manifest(self, df: DataFrame) -> None:
        row_count = df.count()
        manifest = {
            "path": self._path(),
            "row_count": row_count,
            "written_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "checksum": hashlib.sha256(f"{self._path()}:{row_count}".encode()).hexdigest(),
        }
        manifest_path = self.target_config.options.get("manifest_path")
        if manifest_path:
            manifest_df = self.spark.createDataFrame([manifest])
            manifest_df.coalesce(1).write.mode("overwrite").json(manifest_path)
