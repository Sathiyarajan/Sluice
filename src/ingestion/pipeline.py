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

"""Core metadata/config-driven ingestion engine: reader -> transforms -> DQ -> writer(s),
with checkpointing, retry/backoff, metrics, and dead-letter handling."""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from ingestion.config.models import DatasetConfig
from ingestion.readers.factory import ReaderFactory
from ingestion.transforms.base import Transform
from ingestion.utils.checkpoint import CheckpointStore, FileCheckpointStore
from ingestion.utils.dead_letter import DeadLetterHandler
from ingestion.utils.logging_utils import get_logger
from ingestion.utils.metrics import emit_metric
from ingestion.utils.quality import DQResult, run_quality_checks
from ingestion.utils.retry import with_retry
from ingestion.writers.factory import WriterFactory

logger = get_logger("ingestion.pipeline")


class DataQualityFailure(RuntimeError):
    def __init__(self, dataset_name: str, result: DQResult) -> None:
        self.result = result
        super().__init__(
            f"Data quality checks failed for {dataset_name!r}: "
            f"{[v.detail for v in result.violations]}"
        )


@dataclass
class PipelineRunResult:
    dataset_name: str
    row_count: int
    dq_result: DQResult | None = None
    targets_written: list[str] = field(default_factory=list)


class IngestionPipeline:
    def __init__(
        self,
        spark: SparkSession,
        config: DatasetConfig,
        checkpoint_store: CheckpointStore | None = None,
        dead_letter_handler: DeadLetterHandler | None = None,
        transforms: list[Transform] | None = None,
    ) -> None:
        self.spark = spark
        self.config = config
        self.checkpoint_store = checkpoint_store or FileCheckpointStore(".checkpoints")
        self.dead_letter_handler = dead_letter_handler
        self.transforms = transforms or []

    def _extract(self) -> DataFrame:
        reader = ReaderFactory.create(self.spark, self.config.source)
        watermark = self.checkpoint_store.get_watermark(self.config.dataset_name)
        return reader.read_incremental(watermark)

    def _transform(self, df: DataFrame) -> DataFrame:
        for transform in self.transforms:
            df = transform.apply(df)
        return df

    def _load(self, df: DataFrame) -> list[str]:
        written = []
        for target_config in self.config.targets:
            writer = WriterFactory.create(self.spark, target_config)
            run = with_retry(self.config.retry)(writer.write)
            run(df)
            written.append(target_config.type.value)
        return written

    def _advance_watermark(self, df: DataFrame) -> None:
        column = self.config.source.watermark_column
        if not column:
            return
        max_value = df.agg({column: "max"}).collect()[0][0]
        if max_value is not None:
            self.checkpoint_store.set_watermark(self.config.dataset_name, str(max_value))

    def run(self) -> PipelineRunResult:
        dataset_name = self.config.dataset_name
        logger.info("pipeline_started", extra={"context": {"dataset": dataset_name}})

        df = self._extract()
        df = self._transform(df)
        df = df.cache()

        dq_result = run_quality_checks(
            df, self.config.data_quality, self.config.source.schema_fields
        )
        emit_metric("ingestion.row_count", dq_result.row_count, {"dataset": dataset_name})

        if not dq_result.passed:
            emit_metric("ingestion.dq_violations", len(dq_result.violations), {"dataset": dataset_name})
            if self.dead_letter_handler is not None:
                self.dead_letter_handler.send(
                    df, dataset_name, reason="; ".join(v.detail for v in dq_result.violations)
                )
            if self.config.data_quality.fail_pipeline_on_violation:
                raise DataQualityFailure(dataset_name, dq_result)

        targets_written = self._load(df)
        self._advance_watermark(df)

        logger.info(
            "pipeline_completed",
            extra={
                "context": {
                    "dataset": dataset_name,
                    "row_count": dq_result.row_count,
                    "targets": targets_written,
                }
            },
        )
        return PipelineRunResult(
            dataset_name=dataset_name,
            row_count=dq_result.row_count,
            dq_result=dq_result,
            targets_written=targets_written,
        )
