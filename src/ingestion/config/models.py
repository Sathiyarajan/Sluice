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

"""Pydantic models for dataset ingestion configs (one YAML/JSON per dataset)."""
from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class SourceType(str, Enum):
    JDBC = "jdbc"
    S3_PARQUET = "s3_parquet"
    S3_CSV = "s3_csv"
    S3_JSON = "s3_json"
    HIVE = "hive"
    KAFKA = "kafka"
    DELTA = "delta"


class TargetType(str, Enum):
    DATABRICKS_DELTA = "databricks_delta"
    SNOWFLAKE = "snowflake"
    HIVE = "hive"
    S3 = "s3"


class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"
    UPSERT = "upsert"


class SchemaField(BaseModel):
    name: str
    type: str
    nullable: bool = True


class SourceConfig(BaseModel):
    type: SourceType
    options: dict[str, Any] = Field(default_factory=dict)
    schema_fields: list[SchemaField] = Field(default_factory=list, alias="schema")
    watermark_column: Optional[str] = None

    model_config = {"populate_by_name": True}


class TargetConfig(BaseModel):
    type: TargetType
    options: dict[str, Any] = Field(default_factory=dict)
    write_mode: WriteMode = WriteMode.APPEND
    merge_keys: list[str] = Field(default_factory=list)
    partition_columns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_merge_keys(self) -> "TargetConfig":
        if self.write_mode in (WriteMode.MERGE, WriteMode.UPSERT) and not self.merge_keys:
            raise ValueError(
                f"write_mode={self.write_mode.value} requires at least one merge_keys entry"
            )
        return self


class RetryConfig(BaseModel):
    max_attempts: int = 3
    initial_delay_seconds: float = 2.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 60.0


class SLAConfig(BaseModel):
    max_runtime_minutes: int = 60
    alert_channels: list[str] = Field(default_factory=list)


class DataQualityConfig(BaseModel):
    enabled: bool = True
    min_row_count: int = 0
    max_null_fraction: dict[str, float] = Field(default_factory=dict)
    fail_pipeline_on_violation: bool = True


class DatasetConfig(BaseModel):
    dataset_name: str
    description: str = ""
    owner: str = "data-eng"
    source: SourceConfig
    targets: list[TargetConfig]
    retry: RetryConfig = Field(default_factory=RetryConfig)
    sla: SLAConfig = Field(default_factory=SLAConfig)
    data_quality: DataQualityConfig = Field(default_factory=DataQualityConfig)
    schedule: str = "@daily"
    tags: list[str] = Field(default_factory=list)
    upstream_dependencies: list[str] = Field(default_factory=list)

    @field_validator("dataset_name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        if not v or not v.replace("_", "").isalnum():
            raise ValueError("dataset_name must be alphanumeric/underscore")
        return v
