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

"""Data quality checks: row counts, null fractions, schema validation."""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.config.models import DataQualityConfig, SchemaField


@dataclass
class DQViolation:
    check: str
    detail: str


@dataclass
class DQResult:
    passed: bool
    row_count: int
    violations: list[DQViolation] = field(default_factory=list)


def check_row_count(df: DataFrame, min_row_count: int) -> tuple[int, DQViolation | None]:
    count = df.count()
    if count < min_row_count:
        return count, DQViolation(
            "min_row_count", f"row_count={count} < min_row_count={min_row_count}"
        )
    return count, None


def check_null_fractions(
    df: DataFrame, row_count: int, max_null_fraction: dict[str, float]
) -> list[DQViolation]:
    violations: list[DQViolation] = []
    if not max_null_fraction or row_count == 0:
        return violations
    agg_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in max_null_fraction
    ]
    null_counts = df.agg(*agg_exprs).collect()[0].asDict()
    for column, max_fraction in max_null_fraction.items():
        fraction = null_counts.get(column, 0) / row_count
        if fraction > max_fraction:
            violations.append(
                DQViolation(
                    "max_null_fraction",
                    f"column={column} null_fraction={fraction:.4f} > max={max_fraction}",
                )
            )
    return violations


def check_schema(df: DataFrame, expected_fields: list[SchemaField]) -> list[DQViolation]:
    violations: list[DQViolation] = []
    if not expected_fields:
        return violations
    actual_fields = {f.name: f.dataType.typeName() for f in df.schema.fields}
    for field_def in expected_fields:
        if field_def.name not in actual_fields:
            violations.append(
                DQViolation("schema_validation", f"missing expected column {field_def.name!r}")
            )
    return violations


def run_quality_checks(
    df: DataFrame, config: DataQualityConfig, expected_fields: list[SchemaField] | None = None
) -> DQResult:
    if not config.enabled:
        return DQResult(passed=True, row_count=-1)

    row_count, count_violation = check_row_count(df, config.min_row_count)
    violations = [count_violation] if count_violation else []
    violations.extend(check_null_fractions(df, row_count, config.max_null_fraction))
    violations.extend(check_schema(df, expected_fields or []))

    return DQResult(passed=len(violations) == 0, row_count=row_count, violations=violations)
