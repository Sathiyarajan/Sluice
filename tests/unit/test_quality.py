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

from ingestion.config.models import DataQualityConfig, SchemaField
from ingestion.utils.quality import run_quality_checks


def test_quality_pass(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    config = DataQualityConfig(enabled=True, min_row_count=1)
    result = run_quality_checks(df, config)
    assert result.passed
    assert result.row_count == 2


def test_quality_min_row_count_violation(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    config = DataQualityConfig(enabled=True, min_row_count=5)
    result = run_quality_checks(df, config)
    assert not result.passed
    assert any(v.check == "min_row_count" for v in result.violations)


def test_quality_null_fraction_violation(spark):
    df = spark.createDataFrame(
        [(1, None), (2, None), (3, "c")], ["id", "name"]
    )
    config = DataQualityConfig(enabled=True, max_null_fraction={"name": 0.1})
    result = run_quality_checks(df, config)
    assert not result.passed
    assert any(v.check == "max_null_fraction" for v in result.violations)


def test_quality_schema_violation(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    config = DataQualityConfig(enabled=True)
    expected = [SchemaField(name="missing_col", type="string")]
    result = run_quality_checks(df, config, expected_fields=expected)
    assert not result.passed
    assert any(v.check == "schema_validation" for v in result.violations)


def test_quality_disabled(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    config = DataQualityConfig(enabled=False)
    result = run_quality_checks(df, config)
    assert result.passed
