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

from ingestion.config.models import SourceConfig, SourceType
from ingestion.readers.factory import ReaderFactory


def test_read_parquet(spark, tmp_path):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    path = str(tmp_path / "data.parquet")
    df.write.mode("overwrite").parquet(path)

    source = SourceConfig(type=SourceType.S3_PARQUET, options={"path": path})
    reader = ReaderFactory.create(spark, source)
    result = reader.read()
    assert result.count() == 2


def test_read_csv(spark, tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,val\n1,a\n2,b\n", encoding="utf-8")

    source = SourceConfig(type=SourceType.S3_CSV, options={"path": str(csv_path)})
    reader = ReaderFactory.create(spark, source)
    result = reader.read()
    assert result.count() == 2
    assert set(result.columns) == {"id", "val"}


def test_read_incremental_with_watermark(spark, tmp_path):
    df = spark.createDataFrame(
        [(1, "2026-01-01"), (2, "2026-02-01"), (3, "2026-03-01")],
        ["id", "updated_at"],
    )
    path = str(tmp_path / "data.parquet")
    df.write.mode("overwrite").parquet(path)

    source = SourceConfig(
        type=SourceType.S3_PARQUET,
        options={"path": path},
        watermark_column="updated_at",
    )
    reader = ReaderFactory.create(spark, source)
    result = reader.read_incremental("2026-01-15")
    assert result.count() == 2
