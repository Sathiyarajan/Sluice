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

from ingestion.config.models import TargetConfig, TargetType, WriteMode
from ingestion.writers.factory import WriterFactory


def test_s3_writer_append(spark, tmp_path):
    path = str(tmp_path / "out")
    target = TargetConfig(type=TargetType.S3, write_mode=WriteMode.APPEND, options={"path": path})
    writer = WriterFactory.create(spark, target)

    df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
    writer.write(df1)
    df2 = spark.createDataFrame([(2, "b")], ["id", "val"])
    writer.write(df2)

    result = spark.read.parquet(path)
    assert result.count() == 2


def test_s3_writer_overwrite(spark, tmp_path):
    path = str(tmp_path / "out")
    target = TargetConfig(
        type=TargetType.S3, write_mode=WriteMode.OVERWRITE, options={"path": path}
    )
    writer = WriterFactory.create(spark, target)

    writer.write(spark.createDataFrame([(1, "a")], ["id", "val"]))
    writer.write(spark.createDataFrame([(2, "b")], ["id", "val"]))

    result = spark.read.parquet(path)
    assert result.count() == 1
    assert result.first()["id"] == 2


def test_s3_writer_merge_upsert(spark, tmp_path):
    path = str(tmp_path / "out")
    target = TargetConfig(
        type=TargetType.S3,
        write_mode=WriteMode.MERGE,
        merge_keys=["id"],
        options={"path": path},
    )
    writer = WriterFactory.create(spark, target)

    writer.write(spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]))
    writer.write(spark.createDataFrame([(2, "b-updated"), (3, "c")], ["id", "val"]))

    result = spark.read.parquet(path)
    rows = {row["id"]: row["val"] for row in result.collect()}
    assert rows == {1: "a", 2: "b-updated", 3: "c"}


def test_s3_writer_partitioned(spark, tmp_path):
    path = str(tmp_path / "out")
    target = TargetConfig(
        type=TargetType.S3,
        write_mode=WriteMode.APPEND,
        partition_columns=["region"],
        options={"path": path},
    )
    writer = WriterFactory.create(spark, target)
    writer.write(spark.createDataFrame([(1, "us"), (2, "eu")], ["id", "region"]))

    import os

    assert any(p.startswith("region=") for p in os.listdir(path))
