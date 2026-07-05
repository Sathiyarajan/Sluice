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


def test_databricks_writer_append_and_merge(spark, tmp_path):
    path = str(tmp_path / "delta_out")
    append_target = TargetConfig(
        type=TargetType.DATABRICKS_DELTA,
        write_mode=WriteMode.APPEND,
        options={"path": path},
    )
    writer = WriterFactory.create(spark, append_target)
    writer.write(spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]))

    result = spark.read.format("delta").load(path)
    assert result.count() == 2

    merge_target = TargetConfig(
        type=TargetType.DATABRICKS_DELTA,
        write_mode=WriteMode.MERGE,
        merge_keys=["id"],
        options={"path": path},
    )
    merge_writer = WriterFactory.create(spark, merge_target)
    merge_writer.write(spark.createDataFrame([(2, "b-updated"), (3, "c")], ["id", "val"]))

    result = spark.read.format("delta").load(path)
    rows = {row["id"]: row["val"] for row in result.collect()}
    assert rows == {1: "a", 2: "b-updated", 3: "c"}


def test_databricks_writer_overwrite(spark, tmp_path):
    path = str(tmp_path / "delta_out2")
    target = TargetConfig(
        type=TargetType.DATABRICKS_DELTA, write_mode=WriteMode.OVERWRITE, options={"path": path}
    )
    writer = WriterFactory.create(spark, target)
    writer.write(spark.createDataFrame([(1, "a")], ["id", "val"]))
    writer.write(spark.createDataFrame([(2, "b")], ["id", "val"]))

    result = spark.read.format("delta").load(path)
    assert result.count() == 1
    assert result.first()["id"] == 2
