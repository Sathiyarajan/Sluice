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

from ingestion.transforms import (
    AddIngestionMetadata,
    ColumnCast,
    ColumnRename,
    Deduplicate,
    DropColumns,
)


def test_column_rename(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "old_name"])
    result = ColumnRename({"old_name": "new_name"}).apply(df)
    assert result.columns == ["id", "new_name"]


def test_column_cast(spark):
    df = spark.createDataFrame([("1",)], ["id"])
    result = ColumnCast({"id": "int"}).apply(df)
    assert dict(result.dtypes)["id"] == "int"


def test_add_ingestion_metadata(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    result = AddIngestionMetadata("my_dataset").apply(df)
    assert "_ingested_at" in result.columns
    assert result.select("_dataset_name").first()[0] == "my_dataset"


def test_drop_columns(spark):
    df = spark.createDataFrame([(1, "x")], ["id", "junk"])
    result = DropColumns(["junk"]).apply(df)
    assert result.columns == ["id"]


def test_deduplicate_no_order(spark):
    df = spark.createDataFrame([(1, "a"), (1, "a")], ["id", "val"])
    result = Deduplicate(["id"]).apply(df)
    assert result.count() == 1


def test_deduplicate_with_order(spark):
    df = spark.createDataFrame(
        [(1, 100), (1, 200)], ["id", "version"]
    )
    result = Deduplicate(["id"], order_by="version").apply(df)
    assert result.count() == 1
    assert result.first()["version"] == 200
