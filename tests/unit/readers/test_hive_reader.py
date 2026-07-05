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


def test_read_hive_table(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.createOrReplaceTempView("orders_view")
    df.write.mode("overwrite").saveAsTable("default.orders_hive_test")

    source = SourceConfig(type=SourceType.HIVE, options={"table": "default.orders_hive_test"})
    reader = ReaderFactory.create(spark, source)
    result = reader.read()
    assert result.count() == 2
