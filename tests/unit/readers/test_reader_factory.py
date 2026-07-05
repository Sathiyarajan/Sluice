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

import pytest

from ingestion.config.models import SourceConfig, SourceType
from ingestion.readers.base import BaseReader
from ingestion.readers.factory import ReaderFactory
from ingestion.readers.jdbc_reader import JdbcReader


def test_factory_dispatches_jdbc(spark):
    source = SourceConfig(type=SourceType.JDBC, options={"query": "orders"})
    reader = ReaderFactory.create(spark, source)
    assert isinstance(reader, JdbcReader)


def test_factory_register_custom_reader(spark):
    class CustomReader(BaseReader):
        def read(self):
            return spark.createDataFrame([(1,)], ["id"])

    ReaderFactory.register(SourceType.KAFKA, CustomReader)
    source = SourceConfig(type=SourceType.KAFKA, options={})
    reader = ReaderFactory.create(spark, source)
    assert isinstance(reader, CustomReader)
    assert reader.read().count() == 1
