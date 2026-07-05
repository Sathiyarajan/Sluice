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
