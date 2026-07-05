import pytest

from ingestion.config.models import TargetConfig, TargetType
from ingestion.writers.base import BaseWriter
from ingestion.writers.factory import WriterFactory
from ingestion.writers.snowflake_writer import SnowflakeWriter


def test_factory_dispatches_snowflake(spark):
    target = TargetConfig(type=TargetType.SNOWFLAKE, options={"table": "ORDERS"})
    writer = WriterFactory.create(spark, target)
    assert isinstance(writer, SnowflakeWriter)


def test_unsupported_write_mode_raises(spark, tmp_path):
    from ingestion.writers.base import UnsupportedWriteModeError

    target = TargetConfig(type=TargetType.S3, options={"path": str(tmp_path)})
    writer = WriterFactory.create(spark, target)
    writer.target_config.write_mode = "bogus"  # type: ignore[assignment]
    with pytest.raises(UnsupportedWriteModeError):
        writer.write(spark.createDataFrame([(1,)], ["id"]))
