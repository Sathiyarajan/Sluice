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
