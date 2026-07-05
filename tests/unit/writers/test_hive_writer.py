from ingestion.config.models import TargetConfig, TargetType, WriteMode
from ingestion.writers.factory import WriterFactory


def test_hive_writer_overwrite_then_merge(spark):
    table = "default.orders_writer_test"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    target = TargetConfig(
        type=TargetType.HIVE,
        write_mode=WriteMode.OVERWRITE,
        options={"table": table},
    )
    writer = WriterFactory.create(spark, target)
    writer.write(spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]))
    assert spark.table(table).count() == 2

    merge_target = TargetConfig(
        type=TargetType.HIVE,
        write_mode=WriteMode.MERGE,
        merge_keys=["id"],
        options={"table": table},
    )
    merge_writer = WriterFactory.create(spark, merge_target)
    merge_writer.write(spark.createDataFrame([(2, "b-new"), (3, "c")], ["id", "val"]))

    rows = {row["id"]: row["val"] for row in spark.table(table).collect()}
    assert rows == {1: "a", 2: "b-new", 3: "c"}
    spark.sql(f"DROP TABLE IF EXISTS {table}")
