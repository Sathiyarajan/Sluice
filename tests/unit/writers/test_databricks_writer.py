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
