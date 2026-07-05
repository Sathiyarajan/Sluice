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
