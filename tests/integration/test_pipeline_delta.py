"""Integration test: full IngestionPipeline run against local Spark + local Delta Lake."""
from ingestion.config.models import (
    DataQualityConfig,
    DatasetConfig,
    SourceConfig,
    SourceType,
    TargetConfig,
    TargetType,
    WriteMode,
)
from ingestion.pipeline import IngestionPipeline
from ingestion.transforms import AddIngestionMetadata
from ingestion.utils.checkpoint import FileCheckpointStore


def _write_source_parquet(spark, path, rows):
    df = spark.createDataFrame(rows, ["order_id", "customer_id", "amount", "updated_at"])
    df.write.mode("overwrite").parquet(path)


def test_pipeline_initial_load_then_incremental_merge(spark, tmp_path):
    source_path = str(tmp_path / "source")
    delta_path = str(tmp_path / "delta_target")
    checkpoint_dir = tmp_path / "checkpoints"

    _write_source_parquet(
        spark,
        source_path,
        [
            (1, "c1", 10.0, "2026-01-01"),
            (2, "c2", 20.0, "2026-01-02"),
        ],
    )

    config = DatasetConfig(
        dataset_name="customer_orders_it",
        source=SourceConfig(
            type=SourceType.S3_PARQUET,
            options={"path": source_path},
            watermark_column="updated_at",
        ),
        targets=[
            TargetConfig(
                type=TargetType.DATABRICKS_DELTA,
                write_mode=WriteMode.MERGE,
                merge_keys=["order_id"],
                options={"path": delta_path},
            )
        ],
        data_quality=DataQualityConfig(enabled=True, min_row_count=1),
    )

    checkpoint_store = FileCheckpointStore(checkpoint_dir)
    pipeline = IngestionPipeline(
        spark,
        config,
        checkpoint_store=checkpoint_store,
        transforms=[AddIngestionMetadata(config.dataset_name)],
    )
    result = pipeline.run()
    assert result.row_count == 2

    result_df = spark.read.format("delta").load(delta_path)
    assert result_df.count() == 2
    assert checkpoint_store.get_watermark("customer_orders_it") == "2026-01-02"

    # Simulate a later incremental batch: one update to an existing order, one new order.
    _write_source_parquet(
        spark,
        source_path,
        [
            (1, "c1", 15.0, "2026-01-03"),
            (2, "c2", 20.0, "2026-01-02"),
            (3, "c3", 30.0, "2026-01-04"),
        ],
    )

    pipeline_2 = IngestionPipeline(
        spark,
        config,
        checkpoint_store=checkpoint_store,
        transforms=[AddIngestionMetadata(config.dataset_name)],
    )
    result_2 = pipeline_2.run()
    assert result_2.row_count == 2  # only rows past the watermark (order 1 update + order 3)

    final_df = spark.read.format("delta").load(delta_path)
    rows = {row["order_id"]: row["amount"] for row in final_df.collect()}
    assert rows == {1: 15.0, 2: 20.0, 3: 30.0}
    assert checkpoint_store.get_watermark("customer_orders_it") == "2026-01-04"


def test_pipeline_raises_on_dq_violation(spark, tmp_path):
    source_path = str(tmp_path / "source_empty")
    delta_path = str(tmp_path / "delta_target_empty")
    spark.createDataFrame([], "order_id INT, customer_id STRING").write.mode(
        "overwrite"
    ).parquet(source_path)

    config = DatasetConfig(
        dataset_name="empty_dataset_it",
        source=SourceConfig(type=SourceType.S3_PARQUET, options={"path": source_path}),
        targets=[
            TargetConfig(type=TargetType.DATABRICKS_DELTA, options={"path": delta_path})
        ],
        data_quality=DataQualityConfig(
            enabled=True, min_row_count=1, fail_pipeline_on_violation=True
        ),
    )
    pipeline = IngestionPipeline(spark, config, checkpoint_store=FileCheckpointStore(tmp_path / "cp"))

    from ingestion.pipeline import DataQualityFailure

    try:
        pipeline.run()
        assert False, "expected DataQualityFailure"
    except DataQualityFailure as exc:
        assert not exc.result.passed
