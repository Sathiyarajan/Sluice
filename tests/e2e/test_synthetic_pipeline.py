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

"""End-to-end test: synthetic customer_orders dataset flowing through the same
config-driven pipeline used in production, from the sample YAML config through
to a Hive target with data-quality gating, mirroring the real dataset config
shipped in config/datasets/customer_orders.yaml."""
import random
from datetime import datetime, timedelta

from ingestion.config.loader import load_config
from ingestion.pipeline import IngestionPipeline
from ingestion.transforms import AddIngestionMetadata, Deduplicate
from ingestion.utils.checkpoint import FileCheckpointStore
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "datasets" / "customer_orders.yaml"


def _synthetic_orders(spark, path, n=500):
    random.seed(42)
    base_date = datetime(2026, 1, 1)
    rows = [
        (
            f"order-{i}",
            f"cust-{i % 50}",
            round(random.uniform(5, 500), 2),
            (base_date + timedelta(minutes=i)).isoformat(),
        )
        for i in range(n)
    ]
    df = spark.createDataFrame(rows, ["order_id", "customer_id", "amount", "updated_at"])
    df.write.mode("overwrite").parquet(path)
    return n


def test_synthetic_end_to_end_pipeline(spark, tmp_path):
    source_path = str(tmp_path / "synthetic_source")
    row_count = _synthetic_orders(spark, source_path)

    config = load_config(CONFIG_PATH)
    config.source.options["path"] = source_path
    delta_path = str(tmp_path / "delta_target")
    config.targets[0].options = {"path": delta_path}
    config.data_quality.min_row_count = 1
    config.data_quality.max_null_fraction = {}

    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoints")
    pipeline = IngestionPipeline(
        spark,
        config,
        checkpoint_store=checkpoint_store,
        transforms=[
            Deduplicate(["order_id"], order_by="updated_at"),
            AddIngestionMetadata(config.dataset_name),
        ],
    )
    result = pipeline.run()

    assert result.row_count == row_count
    assert result.dq_result.passed

    written = spark.read.format("delta").load(delta_path)
    assert written.count() == row_count
    assert "_ingested_at" in written.columns
    assert checkpoint_store.get_watermark(config.dataset_name) is not None
