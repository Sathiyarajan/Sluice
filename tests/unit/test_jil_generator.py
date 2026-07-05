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

import sys
from pathlib import Path

AUTOSYS_DIR = Path(__file__).parent.parent.parent / "autosys"
sys.path.insert(0, str(AUTOSYS_DIR))

from jil_generator import generate_jil_files, render_dataset_jil  # noqa: E402
from ingestion.config.loader import load_config  # noqa: E402

CONFIG_PATH = Path(__file__).parent.parent.parent / "config" / "datasets" / "customer_orders.yaml"


def test_render_dataset_jil_contains_box_and_command_jobs():
    config = load_config(CONFIG_PATH)
    jil = render_dataset_jil(config)
    assert "insert_job: BOX_CUSTOMER_ORDERS   job_type: BOX" in jil
    assert "insert_job: CMD_CUSTOMER_ORDERS_TO_DATABRICKS_DELTA   job_type: CMD" in jil
    assert "box_name: BOX_CUSTOMER_ORDERS" in jil
    assert "n_retrys: 2" in jil


def test_render_dataset_jil_includes_upstream_condition():
    config = load_config(CONFIG_PATH)
    jil = render_dataset_jil(config)
    assert "condition: success(BOX_RAW_CUSTOMER_ORDERS_LANDED)" in jil


def test_generate_jil_files_writes_to_output_dir(tmp_path):
    written = generate_jil_files(
        config_dir=CONFIG_PATH.parent, output_dir=tmp_path / "generated"
    )
    assert "customer_orders" in written
    output_file = Path(written["customer_orders"])
    assert output_file.exists()
    assert "BOX_CUSTOMER_ORDERS" in output_file.read_text(encoding="utf-8")
