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

from pathlib import Path

from ingestion.config.loader import load_config, load_config_dir

FIXTURES = Path(__file__).parent.parent / "fixtures"


def test_load_yaml_config():
    config = load_config(FIXTURES / "sample_dataset.yaml")
    assert config.dataset_name == "customer_orders"
    assert config.source.type.value == "s3_parquet"
    assert config.targets[0].write_mode.value == "merge"


def test_load_config_dir():
    configs = load_config_dir(FIXTURES)
    names = {c.dataset_name for c in configs}
    assert "customer_orders" in names
