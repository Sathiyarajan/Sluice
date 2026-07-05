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

import pytest
from pydantic import ValidationError

from ingestion.config.models import (
    DatasetConfig,
    SourceConfig,
    SourceType,
    TargetConfig,
    TargetType,
    WriteMode,
)


def _base_source():
    return SourceConfig(type=SourceType.S3_PARQUET, options={"path": "s3://bucket/path"})


def test_target_config_merge_requires_keys():
    with pytest.raises(ValidationError):
        TargetConfig(type=TargetType.HIVE, write_mode=WriteMode.MERGE, merge_keys=[])


def test_target_config_merge_with_keys_ok():
    target = TargetConfig(
        type=TargetType.HIVE, write_mode=WriteMode.MERGE, merge_keys=["id"]
    )
    assert target.write_mode == WriteMode.MERGE


def test_dataset_config_rejects_invalid_name():
    with pytest.raises(ValidationError):
        DatasetConfig(
            dataset_name="bad name!",
            source=_base_source(),
            targets=[TargetConfig(type=TargetType.S3)],
        )


def test_dataset_config_defaults():
    config = DatasetConfig(
        dataset_name="customer_orders",
        source=_base_source(),
        targets=[TargetConfig(type=TargetType.S3)],
    )
    assert config.retry.max_attempts == 3
    assert config.sla.max_runtime_minutes == 60
    assert config.data_quality.enabled is True
