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

"""Load dataset ingestion configs from YAML/JSON files."""
from __future__ import annotations

import json
from pathlib import Path

import yaml

from ingestion.config.models import DatasetConfig


def load_config(path: str | Path) -> DatasetConfig:
    path = Path(path)
    raw = path.read_text(encoding="utf-8")
    if path.suffix in (".yaml", ".yml"):
        data = yaml.safe_load(raw)
    elif path.suffix == ".json":
        data = json.loads(raw)
    else:
        raise ValueError(f"Unsupported config extension: {path.suffix}")
    return DatasetConfig.model_validate(data)


def load_config_dir(directory: str | Path) -> list[DatasetConfig]:
    directory = Path(directory)
    configs = []
    for path in sorted(directory.glob("*.y*ml")) + sorted(directory.glob("*.json")):
        configs.append(load_config(path))
    return configs
