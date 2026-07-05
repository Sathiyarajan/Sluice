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
