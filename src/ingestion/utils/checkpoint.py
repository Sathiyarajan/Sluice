"""Checkpoint/watermark store for incremental loads. File-backed JSON store by default;
swap in a DB-backed implementation for multi-worker production deployments."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional


class CheckpointStore:
    def get_watermark(self, dataset_name: str) -> Optional[str]:
        raise NotImplementedError

    def set_watermark(self, dataset_name: str, value: str) -> None:
        raise NotImplementedError


class FileCheckpointStore(CheckpointStore):
    def __init__(self, base_path: str | Path) -> None:
        self._base_path = Path(base_path)
        self._base_path.mkdir(parents=True, exist_ok=True)

    def _path_for(self, dataset_name: str) -> Path:
        return self._base_path / f"{dataset_name}.json"

    def get_watermark(self, dataset_name: str) -> Optional[str]:
        path = self._path_for(dataset_name)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8")).get("watermark")

    def set_watermark(self, dataset_name: str, value: str) -> None:
        path = self._path_for(dataset_name)
        path.write_text(json.dumps({"watermark": value}), encoding="utf-8")
