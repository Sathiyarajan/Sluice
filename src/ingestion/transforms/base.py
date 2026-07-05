"""Base transform interface. Transforms are pure DataFrame -> DataFrame steps."""
from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class Transform(ABC):
    @abstractmethod
    def apply(self, df: DataFrame) -> DataFrame: ...
