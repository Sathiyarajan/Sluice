"""Metrics emission abstraction. Default emitter logs to stdout as JSON;
swap in a StatsD/CloudWatch emitter in production via `set_emitter`."""
from __future__ import annotations

from typing import Any, Protocol

from ingestion.utils.logging_utils import get_logger

logger = get_logger("ingestion.metrics")


class MetricsEmitter(Protocol):
    def emit(self, metric_name: str, value: float, tags: dict[str, str]) -> None: ...


class LoggingMetricsEmitter:
    def emit(self, metric_name: str, value: float, tags: dict[str, str] | None = None) -> None:
        logger.info(
            "metric_emitted",
            extra={"context": {"metric": metric_name, "value": value, "tags": tags or {}}},
        )


_emitter: MetricsEmitter = LoggingMetricsEmitter()


def set_emitter(emitter: MetricsEmitter) -> None:
    global _emitter
    _emitter = emitter


def emit_metric(metric_name: str, value: float, tags: dict[str, str] | None = None) -> None:
    _emitter.emit(metric_name, value, tags or {})
