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
