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

"""Retry/backoff decorator driven by RetryConfig."""
from __future__ import annotations

import time
from functools import wraps
from typing import Callable, TypeVar

from ingestion.config.models import RetryConfig
from ingestion.utils.logging_utils import get_logger

logger = get_logger("ingestion.retry")

T = TypeVar("T")


def with_retry(retry_config: RetryConfig, exceptions: tuple[type[BaseException], ...] = (Exception,)):
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            delay = retry_config.initial_delay_seconds
            last_exc: BaseException | None = None
            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    logger.warning(
                        "retry_attempt_failed",
                        extra={
                            "context": {
                                "function": func.__name__,
                                "attempt": attempt,
                                "max_attempts": retry_config.max_attempts,
                                "error": str(exc),
                            }
                        },
                    )
                    if attempt == retry_config.max_attempts:
                        break
                    time.sleep(min(delay, retry_config.max_delay_seconds))
                    delay *= retry_config.backoff_multiplier
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator
