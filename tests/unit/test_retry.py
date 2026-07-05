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

from ingestion.config.models import RetryConfig
from ingestion.utils.retry import with_retry


def test_retry_succeeds_after_transient_failures():
    calls = {"count": 0}

    @with_retry(RetryConfig(max_attempts=3, initial_delay_seconds=0.01, backoff_multiplier=1))
    def flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise ValueError("transient")
        return "ok"

    assert flaky() == "ok"
    assert calls["count"] == 3


def test_retry_raises_after_exhausting_attempts():
    calls = {"count": 0}

    @with_retry(RetryConfig(max_attempts=2, initial_delay_seconds=0.01, backoff_multiplier=1))
    def always_fails():
        calls["count"] += 1
        raise ValueError("permanent")

    with pytest.raises(ValueError):
        always_fails()
    assert calls["count"] == 2
