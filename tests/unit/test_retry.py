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
