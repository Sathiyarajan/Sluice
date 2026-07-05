import pytest

from ingestion.utils.secrets import SecretNotFoundError, SecretsProvider


def test_resolve_env_secret(monkeypatch):
    monkeypatch.setenv("MY_SECRET", "super-secret")
    provider = SecretsProvider()
    assert provider.resolve("env:MY_SECRET") == "super-secret"


def test_resolve_env_secret_missing():
    provider = SecretsProvider()
    with pytest.raises(SecretNotFoundError):
        provider.resolve("env:DOES_NOT_EXIST_ABC")


def test_resolve_vault_without_client():
    provider = SecretsProvider()
    with pytest.raises(SecretNotFoundError):
        provider.resolve("vault:secret/data/db#password")


def test_unsupported_reference_format():
    provider = SecretsProvider()
    with pytest.raises(ValueError):
        provider.resolve("plaintext:whatever")
