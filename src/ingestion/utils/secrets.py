"""Secrets resolution: environment variables first, optional HashiCorp Vault backend.
No credentials are ever hardcoded in configs -- configs reference secret names only."""
from __future__ import annotations

import os
from typing import Optional


class SecretNotFoundError(RuntimeError):
    pass


class SecretsProvider:
    """Resolves secret references of the form 'env:VAR_NAME' or 'vault:path#key'."""

    def __init__(self, vault_client: Optional[object] = None) -> None:
        self._vault_client = vault_client

    def resolve(self, reference: str) -> str:
        if reference.startswith("env:"):
            var_name = reference.split(":", 1)[1]
            value = os.environ.get(var_name)
            if value is None:
                raise SecretNotFoundError(f"Environment variable {var_name!r} is not set")
            return value
        if reference.startswith("vault:"):
            if self._vault_client is None:
                raise SecretNotFoundError("Vault client not configured")
            path, _, key = reference.split(":", 1)[1].partition("#")
            secret = self._vault_client.read(path)  # type: ignore[attr-defined]
            data = secret["data"]["data"] if "data" in secret.get("data", {}) else secret["data"]
            if key not in data:
                raise SecretNotFoundError(f"Key {key!r} not found at vault path {path!r}")
            return data[key]
        raise ValueError(f"Unsupported secret reference format: {reference!r}")


default_provider = SecretsProvider()


def resolve_secret(reference: str) -> str:
    return default_provider.resolve(reference)
