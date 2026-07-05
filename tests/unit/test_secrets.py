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
