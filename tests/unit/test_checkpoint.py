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

from ingestion.utils.checkpoint import FileCheckpointStore


def test_checkpoint_roundtrip(tmp_path):
    store = FileCheckpointStore(tmp_path)
    assert store.get_watermark("orders") is None
    store.set_watermark("orders", "2026-07-01T00:00:00")
    assert store.get_watermark("orders") == "2026-07-01T00:00:00"


def test_checkpoint_overwrite(tmp_path):
    store = FileCheckpointStore(tmp_path)
    store.set_watermark("orders", "a")
    store.set_watermark("orders", "b")
    assert store.get_watermark("orders") == "b"
