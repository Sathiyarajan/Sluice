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

"""Dead-letter handling: route rows failing quality/parsing checks to a DLQ path."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ingestion.utils.logging_utils import get_logger

logger = get_logger("ingestion.dead_letter")


class DeadLetterHandler:
    def __init__(self, dlq_base_path: str) -> None:
        self._dlq_base_path = dlq_base_path.rstrip("/")

    def send(self, df: DataFrame, dataset_name: str, reason: str) -> str:
        target_path = f"{self._dlq_base_path}/{dataset_name}"
        df_with_reason = df.withColumn("_dlq_reason", F.lit(reason))
        df_with_reason.write.mode("append").format("parquet").save(target_path)
        logger.warning(
            "records_sent_to_dlq",
            extra={"context": {"dataset": dataset_name, "reason": reason, "path": target_path}},
        )
        return target_path
