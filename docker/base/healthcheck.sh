#!/usr/bin/env bash
# Verifies the Python/PySpark/Delta runtime imports cleanly. Does not start a
# SparkSession (too slow/heavy for a liveness probe) -- import success is
# sufficient to catch a broken venv or missing jars on the classpath.
set -euo pipefail
python3 -c "import pyspark, delta, ingestion.pipeline" || exit 1
