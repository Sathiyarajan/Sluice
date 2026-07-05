#!/usr/bin/env bash
# Generic entrypoint for every reader/writer image. Same framework code runs
# unmodified in every image -- only the bundled connector jars/libs differ.
#
# Usage:
#   entrypoint.sh --config-path /configs/customer_orders.yaml [--dataset NAME] [--mode reader|writer|full]
#
# --dataset defaults to the config file's basename (without extension).
# --mode is translated to the framework's native --target-type flag:
#   full    -> no restriction, pipeline runs source read + all configured targets
#   writer  -> requires --target-type to also be passed (which target to restrict to)
#   reader  -> not supported by the underlying pipeline (it always reads then
#              writes); passing it is a no-op equivalent to "full" and a warning
#              is printed. See README troubleshooting section.
set -euo pipefail

CONFIG_PATH=""
DATASET=""
MODE="full"
TARGET_TYPE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --config-path|--config)
            CONFIG_PATH="$2"; shift 2 ;;
        --dataset)
            DATASET="$2"; shift 2 ;;
        --mode)
            MODE="$2"; shift 2 ;;
        --target-type)
            TARGET_TYPE="$2"; shift 2 ;;
        *)
            echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [[ -z "$CONFIG_PATH" ]]; then
    echo "entrypoint.sh: --config-path <path-to-yaml> is required" >&2
    exit 2
fi

if [[ -z "$DATASET" ]]; then
    DATASET="$(basename "$CONFIG_PATH")"
    DATASET="${DATASET%.*}"
fi

case "$MODE" in
    reader)
        echo "WARNING: --mode reader has no equivalent in run_pipeline.py (source is always read); running full pipeline" >&2
        ;;
    writer)
        if [[ -z "$TARGET_TYPE" ]]; then
            echo "entrypoint.sh: --mode writer requires --target-type <databricks_delta|snowflake|hive|s3>" >&2
            exit 2
        fi
        ;;
    full) : ;;
    *)
        echo "entrypoint.sh: unknown --mode '$MODE' (expected reader|writer|full)" >&2
        exit 2
        ;;
esac

PIPELINE_ARGS=(--dataset "$DATASET" --config-path "$CONFIG_PATH")
if [[ -n "$TARGET_TYPE" ]]; then
    PIPELINE_ARGS+=(--target-type "$TARGET_TYPE")
fi

# Run under spark-submit (not bare `python3 -m`) so connector jars in
# SPARK_JARS_DIR and driver/executor memory settings actually take effect --
# a plain SparkSession.builder.getOrCreate() call does not pick those up.
JAR_ARGS=()
if [[ -d "${SPARK_JARS_DIR:-/opt/spark-jars}" ]] && compgen -G "${SPARK_JARS_DIR:-/opt/spark-jars}/*.jar" > /dev/null; then
    JARS=$(echo "${SPARK_JARS_DIR:-/opt/spark-jars}"/*.jar | tr ' ' ',')
    JAR_ARGS=(--jars "$JARS")
fi

exec spark-submit \
    --master "${SPARK_MASTER:-local[*]}" \
    --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
    --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
    --conf "spark.driver.maxResultSize=${SPARK_DRIVER_MAX_RESULT_SIZE:-1g}" \
    "${JAR_ARGS[@]}" \
    /usr/local/bin/spark_entry.py "${PIPELINE_ARGS[@]}"
