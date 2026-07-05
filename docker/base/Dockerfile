ARG PYTHON_VERSION=3.11
ARG SPARK_VERSION=3.5.1
ARG DELTA_VERSION=3.2.0

# ---------- builder ----------
FROM eclipse-temurin:17-jdk-jammy AS builder

ARG PYTHON_VERSION
ARG SPARK_VERSION
ARG DELTA_VERSION

RUN apt-get update && apt-get install -y --no-install-recommends \
        python${PYTHON_VERSION} python${PYTHON_VERSION}-venv python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN python${PYTHON_VERSION} -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pyspark==${SPARK_VERSION} \
        delta-spark==${DELTA_VERSION} \
        pydantic>=2.5 \
        pyyaml>=6.0 \
        boto3 \
        structlog \
        pytest

# ---------- final ----------
FROM eclipse-temurin:17-jre-jammy AS final

ARG PYTHON_VERSION
ARG SPARK_VERSION
ARG DELTA_VERSION
ENV SPARK_VERSION=${SPARK_VERSION} \
    DELTA_VERSION=${DELTA_VERSION} \
    PATH="/opt/venv/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/venv/bin/python${PYTHON_VERSION} \
    SPARK_JARS_DIR=/opt/spark-jars \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        python${PYTHON_VERSION} curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 ingestion \
    && useradd --uid 1000 --gid ingestion --create-home --shell /bin/bash ingestion \
    && mkdir -p /opt/spark-jars /app \
    && chown -R ingestion:ingestion /opt/spark-jars /app

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app
COPY --chown=ingestion:ingestion src ./src
COPY --chown=ingestion:ingestion pyproject.toml ./
COPY docker/base/entrypoint.sh /usr/local/bin/entrypoint.sh
COPY docker/base/healthcheck.sh /usr/local/bin/healthcheck.sh
COPY docker/base/spark_entry.py /usr/local/bin/spark_entry.py
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/healthcheck.sh

RUN /opt/venv/bin/pip install --no-cache-dir --no-deps -e . \
    && echo "base image: python=$(python3 --version) spark=${SPARK_VERSION} delta=${DELTA_VERSION}"

USER ingestion

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD ["/usr/local/bin/healthcheck.sh"]

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
