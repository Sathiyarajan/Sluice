ARG BASE_IMAGE=ingestion-framework/base:latest
FROM ${BASE_IMAGE}

USER root

ARG DELTA_VERSION=3.2.0
ARG SCALA_VERSION=2.12

# Databricks JDBC driver is EULA-gated (no public Maven coordinate) --
# download it from https://www.databricks.com/spark/jdbc-drivers-download
# and place it at docker/readers/databricks/drivers/DatabricksJDBC42.jar
# before building this image.
COPY docker/readers/databricks/drivers/ /opt/spark-jars/

RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
    && curl -fsSL -o delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
    && curl -fsSL -o delta-storage-${DELTA_VERSION}.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    && test -f /opt/spark-jars/DatabricksJDBC42.jar || { \
        echo "ERROR: DatabricksJDBC42.jar not found -- download it manually (EULA-gated) and place it in docker/readers/databricks/drivers/ before building" >&2; exit 1; \
    } \
    && for f in /opt/spark-jars/*.jar; do \
        test -s "$f" || { echo "Missing/empty jar: $f" >&2; exit 1; }; \
    done \
    && echo "databricks reader image: delta=${DELTA_VERSION}" \
    && chown -R ingestion:ingestion /opt/spark-jars

RUN /opt/venv/bin/pip install --no-cache-dir databricks-sql-connector==3.4.0

USER ingestion
