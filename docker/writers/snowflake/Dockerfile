ARG BASE_IMAGE=ingestion-framework/base:latest
FROM ${BASE_IMAGE}

USER root

ARG SCALA_VERSION=2.12
ARG SPARK_SNOWFLAKE_VERSION=3.2.0-spark_3.5
ARG SNOWFLAKE_JDBC_VERSION=3.16.1

RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
    && curl -fsSL -o spark-snowflake_${SCALA_VERSION}-${SPARK_SNOWFLAKE_VERSION}.jar \
        "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_${SCALA_VERSION}/${SPARK_SNOWFLAKE_VERSION}/spark-snowflake_${SCALA_VERSION}-${SPARK_SNOWFLAKE_VERSION}.jar" \
    && curl -fsSL -o snowflake-jdbc-${SNOWFLAKE_JDBC_VERSION}.jar \
        "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/${SNOWFLAKE_JDBC_VERSION}/snowflake-jdbc-${SNOWFLAKE_JDBC_VERSION}.jar" \
    && for f in /opt/spark-jars/*.jar; do \
        test -s "$f" || { echo "Missing/empty jar: $f" >&2; exit 1; }; \
    done \
    && echo "snowflake writer image: spark-snowflake=${SPARK_SNOWFLAKE_VERSION} jdbc=${SNOWFLAKE_JDBC_VERSION}" \
    && chown -R ingestion:ingestion /opt/spark-jars

RUN /opt/venv/bin/pip install --no-cache-dir snowflake-connector-python==3.11.0

USER ingestion
