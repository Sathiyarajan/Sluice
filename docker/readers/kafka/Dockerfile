ARG BASE_IMAGE=ingestion-framework/base:latest
FROM ${BASE_IMAGE}

USER root

ARG SPARK_VERSION=3.5.1
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.5.1
ARG COMMONS_POOL2_VERSION=2.12.0

RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
    && curl -fsSL -o spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fsSL -o spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    && curl -fsSL -o kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
    && curl -fsSL -o commons-pool2-${COMMONS_POOL2_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
    && for f in /opt/spark-jars/*.jar; do \
        test -s "$f" || { echo "Missing/empty jar: $f" >&2; exit 1; }; \
    done \
    && echo "kafka reader image: kafka-clients=${KAFKA_CLIENTS_VERSION} spark-sql-kafka=${SPARK_VERSION}" \
    && chown -R ingestion:ingestion /opt/spark-jars

RUN /opt/venv/bin/pip install --no-cache-dir kafka-python==2.0.2

USER ingestion
