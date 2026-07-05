ARG BASE_IMAGE=ingestion-framework/base:latest
FROM ${BASE_IMAGE}

USER root

# Spark 3.5.1 bundles Hive 2.3.9 client APIs; match the metastore client version.
ARG HIVE_VERSION=2.3.9
ARG HIVE_JDBC_VERSION=2.3.9

RUN mkdir -p /opt/spark-jars /etc/hive/conf && cd /opt/spark-jars \
    && curl -fsSL -o hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar \
        "https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/${HIVE_JDBC_VERSION}/hive-jdbc-${HIVE_JDBC_VERSION}-standalone.jar" \
    && curl -fsSL -o hive-metastore-${HIVE_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/hive/hive-metastore/${HIVE_VERSION}/hive-metastore-${HIVE_VERSION}.jar" \
    && curl -fsSL -o hive-exec-${HIVE_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/hive/hive-exec/${HIVE_VERSION}/hive-exec-${HIVE_VERSION}.jar" \
    && for f in /opt/spark-jars/*.jar; do \
        test -s "$f" || { echo "Missing/empty jar: $f" >&2; exit 1; }; \
    done \
    && echo "hive writer image: hive=${HIVE_VERSION}" \
    && chown -R ingestion:ingestion /opt/spark-jars /etc/hive/conf

# core-site.xml / hive-site.xml are supplied at deploy time via ConfigMap mount,
# never baked into the image (differ per environment/cluster).
VOLUME ["/etc/hive/conf"]

USER ingestion
