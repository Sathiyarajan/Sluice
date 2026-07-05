ARG BASE_IMAGE=ingestion-framework/base:latest
FROM ${BASE_IMAGE}

USER root

# Must match the Hadoop version PySpark 3.5.1 was built against (3.3.4).
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
    && curl -fsSL -o hadoop-aws-${HADOOP_VERSION}.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    && curl -fsSL -o aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    && for f in /opt/spark-jars/*.jar; do \
        test -s "$f" || { echo "Missing/empty jar: $f" >&2; exit 1; }; \
    done \
    && echo "s3 reader image: hadoop-aws=${HADOOP_VERSION} aws-sdk-bundle=${AWS_SDK_VERSION}" \
    && chown -R ingestion:ingestion /opt/spark-jars

USER ingestion
