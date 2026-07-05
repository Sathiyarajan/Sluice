# A single pipeline run needs the source connector's jars AND every target
# connector's jars on the same Spark classpath at once -- per-connector images
# are for separate Jobs/CronJobs in prod, but local end-to-end testing needs
# both merged into one runtime. This combines readers/s3 + writers/hive.
ARG READER_IMAGE=ingestion-framework/reader-s3:latest
ARG WRITER_IMAGE=ingestion-framework/writer-hive:latest

FROM ${READER_IMAGE} AS reader
FROM ${WRITER_IMAGE}

USER root

ARG DELTA_VERSION=3.2.0
ARG SCALA_VERSION=2.12

COPY --from=reader /opt/spark-jars/ /opt/spark-jars/
# spark-defaults.conf enables the Delta catalog/extensions unconditionally,
# so every local-e2e combination needs delta-spark on the classpath even
# when the target connector under test isn't Delta.
RUN curl -fsSL -o /opt/spark-jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar" \
    && curl -fsSL -o /opt/spark-jars/delta-storage-${DELTA_VERSION}.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
    && chown -R ingestion:ingestion /opt/spark-jars \
    && echo "local-e2e dev image jars: $(ls -1 /opt/spark-jars)"
USER ingestion
