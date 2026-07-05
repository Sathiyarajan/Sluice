ARG BASE_IMAGE=ingestion-framework/base:latest
FROM ${BASE_IMAGE}

USER root

SHELL ["/bin/bash", "-c"]

# Comma-separated subset of: postgres,mysql,oracle
ARG JDBC_DRIVERS=postgres,mysql
ARG POSTGRES_DRIVER_VERSION=42.7.3
ARG MYSQL_DRIVER_VERSION=8.4.0
ARG ORACLE_DRIVER_VERSION=23.4.0.24.05

RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars \
    && IFS=',' read -ra DRIVERS <<< "${JDBC_DRIVERS}" \
    && for d in "${DRIVERS[@]}"; do \
        case "$d" in \
            postgres) \
                curl -fsSL -o postgresql-${POSTGRES_DRIVER_VERSION}.jar \
                    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar" ;; \
            mysql) \
                curl -fsSL -o mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar \
                    "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_DRIVER_VERSION}/mysql-connector-j-${MYSQL_DRIVER_VERSION}.jar" ;; \
            oracle) \
                curl -fsSL -o ojdbc11-${ORACLE_DRIVER_VERSION}.jar \
                    "https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/${ORACLE_DRIVER_VERSION}/ojdbc11-${ORACLE_DRIVER_VERSION}.jar" ;; \
            *) echo "Unknown JDBC driver requested: $d" >&2; exit 1 ;; \
        esac; \
    done \
    && for f in /opt/spark-jars/*.jar; do \
        test -s "$f" || { echo "Missing/empty jar: $f" >&2; exit 1; }; \
    done \
    && echo "jdbc reader image: drivers=${JDBC_DRIVERS} $(ls -1 /opt/spark-jars)" \
    && chown -R ingestion:ingestion /opt/spark-jars

USER ingestion
