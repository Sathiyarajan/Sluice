# docker/

Container images for the ingestion framework. One shared **base** image
(Python + PySpark + Delta + framework code) plus thin **reader**/**writer**
layers that add only the connector-specific JARs/libs on top. A **dev**
folder holds local-only compose tooling. Every reader/writer image is
combined with the base image at build time via a `BASE_IMAGE` build arg —
none of them are usable standalone.

```
docker/
├── base/                     shared runtime image (Python, PySpark, Delta, framework src)
├── readers/
│   ├── s3/                   Hadoop S3A + AWS SDK jars
│   ├── jdbc/                 Postgres/MySQL/Oracle JDBC drivers
│   ├── kafka/                Spark-Kafka connector + kafka-python
│   ├── hive/                 Hive JDBC/metastore/exec jars
│   └── databricks/           Delta + Databricks JDBC driver (EULA-gated)
├── writers/
│   ├── s3/                   same as readers/s3
│   ├── hive/                 same as readers/hive
│   ├── snowflake/             Spark-Snowflake connector + snowflake-connector-python
│   └── databricks/           same as readers/databricks
└── dev/
    ├── local-e2e/Dockerfile  merges one reader + one writer image for local testing
    ├── spark-defaults.conf   Spark config for the local-e2e / docker-compose stack
    └── hive-site.xml         Hive metastore client config for the same stack
```

## base/ — shared runtime image

**`Dockerfile`** — multi-stage build.

- **builder stage** (`eclipse-temurin:17-jdk-jammy`): installs
  `python${PYTHON_VERSION}` + `python${PYTHON_VERSION}-venv` (both required —
  `-venv` provides `ensurepip`, without it `python -m venv` fails with
  `ensurepip is not available`), creates `/opt/venv`, pip-installs:
  - `pyspark==${SPARK_VERSION}` (default `3.5.1`)
  - `delta-spark==${DELTA_VERSION}` (default `3.2.0`)
  - `pydantic>=2.5`, `pyyaml>=6.0`, `boto3`, `structlog`, `pytest`
- **final stage** (`eclipse-temurin:17-jre-jammy`, smaller — no JDK):
  installs only `python${PYTHON_VERSION}` + `curl`, creates a non-root
  `ingestion` user (uid/gid 1000), copies the venv from the builder stage,
  copies `src/` + `pyproject.toml` and installs the framework package
  editable (`pip install --no-deps -e .`), copies the three helper scripts
  below into `/usr/local/bin/`, and switches to `USER ingestion`.

  Build args: `PYTHON_VERSION` (3.11), `SPARK_VERSION` (3.5.1),
  `DELTA_VERSION` (3.2.0).

  Key env vars baked in: `PYSPARK_PYTHON=/opt/venv/bin/python3.11`,
  `SPARK_JARS_DIR=/opt/spark-jars` (empty in the base image — populated by
  reader/writer layers), `PYTHONUNBUFFERED=1`.

**`entrypoint.sh`** — the container's `ENTRYPOINT` in every derived image
(same script runs unmodified regardless of which connectors are baked in).

```
entrypoint.sh --config-path /configs/<dataset>.yaml [--dataset NAME] [--mode reader|writer|full]
```

- `--dataset` defaults to the config file's basename (without extension).
- `--mode full` (default): reads source + writes all configured targets.
- `--mode writer --target-type <databricks_delta|snowflake|hive|s3>`:
  restricts to one target.
- `--mode reader`: **not supported** by the underlying pipeline (it always
  reads then writes) — treated as a no-op equivalent to `full` with a
  warning printed to stderr.
- Builds a `--jars` list from every `*.jar` under `$SPARK_JARS_DIR`
  (default `/opt/spark-jars`) if any exist, then `exec`s
  `spark-submit --master ${SPARK_MASTER:-local[*]} --driver-memory
  ${SPARK_DRIVER_MEMORY:-2g} --executor-memory ${SPARK_EXECUTOR_MEMORY:-2g}
  ... /usr/local/bin/spark_entry.py <args>`. Running under `spark-submit`
  (not a bare `python3 -m`) is required for the jars and memory settings to
  actually take effect.

**`spark_entry.py`** — one-line shim: `spark-submit` needs a script path,
not a `python -m` module invocation, so this just forwards to
`ingestion.run_pipeline.main`.

**`healthcheck.sh`** — the `HEALTHCHECK` command (`--interval=30s
--timeout=10s --start-period=15s --retries=3`). Only checks that
`import pyspark, delta, ingestion.pipeline` succeeds — deliberately does
**not** start a SparkSession (too slow/heavy for a liveness probe); import
success is enough to catch a broken venv or missing classpath jars.

## readers/ and writers/

Every Dockerfile in these two trees follows the same shape:

```dockerfile
ARG BASE_IMAGE=ingestion-framework/base:latest
FROM ${BASE_IMAGE}
USER root
# ... download connector jars into /opt/spark-jars, chown to ingestion ...
USER ingestion
```

They are never built standalone — always with
`--build-arg BASE_IMAGE=<your-built-base-tag>`. Reader and writer images for
the *same* connector (e.g. `readers/s3` and `writers/s3`) are identical
content-wise; they're kept as separate Dockerfiles so reader-only and
writer-only production images can be built/tagged/scanned independently
even though today they'd produce the same jars.

| Path | Adds | Notable ARGs (defaults) |
|---|---|---|
| `readers/s3`, `writers/s3` | `hadoop-aws` + `aws-java-sdk-bundle` jars | `HADOOP_VERSION=3.3.4` (must match the Hadoop version PySpark 3.5.1 was built against), `AWS_SDK_VERSION=1.12.262` |
| `readers/jdbc` | Postgres/MySQL/Oracle JDBC jars, selected via `JDBC_DRIVERS` | `JDBC_DRIVERS=postgres,mysql` (comma-separated subset of `postgres,mysql,oracle`), `POSTGRES_DRIVER_VERSION=42.7.3`, `MYSQL_DRIVER_VERSION=8.4.0`, `ORACLE_DRIVER_VERSION=23.4.0.24.05`. **Requires `SHELL ["/bin/bash", "-c"]`** — the RUN block uses `IFS=',' read -ra` / arrays / `<<<`, which are bash-only and fail under Debian's default `/bin/sh` (dash) with `Syntax error: redirection unexpected`. |
| `readers/kafka` | `spark-sql-kafka-0-10`, `spark-token-provider-kafka-0-10`, `kafka-clients`, `commons-pool2` jars, plus `pip install kafka-python==2.0.2` | `SPARK_VERSION=3.5.1`, `SCALA_VERSION=2.12`, `KAFKA_CLIENTS_VERSION=3.5.1`, `COMMONS_POOL2_VERSION=2.12.0` |
| `readers/hive`, `writers/hive` | `hive-jdbc` (standalone), `hive-metastore`, `hive-exec` jars; declares `VOLUME ["/etc/hive/conf"]` | `HIVE_VERSION=2.3.9`, `HIVE_JDBC_VERSION=2.3.9` (matches the Hive 2.3.9 client APIs bundled in Spark 3.5.1). `core-site.xml`/`hive-site.xml` are **never baked into the image** — supplied at deploy time via a mounted ConfigMap/volume, since they differ per environment/cluster. |
| `writers/snowflake` | `spark-snowflake`, `snowflake-jdbc` jars, plus `pip install snowflake-connector-python==3.11.0` | `SCALA_VERSION=2.12`, `SPARK_SNOWFLAKE_VERSION=3.2.0-spark_3.5` (⚠ verify against `https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/maven-metadata.xml` before bumping — an earlier `2.16.0-spark_3.5` default 404'd; Maven doesn't publish every combination for every Spark line), `SNOWFLAKE_JDBC_VERSION=3.16.1` |
| `readers/databricks`, `writers/databricks` | `delta-spark`/`delta-storage` jars, plus a **manually downloaded** `DatabricksJDBC42.jar` copied from `drivers/`, plus `pip install databricks-sql-connector==3.4.0` | `DELTA_VERSION=3.2.0`, `SCALA_VERSION=2.12`. **Build fails on purpose** if `drivers/DatabricksJDBC42.jar` is absent — the Databricks JDBC driver is EULA-gated with no public Maven coordinate; download it from `https://www.databricks.com/spark/jdbc-drivers-download` and place it at `docker/{readers,writers}/databricks/drivers/DatabricksJDBC42.jar` before building. The `drivers/.gitkeep` files exist only so the (gitignored) directory is tracked. |

All jar-downloading `RUN` blocks end with a loop asserting every
`/opt/spark-jars/*.jar` is non-empty (`test -s "$f"`) — a partial/failed
download fails the build immediately instead of producing a silently broken
image.

## dev/ — local-only tooling

**`local-e2e/Dockerfile`** — merges exactly one reader image and one writer
image into a single runtime for local end-to-end testing (a single pipeline
run needs both the source connector's jars and the target connector's jars
on the same Spark classpath at once; in production these stay as separate
per-connector images used by separate Jobs/CronJobs).

```dockerfile
ARG READER_IMAGE=ingestion-framework/reader-s3:latest
ARG WRITER_IMAGE=ingestion-framework/writer-hive:latest
FROM ${READER_IMAGE} AS reader
FROM ${WRITER_IMAGE}
# copies /opt/spark-jars from the reader stage into the writer-based final image
```

Also downloads `delta-spark`/`delta-storage` jars directly (`DELTA_VERSION`
build arg, default `3.2.0`) — required because
`docker/dev/spark-defaults.conf` unconditionally enables
`io.delta.sql.DeltaSparkSessionExtension` / `DeltaCatalog` for **every**
local-e2e reader/writer combination, even ones (like s3+hive) that don't
target Delta themselves. Without these jars the Spark job fails at startup
with `ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog`.

Build (from repo root, so `src/`/`pyproject.toml` are in context):

```bash
export PATH="$PATH:/c/Program Files/Docker/Docker/resources/bin"
docker build -f docker/base/Dockerfile -t dataforge-base:local .
docker build -f docker/readers/s3/Dockerfile   --build-arg BASE_IMAGE=dataforge-base:local -t reader-s3:local .
docker build -f docker/writers/hive/Dockerfile --build-arg BASE_IMAGE=dataforge-base:local -t writer-hive:local .
docker build -f docker/dev/local-e2e/Dockerfile \
  --build-arg READER_IMAGE=reader-s3:local \
  --build-arg WRITER_IMAGE=writer-hive:local \
  -t local-e2e:local .
```

(the top-level `docker-compose.yaml` does this same build for you via its
`runner` service, tagging the result `sluice-runner:latest` — see the repo
root README/compose file for the full local stack.)

**`spark-defaults.conf`** — mounted into every local-e2e/compose container at
`/opt/venv/lib/python3.11/site-packages/pyspark/conf/spark-defaults.conf`.
Configures: MinIO as the S3A endpoint (`http://minio:9000`, path-style
access, SSL off, `SimpleAWSCredentialsProvider`), `spark.sql.warehouse.dir
/tmp/spark-warehouse`, Hive catalog implementation, and the Delta
extension/catalog (see above — needed even for non-Delta local runs).

**`hive-site.xml`** — mounted at `/etc/hive/conf/hive-site.xml`. Just one
property: `hive.metastore.uris = thrift://hive-metastore:9083`. The hostname
`hive-metastore` must resolve to wherever the metastore is actually running
(the docker-compose service of that name, or a Kubernetes Service of that
name in the same namespace — see `k8s/dev/local-e2e/hive-metastore.yaml`).

## Build order / dependency graph

```
base  →  readers/*, writers/*  →  dev/local-e2e (needs one reader + one writer tag)
```

Nothing under `readers/`, `writers/`, or `dev/` builds without a
`BASE_IMAGE`/`READER_IMAGE`/`WRITER_IMAGE` pointing at an already-built
image — there is no default registry these fall back to that will resolve
outside of a real CI/registry setup; for local work always build `base`
first and pass explicit `--build-arg`s.

## Known local-environment gaps

- **Databricks reader/writer images cannot be built** without manually
  downloading `DatabricksJDBC42.jar` (EULA-gated, no public Maven
  coordinate) into `docker/{readers,writers}/databricks/drivers/`. This is
  a hard `RUN` failure by design, not a bug.
- Pinned third-party versions (base image tags, Maven artifact versions)
  can go stale — `docker/writers/snowflake`'s `SPARK_SNOWFLAKE_VERSION` and
  `docker-compose.yaml`'s `minio/mc` tag have both already needed bumping
  once after upstream removed the previously-pinned version. If a jar
  download 404s, check the artifact's `maven-metadata.xml` for the current
  version before assuming the Dockerfile itself is wrong.
