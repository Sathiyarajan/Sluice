# k8s/

Kustomize-based Kubernetes manifests for running the ingestion framework as
a one-shot `Job` (backfill/manual run) or a scheduled `CronJob` (recurring
ingestion). `base/` holds the generic, placeholder-filled templates;
`jobs/`, `cronjobs/`, `configmaps/`, `secrets/` are overlays/resources that
patch those placeholders into a concrete, runnable dataset. `dev/local-e2e/`
is a self-contained local substitute stack (MinIO + Hive metastore + a
runnable Job/CronJob) for testing on `kind` without any real cloud
credentials or a Databricks endpoint.

```
k8s/
├── kind-config.yaml                 kind cluster config (NodePort mappings for local dev)
├── base/                            generic Job/CronJob/ServiceAccount templates (never applied standalone)
├── configmaps/                      dataset pipeline-config ConfigMap(s)
├── secrets/                         ExternalSecret CRs (+ a local-dev SecretStore/RBAC substitute)
├── jobs/customer-orders/            overlay: base -> a concrete one-shot Job
├── cronjobs/customer-orders/        overlay: base -> a concrete scheduled CronJob
└── dev/local-e2e/                   local-only MinIO+Hive+Job+CronJob stack for kind
```

## kind-config.yaml

`kind create cluster --config k8s/kind-config.yaml` config. Maps 3 host
ports into the control-plane node's `extraPortMappings` so services exposed
via NodePort are reachable from the host without an Ingress:

| Host port | Container port | For |
|---|---|---|
| 9000 | 30900 | MinIO API |
| 9001 | 30901 | MinIO console |
| 4040 | 30040 | Spark UI |

⚠ These host ports collide with `docker-compose.yaml`'s MinIO ports (also
9000/9001) — **don't run the kind cluster and the docker-compose stack at
the same time**; `kind create cluster` will hold the ports even if nothing
in the cluster is using them yet, and `docker compose up` will fail with
`port is already allocated`. Tear one down before starting the other.

## base/ — generic templates (not standalone-applicable)

**`kustomization.yaml`** — lists `job.yaml`, `cronjob.yaml`,
`serviceaccount.yaml` as resources. All three must be listed here (not just
the ones a given overlay's patches target) — see the "why base lists both
Job and CronJob" note under `jobs/`/`cronjobs/` below.

**`serviceaccount.yaml`** — a single `ServiceAccount/ingestion-runner`, used
by both the Job and CronJob pod specs and by the local-dev `SecretStore`'s
`kubernetes` provider auth (see `secrets/local-dev-secretstore.yaml`).

**`job.yaml`** — a `batch/v1 Job` named `ingestion-job` with everywhere a
concrete deployment must fill in a placeholder:

| Placeholder | Filled in by | Meaning |
|---|---|---|
| `REPLACE_ME_IMAGE` | overlay patch | which reader+writer image to run |
| `REPLACE_ME_CONFIG_FILE` | overlay patch | dataset config filename under `/configs` |
| `REPLACE_ME_SECRET` | overlay patch | which Secret to `envFrom` (credentials) |
| `REPLACE_ME_CONFIGMAP` | overlay patch | which ConfigMap to mount at `/configs` |

Fixed shape: `restartPolicy: Never`, `serviceAccountName: ingestion-runner`,
`backoffLimit: 2`, `activeDeadlineSeconds: 3600`, resources
`requests{cpu:2,memory:6Gi}` / `limits{cpu:4,memory:8Gi}`, env
`SPARK_MASTER=local[*]`, `SPARK_DRIVER_MEMORY=4g`,
`SPARK_EXECUTOR_MEMORY=4g` (all overridable via JSON6902 patches in an
overlay if a dataset needs different sizing).

**`cronjob.yaml`** — same shape as `job.yaml` but wrapped in a
`batch/v1 CronJob` (`ingestion-cronjob`), adding `schedule: "0 6 * * *"`,
`concurrencyPolicy: Forbid`, `successfulJobsHistoryLimit: 3`,
`failedJobsHistoryLimit: 3`, `startingDeadlineSeconds: 300`.

**Applying `k8s/base` directly will fail** —
`kubectl apply -k k8s/base` errors on both the Job and CronJob with
`Invalid value: "REPLACE_ME_SECRET": a lowercase RFC 1123 subdomain must
consist of..."`. This is expected: `base` is a template only meant to be
consumed through an overlay (`jobs/*`, `cronjobs/*`) that patches every
`REPLACE_ME_*` placeholder to a real value. The ServiceAccount alone is
harmless/idempotent to apply standalone if you need it created early (e.g.
before `secrets/local-dev-secretstore.yaml`'s RBAC binding).

## Why every overlay lists `../../base` (not `../../base/job.yaml`)

Kustomize v5's load restrictor rejects resource entries that point at a
file **outside** the current kustomization's own directory tree — e.g.
`resources: [../../configmaps/customer-orders-config.yaml]` from
`jobs/customer-orders/` fails with:

```
security; file '.../k8s/configmaps/customer-orders-config.yaml' is not in or
below '.../k8s/jobs/customer-orders': must build at directory
```

The fix used throughout this repo: reference the **sibling kustomization
directory** (`../../configmaps`, `../../secrets`, `../../base`), which has
its own `kustomization.yaml` and is a legal cross-reference, instead of a
single file inside it. This is also why `base/kustomization.yaml` had to
list `cronjob.yaml` even before any overlay used it as a standalone
resource: once `cronjobs/customer-orders` needed to reference `../../base`
as a whole (rather than the illegal `../../base/cronjob.yaml`), the Job
*and* CronJob both got pulled into every overlay that references `base` —
see the next section for why each overlay then deletes the one it doesn't
want.

## jobs/customer-orders/ and cronjobs/customer-orders/

Both overlays have the identical shape:

```yaml
resources:
  - ../../base
  - ../../configmaps
  - ../../secrets
patches:
  - patch: |-
      $patch: delete
      apiVersion: batch/v1
      kind: <the other kind>       # CronJob in jobs/, Job in cronjobs/
      metadata:
        name: <the other kind's base name>
  - target:
      kind: <Job|CronJob>
      name: ingestion-<job|cronjob>
    patch: |-
      # JSON6902 ops filling every REPLACE_ME_* placeholder
```

The `$patch: delete` entry is **required**, not cosmetic: because
`../../base` now contains both `job.yaml` and `cronjob.yaml`, without it
each overlay would render the *other* kind unpatched, `REPLACE_ME_*`
placeholders and all, alongside the correctly patched one — silently
producing a second, broken resource on `kubectl apply`.

The `target:`-scoped JSON6902 patch fills in the production values for the
`customer_orders` dataset:

| Path | Value |
|---|---|
| `metadata/name` | `customer-orders-run` (Job) / `customer-orders-cron` (CronJob) |
| `.../containers/0/image` | `REPLACE_ME_REGISTRY/reader-s3-writer-databricks:latest` — **replace with your real registry/tag before applying to a real cluster**; this is never resolved by kustomize itself |
| `.../containers/0/args/1` | `/configs/customer_orders.yaml` |
| `.../containers/0/envFrom/0/secretRef/name` | `databricks-credentials` (replaces the base's `REPLACE_ME_SECRET`) |
| `.../containers/0/envFrom/-` (added) | a second `secretRef: s3-credentials` |
| `.../volumes/0/configMap/name` | `customer-orders-config` |
| CronJob only: `spec/schedule` | `0 6 * * *` |

**This overlay targets Databricks and cannot be run against the local `kind`
cluster** — the image name implies a `reader-s3-writer-databricks` image,
which needs the EULA-gated `DatabricksJDBC42.jar` (see `docker/README.md`)
and a real Databricks workspace/token. `kubectl apply -k
k8s/jobs/customer-orders --dry-run=client` (structural validation) passes;
actually running it does not, locally. For a local, fully runnable
equivalent of the same read → transform → write flow, see `dev/local-e2e/`
below.

## configmaps/

**`kustomization.yaml`**: single resource, `customer-orders-config.yaml`.

**`customer-orders-config.yaml`**: a `ConfigMap/customer-orders-config`
holding one key, `customer_orders.yaml`, whose value is the actual
ingestion-framework dataset config (production shape):

- `source.type: s3_parquet`, path `s3a://raw-bucket/customer_orders/`,
  schema `order_id/customer_id/amount/updated_at`, watermark column
  `updated_at`
- `targets: [{type: databricks_delta, write_mode: merge, merge_keys:
  [order_id], options.table: analytics.customer_orders, schema_evolution:
  true}]`
- `retry`: 3 attempts, exponential backoff (5s initial, ×2 multiplier, 120s
  cap)
- `sla.max_runtime_minutes: 45`, alert channels (`slack:#data-eng-alerts`,
  `email:oncall-data-eng@example.com`)
- `data_quality`: enabled, `min_row_count: 1`, `max_null_fraction` per
  column, `fail_pipeline_on_violation: true`

This is mounted at `/configs/customer_orders.yaml` in the pod by the
`jobs`/`cronjobs` overlays (see `dataset-config` volume in `base/job.yaml`).

## secrets/

**`kustomization.yaml`** lists the 4 real `ExternalSecret` CRs plus (new,
local-only) `local-dev-secretstore.yaml`, then patches every
`ExternalSecret`'s `secretStoreRef.name` from the checked-in
`REPLACE_ME_SECRET_STORE` placeholder to `local-dev-store`, and each one's
`remoteRef.key`/`property` to point at the local dummy secret's keys
instead of a real vault path. **Revert or override these patches before
pointing this overlay at a real SecretStore** — they exist purely so
`kubectl apply -k k8s/secrets` produces synced secrets on a bare local
cluster with no Vault/AWS Secrets Manager/etc. configured.

**`s3-credentials-external-secret.yaml`**,
**`jdbc-credentials-external-secret.yaml`**,
**`databricks-credentials-external-secret.yaml`**,
**`snowflake-credentials-external-secret.yaml`** — each an
`external-secrets.io/v1 ExternalSecret` (⚠ **not** `v1beta1` — the current
External Secrets Operator Helm chart only serves `v1`; `v1beta1` manifests
fail `kubectl apply` with `no matches for kind "ExternalSecret" in version
"external-secrets.io/v1beta1"`, "ensure CRDs are installed first" — that
error means an apiVersion mismatch, not a missing operator, if the operator
is in fact installed). Each declares `refreshInterval: 1h`,
`secretStoreRef.kind: SecretStore`, `target.creationPolicy: Owner`, and a
`data[]` list mapping `secretKey` (the key inside the resulting `Secret`,
e.g. `AWS_ACCESS_KEY_ID`) to a `remoteRef.key`/`property` in the backing
store. Requires the [External Secrets
Operator](https://external-secrets.io) installed on the cluster — install
via:

```bash
helm repo add external-secrets https://charts.external-secrets.io
helm repo update
helm install external-secrets external-secrets/external-secrets \
  --namespace external-secrets --create-namespace --wait
```

Never commit real plaintext credentials into these files — they only
reference *where* the real secret lives in an external store; the actual
value comes from whatever `SecretStore`/`ClusterSecretStore` backend is
configured for the target environment (Vault, AWS Secrets Manager, etc.,
via `secretStoreRef.name`, pointed at `REPLACE_ME_SECRET_STORE` here).

**`local-dev-secretstore.yaml`** *(local/dev only — not a production
manifest)* — three resources that make the above `ExternalSecret`s
actually sync on a bare local cluster with no real secret backend:

1. `Secret/local-dummy-creds` — MinIO admin creds
   (`minioadmin`/`minioadmin`, matching the docker-compose/`dev/local-e2e`
   fixtures) plus placeholder JDBC/Snowflake/Databricks values
   (`local-dev-password`, `http://localhost`, `local-dev-token`) — **never
   real credentials**.
2. `Role/local-dev-secret-reader` + `RoleBinding` — grants
   `get/list/watch` on `secrets` to the `ingestion-runner` ServiceAccount.
   **Required**: without this the `SecretStore` reports `Ready: False`,
   `InvalidProviderConfig`, `"client is not allowed to get secrets"` — the
   `kubernetes` provider needs RBAC just like any other client of the API
   server.
3. `SecretStore/local-dev-store` — provider `kubernetes`, `remoteNamespace:
   default`, authenticating as `ingestion-runner` via
   `auth.serviceAccount`, with `server.caProvider` pointed at the
   automatically-present `kube-root-ca.crt` ConfigMap.

After applying (`kubectl apply -k k8s/secrets`), verify sync with:

```bash
kubectl get externalsecrets           # STATUS should read SecretSynced, READY True
kubectl get secrets s3-credentials jdbc-credentials snowflake-credentials databricks-credentials
```

If `STATUS` stays `SecretSyncedError` right after apply, give it a few
seconds — the operator polls/retries; `describe externalsecret <name>`
shows the exact provider-side error.

## dev/local-e2e/ — local-only runnable substitute for the Databricks overlay

Because `jobs/customer-orders` and `cronjobs/customer-orders` need a
Databricks endpoint and driver that aren't available locally, this
directory provides a **fully local, fully runnable** equivalent of the same
read → transform → write shape (MinIO/S3 CSV source → Hive target),
mirroring the `docker-compose.yaml` stack but running in-cluster on `kind`.
Nothing here is meant to be reused in a real deployment — it exists only
for local smoke-testing.

**`kustomization.yaml`**: resources `minio.yaml`, `hive-metastore.yaml`,
`job.yaml` (does **not** include `cronjob.yaml` — apply that one
separately, see below).

**`minio.yaml`** — `Deployment/minio` (image
`minio/minio:RELEASE.2024-06-13T22-53-53Z`, `server /data
--console-address :9001`, root creds `minioadmin`/`minioadmin`,
`readinessProbe` on `/minio/health/live:9000`) + `Service/minio` (ports
9000 api / 9001 console) + `Job/minio-init` (image
`minio/mc:RELEASE.2025-08-13T08-35-41Z-cpuv1` — ⚠ the older
`RELEASE.2024-06-13T22-53-53Z` tag this originally used **no longer exists
on Docker Hub**, upstream retagged/pruned it; if a pull 404s on this image,
check current tags via the Docker Hub API before assuming the manifest is
wrong). The init Job retries `mc alias set` in a loop (`until mc alias
set ...; do sleep 2; done`) since MinIO may not be ready yet when the Job
starts — seeing a few `connection refused` lines in its logs before it
succeeds is expected, not a failure. Once connected it creates
`raw-bucket/customer_orders` and seeds a one-row `sample.csv`
(`order_id,customer_id,amount,updated_at` / `1,c1,10.5,2024-01-01T00:00:00Z`).

**`hive-metastore.yaml`** — `Deployment/metastore-db` (`postgres:16-alpine`,
db `metastore`, user/pass `hive`/`hive`, `pg_isready` readiness probe) +
`Service/metastore-db` (5432) + `Deployment/hive-metastore` (image
`apache/hive:3.1.3`, `SERVICE_NAME=metastore`, JDBC connection string
pointed at `metastore-db:5432`) + `Service/hive-metastore` (9083).

Both Service names (`minio`, `hive-metastore`) are **deliberately identical**
to the docker-compose service names, so the existing
`docker/dev/spark-defaults.conf` (`s3a.endpoint = http://minio:9000`) and
`docker/dev/hive-site.xml` (`hive.metastore.uris =
thrift://hive-metastore:9083`) resolve correctly with **zero changes** — no
Kubernetes-specific endpoint rewriting needed, only the manifests
themselves are new.

**`job.yaml`** — three resources:

1. `ConfigMap/customer-orders-local-config` — a local-safe dataset config
   (`customer_orders_local.yaml` key): `source.type: s3_csv` (matches the
   CSV fixture `minio-init` seeds — using `s3_parquet` here fails with
   `CANNOT_READ_FILE_FOOTER: ... is not a Parquet file`, since the sample
   data is CSV, not Parquet), target `type: hive` (`write_mode: append`,
   `table: default.customer_orders_local`), `retry.max_attempts: 1`,
   `sla.max_runtime_minutes: 10`, `data_quality.fail_pipeline_on_violation:
   false`.
2. `ConfigMap/local-e2e-spark-conf` — inlines the same
   `spark-defaults.conf`/`hive-site.xml` content described in
   `docker/README.md`'s `dev/` section, so they can be mounted via
   `subPath` without a `docker/dev` bind-mount (not possible from inside a
   pod).
3. `Job/customer-orders-local-run` — runs image `sluice-runner:latest`
   (the merged reader-s3 + writer-hive + delta image built by
   `docker-compose.yaml`'s `runner` service / `docker/dev/local-e2e/Dockerfile`
   — must be `kind load docker-image sluice-runner:latest --name
   <cluster>`'d in first, it is **not** pulled from a registry).
   `envFrom: [secretRef: s3-credentials]` reuses the already-synced Secret
   from `k8s/secrets` (MinIO admin creds) rather than duplicating them.
   Mounts the two ConfigMaps above at `/configs`,
   `.../pyspark/conf/spark-defaults.conf` (via `subPath:
   spark-defaults.conf`), and `/etc/hive/conf/hive-site.xml` (via `subPath:
   hive-site.xml`). `backoffLimit: 0`, `activeDeadlineSeconds: 600`,
   requests `cpu:1/memory:2Gi`, limits `cpu:2/memory:3Gi` — deliberately
   smaller than `base/job.yaml`'s production sizing, sized for a kind
   node.

**`cronjob.yaml`** *(applied separately — not in this dir's
`kustomization.yaml`)* — `CronJob/customer-orders-local-cron`, identical
container/volume shape to `job.yaml`'s Job, reusing the same two
ConfigMaps. Exists specifically to exercise `kubectl create job
--from=cronjob/...` locally, since the production `cronjobs/customer-orders`
overlay can't actually run here (Databricks, see above).

### Running the full local-e2e stack end to end

```bash
# 0. prerequisites already applied: k8s/base's ServiceAccount, k8s/secrets (for s3-credentials)
kind load docker-image sluice-runner:latest --name <your-cluster>

kubectl apply -f k8s/dev/local-e2e/minio.yaml
kubectl apply -f k8s/dev/local-e2e/hive-metastore.yaml
kubectl wait --for=condition=available deployment/minio deployment/metastore-db deployment/hive-metastore --timeout=120s
kubectl wait --for=condition=complete job/minio-init --timeout=60s

kubectl apply -f k8s/dev/local-e2e/job.yaml
kubectl wait --for=condition=complete job/customer-orders-local-run --timeout=180s
kubectl logs job/customer-orders-local-run   # expect: "pipeline_completed", row_count > 0, targets: ["hive"]

# exercising the CronJob path:
kubectl apply -f k8s/dev/local-e2e/cronjob.yaml
kubectl create job --from=cronjob/customer-orders-local-cron customer-orders-local-triggered
kubectl wait --for=condition=complete job/customer-orders-local-triggered --timeout=180s
```

Teardown: `kind delete cluster --name <your-cluster>` removes everything in
one shot (no need to `kubectl delete` individual resources first).

## Known local-environment gaps / things to double check before real use

- `jobs/customer-orders` and `cronjobs/customer-orders` are **production
  manifests targeting Databricks** — they pass `--dry-run=client` locally
  but cannot actually run without a real Databricks workspace/token and the
  EULA-gated JDBC driver baked into the image. Use `dev/local-e2e/` to
  smoke-test the framework itself locally.
- `REPLACE_ME_REGISTRY` in both overlays' image patches must be replaced
  with a real registry path before applying anywhere real — kustomize does
  not validate or resolve this for you.
- `k8s/secrets`'s `local-dev-store`/RBAC/dummy-Secret patches are
  local-only scaffolding. Before pointing this overlay at a real cluster,
  either remove `local-dev-secretstore.yaml` from
  `secrets/kustomization.yaml` and its patches, or override them with an
  overlay of your own that repoints `secretStoreRef.name` at your real
  Vault/AWS Secrets Manager `SecretStore`.
- Applying `k8s/base` directly is expected to fail on the Job/CronJob
  (placeholder validation) — this is not a bug, only overlays are meant to
  be applied.
- The kind cluster's NodePort mappings (9000/9001) and docker-compose's
  MinIO port mappings collide — don't run both at once.
