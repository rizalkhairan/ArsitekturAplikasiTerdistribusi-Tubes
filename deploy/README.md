# Deployment

The deployable baseline is `deploy/kustomization.yaml`. It includes the
application workloads, the lean Citus storage, the `canonical-api` adapter and
the one-shot `citus-init` Job.

## Prerequisites

- A working three-node k3s cluster with the `local-path` StorageClass.
- `kubectl` with Kustomize, Docker on the build machine, and SSH access to the
  k3s nodes if images are imported manually.
- No node name, pod IP or host path is embedded in the manifests.

## Node contract

Run the portable label script once per cluster:

```bash
bash scripts/label-nodes.sh <control-node> <app-node> <store-node>
```

It applies:

```text
bnpb.io/role=control|app|store
bnpb.io/app-eligible=true on app and store
app-eligible/store-eligible/aggregator-eligible compatibility labels
```

No custom taint is applied: the Tester Agent must be able to run on every node.
The script also creates `citus-node-map`, which the Audit API uses to translate
Citus Service names into the Kubernetes node names expected by
`/internal/audit/location`.

Citus placement:

```text
citus-coordinator  role=control
citus-worker-1     role=app
citus-worker-2     role=store
citus-worker-3     role=app, replicas=0 (used for node-addition tests)
```

## Images and secrets

```bash
TAG=m2 scripts/build-images.sh
# import/push every image referenced by deploy/, including
# docker.io/citusdata/citus:13.0.1-pg16
bash scripts/citus-secrets.sh
```

`bash scripts/citus-secrets.sh` creates:

- `bnpb-canonical-cluster` — PostgreSQL superuser and application passwords.
- `bnpb-citus-admin` — schema/admin DSN.
- `bnpb-citus-api` — adapter DSN using `canonical_app`.

The values come from the environment or from the private
`deploy/k3s/.canonical.env` file. If that file does not exist, export
`CANONICAL_APP_PASSWORD` and `POSTGRES_PASSWORD` (or
`PATRONI_SUPERUSER_PASSWORD`) before running the script.

## Deploy

```bash
bash scripts/deploy.sh <control-node> <app-node> <store-node>
```

This labels the nodes, creates the Citus secrets, applies `deploy/`, and waits
for `citus-init` plus the adapter/Aggregator rollouts. The equivalent manual
commands are:

```bash
kubectl apply -k deploy
kubectl -n sistem-informasi-kebencanaan-bnpb wait \
  --for=condition=complete job/citus-init --timeout=300s
kubectl -n sistem-informasi-kebencanaan-bnpb rollout status \
  deployment/canonical-api deployment/aggregator --timeout=180s
```

Service names are the only storage addresses used by workloads:

```text
citus-coordinator:5432
citus-worker-1:5432
citus-worker-2:5432
citus-worker-3:5432
canonical-api:8080
canonical-store:8080   # compatibility alias
```

`citus-init` waits for the coordinator and workers, creates the database,
role and Citus extension, registers nodes by Service name, and then runs the
embedded `canonical-admin migrate` schema.

## Verify storage

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator-0 -- \
  psql -h 127.0.0.1 -U postgres -d canonical -At <<'SQL'
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='canonical.events'::regclass;
SELECT count(*) FROM pg_dist_placement p JOIN pg_dist_node n USING(groupid)
 WHERE n.isactive AND p.shardstate=1;
SELECT canonical.replication_ready(), canonical.primaries_ready();
SQL
```

Expected: **12 shards, 48 active placements, `t|t`** with replication factor 2.

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/canonical-api 18080:8080
curl -s 'http://127.0.0.1:18080/v1/events?limit=5' | jq '{items:(.items|length),next_cursor,partial}'
```

## Failure and node-addition drills

Temporarily remove a worker and keep writes available with a single copy:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb scale statefulset citus-worker-2 --replicas=0
kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator-0 -- \
  psql -h 127.0.0.1 -U postgres -d canonical -c \
  "SELECT citus_disable_node('citus-worker-2',5432,synchronous:=true);
   UPDATE canonical.recovery
      SET durability='single_copy', fenced_node='citus-worker-2'
    WHERE singleton;"
```

Restore it and return to replicated mode:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb scale statefulset citus-worker-2 --replicas=1
kubectl -n sistem-informasi-kebencanaan-bnpb exec citus-coordinator-0 -- \
  psql -h 127.0.0.1 -U postgres -d canonical -c \
  "SELECT citus_activate_node('citus-worker-2',5432);
   UPDATE canonical.recovery SET durability='replicated', fenced_node=NULL WHERE singleton;"
```

Add a fourth worker:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb scale statefulset citus-worker-3 --replicas=1
kubectl -n sistem-informasi-kebencanaan-bnpb wait \
  --for=condition=ready pod/citus-worker-3-0 --timeout=180s
kubectl -n sistem-informasi-kebencanaan-bnpb delete job citus-init --ignore-not-found
kubectl apply -f deploy/kubernetes/base/citus-init.yaml
kubectl -n sistem-informasi-kebencanaan-bnpb wait \
  --for=condition=complete job/citus-init --timeout=300s
```

Then rebalance/verify the placement count and keep replication factor 2.

## Observability

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb get pods,svc,pvc -o wide
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/grafana 3000:3000
```

P1 analytics are served by the storage owner at `canonical-api:8080/analytics`.
The page reads `/internal/analytics/summary`, which aggregates through the same
Citus cursor path as the Audit API and shows a clear `partial` banner when a
worker is unreachable. Open it with:

```bash
kubectl -n sistem-informasi-kebencanaan-bnpb port-forward svc/canonical-api 18080:8080
# http://127.0.0.1:18080/analytics
```

Grafana remains the metrics/traces/logs dashboard and has no direct database
credential.

## Legacy tooling

`deploy/k3s/manage.py` remains for cluster provisioning and image-bundle
helpers, but the supported deployment path is the Kustomize baseline above.
The old Patroni/JSON canonical-store manifests and recovery tooling have been
removed.
