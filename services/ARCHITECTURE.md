# Milestone 2 architecture

The system runs on one K3s cluster. The Aggregator owns canonical storage and
query logic; the Client-Facing API remains a separate service. P1 storage is
lean Citus without Patroni: Citus shard replication factor 2 provides the
durable copies, and the coordinator remains the single SQL entry point.

## Deployment

Nodes are identified by role labels, never by hostname or pod IP:

```text
bnpb.io/role=control   coordinator, application coordinator
bnpb.io/role=app       application workloads, Citus worker 1/3
bnpb.io/role=store     Citus worker 2, observability
bnpb.io/app-eligible   true on app/store; false on control
```

No custom taint is applied, so the Tester Agent can run on every node.
`scripts/label-nodes.sh` applies the role/label contract. Manifests use
`local-path` dynamic PVCs, so no hostPath or device-specific path is embedded.

```text
Browser / CLI --> Client-Facing API --> Aggregator replicas --> canonical-api --> Citus
                         |                    |
                        Auth           poll BMKG/PVMBG/Sensor
                                              |
                                 event + transactional outbox
                                              |
                                      publish to Broker
                                              |
                                   Consumers / notifications

Field reports --> durable ingest --> Broker --> Consumer --> Aggregator API
Satellite feed --> Stream processor --> Aggregator API
Analytics --> Client-Facing API --> Aggregator --> distributed SQL
```

`services/bnpb` is the Aggregator. Its internal modules handle storage, queries,
audit and outbox publishing. Other services use authenticated Aggregator APIs.

| Team role | Responsibility |
| --- | --- |
| Data/platform (A) | Aggregator storage/query modules; Citus placement and recovery; audit; analytics; deployment and telemetry. |
| Aggregator/resilience (B) | Pollers and ownership authority; Aggregator routing, replicas and entry point; sensor resilience, priority notifications, load tests and game day. |
| Ingest/stream (C) | Broker/pipeline-state durability; raw envelopes, DLQ/replay, durable field reports, satellite checkpoints and audit unprocessed. |

## Storage and partitioning

Citus provides hash sharding, SQL routing, distributed queries and rebalancing.
The coordinator and three workers are plain PostgreSQL/Citus instances in
StatefulSets. Application credentials use `canonical-api`; the schema job uses a
separate administrator DSN.

The distribution column is `event_key`, the JSON-encoded exact
`[source, source_ref_id]` pair. `events` and `outbox` share **12 hash shards**
and replication factor **2**, giving 24 placements per table (48 total). The
default is intentionally portable; it can be raised after capacity testing.

The Aggregator's legacy `List` path uses one sargable query with expression
indexes. The `/v1/events` path keeps cursor traversal so partial shard coverage
is explicit. Reads use a short-lived singleflight cache, background freshness
refresh, stale-on-error and a bounded read bulkhead.

Citus owns placement metadata; there is no application hash ring. Register a
new worker with its Service name, then rebalance. A Citus worker is a PostgreSQL
instance, distinct from a Kubernetes worker node.

## Replication and recovery

Three workers plus the coordinator store the shards. With RF2 every shard has
two active placements, normally on different nodes. This is **asynchronous
Citus replication**, not a synchronous Patroni quorum.

| State | Write acknowledgement |
| --- | --- |
| Healthy | Commit routed by Citus with two active placements per shard. |
| Worker unavailable | After explicit `citus_disable_node`, writes may continue in `single_copy`; the adapter reports reduced durability. |
| Worker recovering | Reactivate the node, restore `replicated` policy, then rebalance and verify 48 placements. |

The administrator controls the `canonical.recovery` row. Writers hold a shared
row lock through commit, so a durability transition cannot race a mutation.
Single-copy commits cannot survive loss of the remaining copy before repair.

The coordinator is a single logical entry point and therefore not HA. Losing
the control node requires restoring the coordinator PVC; this is a deliberate
lean-deployment trade-off. Worker loss is handled by Citus placement failover.

## Operations

```bash
kubectl apply -k deploy
kubectl -n sistem-informasi-kebencanaan-bnpb wait \
  --for=condition=complete job/citus-init --timeout=300s
```

Service DNS is fixed: `citus-coordinator`, `citus-worker-1`,
`citus-worker-2`, `citus-worker-3`, `canonical-api` and `canonical-store`.
`citus-worker-3` starts at zero replicas and is scaled for node-addition drills.
P1 analytics are served by `canonical-api:8080/analytics`; the page uses the
Audit cursor path and carries a `partial` marker. Grafana remains metrics,
traces and logs only, with no canonical database credential.

References: [distributed tables](https://docs.citusdata.com/en/stable/develop/reference_ddl.html),
[node properties](https://docs.citusdata.com/en/stable/develop/api_udf.html#citus-set-node-property),
[cluster management](https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html).
