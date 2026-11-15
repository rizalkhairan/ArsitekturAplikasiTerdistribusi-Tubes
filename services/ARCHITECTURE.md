# Milestone 2 architecture

**Design decisions; implementation and failure behavior remain to be validated.**
The system uses PostgreSQL/Citus with Patroni on one K3s cluster. The existing
Aggregator owns canonical storage and query logic; Client-Facing API remains
separate. Shared API contracts require agreement across the team.

## Deployment

Three Kubernetes nodes with separate kernels provide the baseline topology.
Names below identify logical roles; physical hosts, operating systems and VM
allocations depend on the deployment environment.

| Node | Role | Workloads |
| --- | --- | --- |
| `node-control` | K3s control plane + worker | Applications, datastore; preferred telemetry location |
| `node-app` | K3s worker | Applications and datastore |
| `node-store` | K3s worker | Datastore and one Aggregator |

Three Aggregators run initially, one per node, behind one entry point.
Control-plane/application nodes have `app-eligible=true`; all nodes have
`store-eligible=true` and `aggregator-eligible=true`. Enforce eligibility
with node selectors/affinity and separate each database primary/standby pair
across nodes. Aggregator placement must support the 3 → 5 → 2 test:
unconditional required Pod anti-affinity would prevent five replicas.

The storage node also runs necessary system components and the unchanged
Tester Agent. All locally managed services—including mocks, broker,
processors, sensor and telemetry—run in Kubernetes. The satellite feed is
external; clients and load generators access the cluster externally.

Use stable private node addresses and verify node and cross-node Pod
connectivity. Application namespace: `sistem-informasi-kebencanaan-bnpb`.
Apply the specification's role labels and resource declarations; install
the supplied Tester Agent unchanged on every node. Store data uses local
PVs/hostPath, with private directories per node.

Baseline destructive tests target worker-only nodes, retaining the control
plane. The specification permits broader scenarios with at least three nodes;
this design provides no control-plane HA. Physical-host placement is
specified in each deployment's topology declaration.

## Services and ownership

```text
Browser / CLI --> Client-Facing API --> Aggregator replicas --> PostgreSQL/Citus
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

`services/bnpb` remains the Aggregator; its internal modules handle storage,
queries, audit and outbox publishing. Only its application credentials access
canonical tables. Other services use authenticated Aggregator APIs.
Replication and Patroni use separate internal credentials. Enforce ownership
through credentials, network restrictions and private volumes.

| Team role | Responsibility |
| --- | --- |
| Data/platform (A) | Aggregator storage/query modules; replication/recovery integration; version/fencing enforcement; audit events/checksum/location; analytics; deployment, isolation and shared telemetry. |
| Aggregator/resilience (B) | Pollers and ownership authority; Aggregator server/routing, replicas and entry point; sensor resilience, priority notifications, load tests and game day. |
| Ingest/stream (C) | Broker/pipeline-state durability; raw envelopes, DLQ/replay, durable field reports, satellite processing/checkpoints and audit unprocessed. |

A/B agree fencing and poll-window commits; A/C agree versions, retractions,
retries, publisher ownership and outbox delivery. B integrates A's Audit
handlers and proxies C's unprocessed route. C retains private pipeline state
and uses Aggregator APIs for canonical mutations.

Telemetry provides service metrics, cross-service traces and correlated
logs. Use one instance per selected telemetry component with short retention;
tools and durations remain open. Prefer control-plane-node placement to
retain visibility through worker failures; telemetry has no HA guarantee.

## Storage and partitioning

Replace canonical SQLite with **PostgreSQL + Citus**. Citus provides hash
sharding, SQL routing, distributed queries and rebalancing; PostgreSQL
provides transactions and streaming replication; Patroni manages promotion
and Citus routing updates. Automation must handle replica rebuilding,
membership changes and rebalance operations.

SQLite offers code reuse and an embedded runtime but leaves distributed
replication, routing and recovery protocols to the application.

| Partitioning | Geographic skew | Trade-off |
| --- | --- | --- |
| Geographic/range by area | Regional bursts concentrate data/writes | Efficient regional targeting; hot areas need splitting/balancing. |
| Hash stable event identity — selected | Distinct events from a region spread across shards | Area/national queries span shards; updates to one event remain concentrated. |

Proposed distribution column: an unambiguous `event_key` derived from
`(source, source_ref_id)`. Include it in uniqueness constraints and colocate
event/outbox records where possible. Exact schema and shard count remain
open pending contracts and dataset sizing.

Citus owns placement metadata; no application hash ring. Register new data
workers and rebalance to redistribute existing shards. The coordinator also
stores assigned shards. A Citus worker is a PostgreSQL instance, distinct
from a Kubernetes worker node.

Validate coordinator data placement with Patroni's integration and pin
compatible PostgreSQL/Citus/Patroni versions.
References: [distributed tables](https://docs.citusdata.com/en/stable/develop/reference_ddl.html),
[node properties](https://docs.citusdata.com/en/stable/develop/api_udf.html#citus-set-node-property),
[cluster management](https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html).

## Replication and recovery

Physical replication copies an entire PostgreSQL instance. Three independent
data groups each have a primary and standby, giving **six PostgreSQL/Patroni
instances**, two per node:

| Group | Initial primary | Initial standby |
| --- | --- | --- |
| A — coordinator + assigned data | `node-control` | `node-app` |
| B — data | `node-app` | `node-store` |
| C — data | `node-store` | `node-control` |

Groups contain multiple shards. Each node holds two groups, not the full
dataset; either worker-only node can fail while a copy of every group survives. Validate
actual distribution and routing after promotion.

Run Patroni alongside PostgreSQL in each instance's container, using
Kubernetes as its distributed configuration store and its Citus integration.
No separate etcd or PostgreSQL monitor is selected. Coordination depends on
the control plane. Scope Patroni's API permissions and customize its
role label, e.g. `postgres-role`, to retain `role=store` for Tester Agent.

| State | Write acknowledgement |
| --- | --- |
| Healthy | Durable commit on primary and standby on different nodes. |
| Worker node unavailable | Continue after safe failover; retain replicated commits where possible and explicitly report single-copy/reduced redundancy. |
| Worker node recovering | Continue service, rebuild/catch up replicas and restore replicated acknowledgements. |

Configure synchronous replication, promotion eligibility, degraded transitions
and detection timeouts. Streaming replication is asynchronous by default;
strict synchronous mode can block writes without a standby. Single-copy
commits cannot survive loss of the remaining copy before repair.

Fence isolated/resumed database primaries and separately reject stale poller
ownership. Validate clock skew, freeze/resume, partitions and disk loss.
An empty replacement must automatically restore copies and routing;
replica repair differs from adding capacity and redistributing shards.
Pod rescheduling or Patroni installation alone does not establish recovery.
Prove restored redundancy with a subsequent worker-loss test.

References: [Patroni/Citus](https://patroni.readthedocs.io/en/latest/citus.html),
[Kubernetes coordination](https://patroni.readthedocs.io/en/latest/kubernetes.html),
[replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html).

## API and consistency

| Operation | Required behavior |
| --- | --- |
| Canonical write | Atomically validate version/authority, mutate the event and write its outbox change; acknowledge under the replication policy. |
| Updates/retractions | Propagate changes as well as initial inserts. |
| Retry | Prevent duplicate events/effects; reject stale versions and conflicting reuse of a version. |
| Poller ownership | B supplies authority; A enforces it at the durable write boundary, including after freeze/resume. |
| Field report | C durably owns input before returning 202; acceptance is distinct from canonical visibility. |
| Audit list | Ascending `(ingested_at, hazard_id)`, cursor pagination, one logical copy per event. |
| Checksum | Per-source byte-sorted `source_ref_id` values joined by newline, no trailing newline; lowercase SHA-256. |
| Analytics | Count each active event once across shards; cover all five required hazard categories. |
| Missing data | Bounded responses, endpoint-specific partial markers and visibly incomplete totals. |

Define snapshot/cursor behavior during updates, promotion and repair,
failover response bounds and how reduced redundancy is reported.
The Aggregator must implement partial-result handling; distributed SQL
alone does not provide the required API contract. Validate write continuity
and critical notification latency during recovery.

