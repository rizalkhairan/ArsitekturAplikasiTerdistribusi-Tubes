# Deployment

Run `make` for available commands. Shared prerequisites: Python 3.11+, Make,
Git, OpenSSH and kubectl with Kustomize. Platform labels, ports, credentials and
resource classes follow [the platform conventions](../contracts/m2/PLATFORM.md).

## Canonical PostgreSQL storage

The opt-in overlay uses PostgreSQL 16.6/Citus 13.0.1/Patroni 4.1.5 and the six
existing canonical local PVs in [the architecture](../services/ARCHITECTURE.md).
Each pair has required node anti-affinity and 512Mi/1Gi memory settings. Three
stateless Aggregators use soft spreading, permitting B's later 3 → 5 → 2 test.

```bash
make canonical-render
make images CANONICAL_STORAGE=postgres
make deploy CANONICAL_STORAGE=postgres             # Provisioned k3s nodes
# Development alternative: three kind nodes; allow about 9GiB Docker RAM
make dev-up CANONICAL_STORAGE=postgres DEV_NODES=3 BUILD_IMAGES=1
make canonical-check SECURITY_PROFILE=--dev        # Omit profile on k3s
```

This switches backends without importing SQLite. For an existing dataset,
quiesce M1 polling/publication first, retain the database, and keep clients and
producers stopped until import/verification finish. The overlay retains the M1
PVC. Supply `CANONICAL_ADMIN_DSN` through the environment using a private
administrator connection/tunnel, then run:

```bash
cd services/bnpb
go run ./cmd/canonical-admin -sqlite /path/to/quiescent/bnpb.db import-m1
```

Import opens SQLite read-only and preserves IDs, ingestion timestamps, broker
message IDs/payloads and publication state. It initializes versions to 1 with
source-named partitions; producers resume above that version. Conflicting IDs
abort the import; repeated imports preserve newer versions. Normal Aggregators
receive only `canonical_app` credentials; a separate Job initializes the schema.

Preserve private `deploy/k3s/.canonical.env` for redeployment. Three Secrets
separate cluster/replication credentials, schema-administrator DSN, and
Aggregator DSN/service-token mappings. Changing Secrets alone does not rotate
database passwords. B's three initial poller identities have distinct tokens;
additional replicas require provisioned identities. C receives only its own
producer token. Keep `CANONICAL_STORAGE=postgres` on subsequent deployments;
tooling refuses an accidental SQLite rollback.

Events/outbox rows use 48 colocated hash shards keyed by the exact JSON-encoded
`[source, source_ref_id]` pair; geography is excluded. Physical replication
protects each group. Patroni uses Kubernetes Endpoints as DCS and updates Citus
routing; `postgres-role` preserves `role=store`. Explicit projected API tokens
and namespace-scoped Patroni RBAC coexist with private PVC/Secret admission and
database ingress restrictions.

The initial policy requires all three synchronous standbys, strict synchronous
Patroni mode and `synchronous_commit=remote_apply`. Successful mutations return
`durability: replicated`; retries force fresh WAL before acknowledgement to
cover uncertain prior commits. A missing standby returns 503. Single-copy
availability, safe promotion, repair and membership changes remain recovery
work; do not disable strict mode to claim that behavior. Database primary
fencing under freeze/partition still needs live failure tests.

The [canonical contract](../contracts/m2/README.md) defines authenticated event
PUTs, versions and retractions. Two implemented handoff routes need B/C review:

- `PUT /internal/canonical/authority`, authority-only credential:
  `{partition, epoch, owner, expires_at}`. Higher epochs transfer ownership;
  same-epoch renewals require the same owner and a valid lease. Database time
  governs expiry. Installation and mutations lock the same persisted row.
- `POST /internal/canonical/windows`, producer credential:
  `{authority, window_id, previous_cursor, next_cursor, items}`, with items
  `{source, source_ref_id, mutation}`. The entire batch and receipt commit
  atomically, including empty windows. Retries preserve content. B must CAS its
  cursor only after success, verifying current owner/epoch and prior cursor.
  These routes do not implement B's lease issuance or cursor CAS.

PostgreSQL mode disables unfenced M1 pollers; B connects lease-aware polling and
C persists producer versions. Updates/retractions produce outbox messages with
`event_id`, `version` and `operation`. Publication is at least once and orders
pending changes per identity; consumers must reject duplicate/older versions
and handle retractions. Existing `/hazards` reads retain internal authentication
and fields; failed queries return 503. Snapshot pagination, partial reads,
Audit and analytics remain subsequent work.

```bash
make platform-check
make canonical-test           # SQL tests skip unless CANONICAL_TEST_DSN is set
make canonical-integration    # Disposable Docker: 3 primaries + 3 standbys
```

The integration runner removes only its own containers/network. It checks real
Citus transactions, version/epoch races, receipts, outbox retries, M1 import,
shard placement, WAL replay and rejection after standby loss. It does not run
Patroni/Kubernetes. These checks do not prove separate-kernel durability,
promotion, partition safety, live policy enforcement or B/C end-to-end ingest.
Use `canonical-check` for live placement/replication readiness before collecting
failure evidence. Configuration follows the official
[Patroni Citus integration](https://patroni.readthedocs.io/en/latest/citus.html)
and [replication guidance](https://patroni.readthedocs.io/en/latest/replication_modes.html).

## Container development

Requires Docker and kind v0.30.0. On Windows, run from WSL2 with Docker Desktop
integration enabled and keep the checkout in the Linux filesystem.

```bash
make dev-up DEV_NODES=2 BUILD_IMAGES=1  # Build images, create cluster, deploy
make dev-smoke                         # Verify login, ingestion and access scopes
make dev-forward                       # API at localhost:9080; Ctrl-C closes tunnel
```

With an existing image bundle, omit `BUILD_IMAGES=1`. Subsequent `make dev-up`
commands remember the existing node count and resume stopped node containers.
The development kubeconfig is `deploy/k3s/.dev/kubeconfig`.

| DEV_NODES | Logical control location | Logical app location | Logical store location |
| --- | --- | --- | --- |
| 1 | control-plane | control-plane | control-plane |
| 2 | control-plane | worker | worker |
| 3 (initial default) | control-plane | worker | worker2 |

Local PV affinity and data directories follow this mapping; stateless pods use
eligible nodes. Replica counts and resource settings remain unchanged. All kind
nodes share Docker's memory and kernel. Allow Docker Desktop 4GiB for one/two
nodes or 5GiB for three; startup checks usable memory before building.

Changing the count/image requires `make dev-down` first. **This deletes the
cluster and its container-local data.** Kind profiles are for development and
cannot demonstrate the assignment's separate-kernel topology. Single-node mode
is a coding convenience, not an explicit specification exception.

## Native Linux and worker VMs

The libvirt provider supports native Linux amd64 with KVM and systemd:

```bash
make host-tools            # Once: install libvirt/KVM, virt-install and cloud-init tools
make bootstrap-plan        # Preview without changes
sudo -v
make bootstrap             # Create/configure worker VMs and join k3s
make deploy BUILD_IMAGES=1 # Build images and deploy applications
```

To customize, copy `deploy/k3s/bootstrap.example.json` to
`deploy/k3s/bootstrap.json`. Defaults: two Debian 12 VMs, each 2 vCPU/2GiB RAM, disks
40GiB and 32GiB, on the private 192.168.56.0/24 NAT network. Bootstrap verifies
the cloud image against SHA512SUMS, configures guests through cloud-init, creates
SSH credentials and installs k3s. An existing native server is reused; otherwise
one is installed. The generated inventory records actual topology information.

Repeated runs reuse managed VMs and disks. Available RAM, subnet conflicts and
ownership checks run before creation. Ensure VM allocations, the native host
and application workloads fit physical RAM. Choose a nonconflicting subnet;
allow private-network TCP 6443/10250 and UDP 8472 for k3s as appropriate.

`make vm-stop` preserves VM disks. **`make vm-destroy` deletes managed worker
VMs and their disks.** Neither operation removes the native control plane.
Native Windows VM creation is not implemented; use the development profile or
create Linux guests separately and select the existing-machines provider.

## Existing machines and image bundles

```bash
make config
# Fill deploy/k3s/inventory.json with node addresses, SSH access and host resources.
make bootstrap BOOTSTRAP_PROVIDER=existing
make deploy
```

Use `ssh: local` for a local control plane, otherwise `user@host`. Remote
provisioning needs trusted SSH host keys and noninteractive sudo. Configuration
uses ConfigMaps; generated credentials stay in private `deploy/k3s/.env` (0600).
Do not regenerate credentials for an existing database without a migration.

Deployment uses `$KUBECONFIG` or `deploy/k3s/kubeconfig`. Kubernetes runtime is
containerd; Docker is only required on the image build machine. `make images`
creates `deploy/k3s/images.tar` and matching `images.json`; copy both to deploy
elsewhere. Override their location with `IMAGE_ARCHIVE=/path/images.tar`.
The builder requires `docker image save --platform` support. Deployment verifies
the archive checksum/architecture and imports it on every node before rollout.

## Checks and current scope

`make platform-check` requires PyYAML (`python3-yaml` on Debian/Ubuntu).
`make render` and `make dev-render DEV_NODES=2` render manifests without a cluster.
`make platform-status` shows native-cluster resources; `make topology` prints
an inventory-derived topology table for deployment documentation.

The baseline runs one SQLite-backed Aggregator and a single Kafka broker.
Do not increase stateful replicas without implementing coordination/replication.
Six canonical PV/PVC pairs reserve the planned PostgreSQL/Citus placement;
reserved volumes do not establish replication or recovery guarantees. Data uses
node-local disks with Retain policies. Remaining pipeline components require
separate integration.

## Storage isolation and Tester Agent

Deployment applies the [assignment-pinned manifest](https://raw.githubusercontent.com/susTuna/if4031-tester-agent/ebb55261711123a9a211739ffbf140a275caa6da/manifest.yaml)
from `deploy/tester-agent/manifest.yaml` unchanged, outside Kustomize. Its SHA-256
is checked before installation. No fault scenario runs automatically.

```bash
make tester-install                       # Also included in deploy/dev-up
make tester-check                         # Every node ready, RBAC, /app list on each node
make isolation-check                      # Admission, RBAC and live network probes
# Append SECURITY_PROFILE=--dev to use deploy/k3s/.dev/kubeconfig.
```

Requires Kubernetes 1.30+ with ValidatingAdmissionPolicy enabled and an enforcing
NetworkPolicy controller. [K3s includes network policy enforcement](https://docs.k3s.io/networking/networking-services#network-policy-controller);
do not disable it. `dev-up` adds kube-router v2.6.2 in
[firewall-only mode](https://www.kube-router.io/docs/user-guide/#running-as-agent)
to enforce policies alongside kindnet and kube-proxy. For an existing kind cluster,
run `make dev-network` before the checks; this preserves its CNI and node data.
The controller image requires registry access or a preloaded image. It is a
system DaemonSet in `kube-system`, outside the application overlay and VM profile.
`isolation-check` requires a healthy baseline, administrative impersonation rights
and the images' existing `wget`.
It performs server dry-runs and bounded HTTP probes without creating pods,
mounting private volumes, reading Secrets or triggering faults. Run `dev-smoke`
after deployment as well. Rendering/static checks do not establish enforcement.

Each current component has a distinct ServiceAccount, no API grants and no mounted
API token. Admission restricts claims and Secret references to the owning service
(including init containers), checks identity/role labels and rejects host access,
privileged capabilities, indirect Secret volumes and ephemeral containers.
SQLite databases remain embedded in their owner; there is no database socket or
password to share. Existing JWT/upstream/internal API trust relationships remain
explicit Secret allowlists. Database transport credentials and grants belong with
the actual PostgreSQL/Patroni provisioning; reserved canonical volumes remain
unmounted and cannot be borrowed. RWO alone does not enforce service ownership;
the admission policy supplies that boundary. Cluster administrators remain trusted.

Ingress defaults to deny. Allowed paths cover Client API → Auth/Aggregator,
Aggregator → mocks/broker, consumers/topic creation → broker, and Prometheus
scrapes. Reserved stores accept SQL from Aggregator and SQL/Patroni traffic
between stores. Tester Agent has unrestricted application ingress; its namespace,
privileged pod, host mounts, tolerations and supplied RBAC remain untouched.
Egress remains available for DNS, external feeds, Kubernetes coordination and
telemetry. No application policy targets system or Tester Agent namespaces.

B/C integration keeps the current broker/consumer/topic-init names and resource
classes. Proposed field-report and satellite callers have Aggregator/broker
network paths reserved. Before adding their workloads, register their identities,
private claims and Secret references in `identities.yaml` and `admission.yaml`;
add narrowly scoped ingress for the supplied sensor's actual listener. Coordination
API permissions, broker authentication/replication and pipeline-state volumes
require B/C's concrete manifests; no shared credential or privileged bypass is
provided. Preserve `role=store` for future databases and separate replication
credentials from Aggregator credentials as specified in the architecture.

VM/demo validation remains pending. Re-run Agent discovery, isolation and smoke
checks on the separate-kernel topology before running practice scenarios. Practice
names come from `make tester-check`; run an explicitly chosen scenario with
`kubectl --kubeconfig deploy/k3s/kubeconfig exec -n tester-agent daemonset/tester-agent -- /app run practice <name>`.


## Operational telemetry

`make deploy` / `make dev-up` also install Prometheus, Grafana, Tempo, Loki,
OpenTelemetry Collector and Alloy. Rebuild the image bundle after this change;
it now includes the six pinned telemetry images. The stack adds **768Mi requested,
1536Mi limited RAM**, with one replica per component, preferring the control plane.
Application namespace/role labels and resource classes remain unchanged.
Standalone `observability-install` assumes the updated base manifests and Secrets
have already been applied.

```bash
make observability-render                         # Inspect the separate overlay
make observability-validate                       # Pinned binaries validate configs (Docker)
make observability-check OBSERVABILITY_PROFILE=--dev
make telemetry-smoke OBSERVABILITY_PROFILE=--dev   # Real request, metrics, trace and log evidence
make observability-forward OBSERVABILITY_PROFILE=--dev
```

Grafana opens at `http://localhost:3000`, username `admin`; its generated password
is `GRAFANA_ADMIN_PASSWORD` in private `deploy/k3s/.env`. Configuration upgrades
add this credential without rotating existing keys. The provisioned **BNPB service
operations** dashboard shows throughput, 5xx fraction, p95 latency, scrape
availability and correlation-ID log searches. Explore includes Prometheus, Tempo
and Loki; a structured log's TraceID links to Tempo. Backends are ClusterIP-only.

Client API, Aggregator read HTTP and Auth now use the shared telemetry helper,
including trace propagation on both Client API upstreams. Their metrics use
internal port 9090; the public Client API Service exposes no metrics port.
The smoke check authenticates with a temporary access token, sends a uniquely
identified hazard query and waits up to 60 seconds for both services to appear
in Prometheus, the same Tempo trace and Loki logs. It prints IDs, never credentials.

Alloy reads pod logs through namespace-scoped `pods`/`pods/log` permissions and
an explicitly projected token. It mounts no node/application disk. Log bodies
retain correlation/trace IDs as searchable JSON fields, avoiding high-cardinality
index labels. Other application accounts retain no API token or grants.

Prometheus/Tempo retain six hours; Loki retains 24 hours. Each backend uses private
`emptyDir` storage with a 1Gi budget: restart/eviction loses its history. This
single-instance stack provides no HA or evidence of capacity under demo load.
BMKG/PVMBG, Kafka and consumer/pipeline instrumentation remain their owners'
integration work; unavailable scrape targets stay visible. HTTP read traces do
not establish polling or asynchronous-message trace propagation. Live cluster
checks and separate-kernel VM/demo evidence remain required.
