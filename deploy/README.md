# Deployment

Run `make` for available commands. Shared prerequisites: Python 3.11+, Make,
Git, OpenSSH and kubectl with Kustomize. Platform labels, ports, credentials and
resource classes follow [the platform conventions](../contracts/m2/PLATFORM.md).

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
nodes share Docker's memory and kernel. Allow Docker Desktop 3GiB for one/two
nodes or 4GiB for three; startup checks usable memory before building.

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
node-local disks with Retain policies. Service isolation, Tester Agent,
observability and the remaining pipeline components require separate integration.
