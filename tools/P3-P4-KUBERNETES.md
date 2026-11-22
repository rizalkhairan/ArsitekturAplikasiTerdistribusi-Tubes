# P3/P4 handoff

Run in Bash from the repository root on the machine with access to the deployed
Kubernetes cluster. No Docker daemon is used by these acceptance runners.

## One-time configuration

```bash
cp tools/p4-acceptance.env.example tools/.env.p3-p4
```

Edit `tools/.env.p3-p4`: set KUBECONFIG, namespace, official reference JSON path,
and official workload adapter path. The combined runner loads it automatically.
The private configuration is ignored by git. Test evidence is saved in the
repository-root `test-results/` folder and can be committed after review.

Prerequisites: Bash 4+, kubectl, curl, jq, Go compatible with the service module,
GNU shell tools, and permissions to inspect workloads/Secrets/logs, scale and exec
pods, copy helpers, and provision/delete temporary Deployments/Services/PVCs.
The Go helpers need module-cache or download access. The cluster needs the
processor image, python:3.12-alpine, local-path provisioning, and enough resources
for a temporary processor/mock. Close other port-forwards/Compose instances on
ports 9080, 8094, 8082, 8085, 8096, 8098, 8099, and 18085.

## Commands

Read-only readiness check (does not require official workload/reference files):

```bash
bash tools/p3-p4-kubernetes-test.sh --check-only
```

Full run, after preparing the official resources and running feed session:

```bash
bash tools/p3-p4-kubernetes-test.sh
```

This interrupts application components. Use a dedicated rehearsal/demo cluster.
It does not create the cluster, deploy the application, or start the exam session.
P3 changes PVMBG schema modes and restores baseline. It restores saved replica
counts on normal failure/exit. P4 creates a temporary mock and processor, writes
synthetic records through the real canonical API, then removes fixture resources.
Synthetic canonical records remain. Exact identities created in the current run
are excluded from official comparison; unrelated old records are not hidden.
P4 crash injection force-deletes the temporary processor pod with zero grace and
requires a replacement pod; this works with the distroless production image.
An uninterrupted baseline uses separate disposable state and compares the local
business projection; canonical delivery is required on the recovered run.

## Official resources

The workload adapter is a Bash file invoking the actual supplied workload and
waiting for completion. It must write to `$P4_LOAD_RECEIPT`:

```json
{"speed":60,"final_offset":12345,"ended_at_unix":1790000000}
```

These must be actual values; ended_at_unix is burst completion time. The harness
cannot infer the assistant workload interface or independently authenticate this
receipt. Integrate and review the adapter once those resources are available.
Reference format is `{"fires":[...]}` with complete canonical fire fields and
the same source session. A different reference format requires an explicit,
reviewed adapter. Do not manufacture references to make tests pass.

## Results and limitations

`test-results/latest-run.txt` identifies the latest result directory.
`summary.md` shows mode/status/exit code; `console.log` captures all output.
Stage evidence includes durable receipt checks, recovered/baseline projections,
mapping fixtures, lag samples, workload output, canonical pages and diffs.
Exit 0 means assertions for the selected mode passed; exit 2 means blocked;
other nonzero results mean failure. Readiness PASS is not acceptance PASS.

Scripts have passed syntax and local fixture checks, not an end-to-end cluster
run. Platform manual evidence remains necessary: separate kernels/topology,
Lampiran B values, runtime isolation, observability and official Tester Agent
scenario/auditor output. P3 keeps the existing policy that Audit sees metadata
and only Ops sees raw/replays; confirm this interpretation with the requirement.
Return the complete result folder for debugging. Do not include the private
configuration or Kubernetes Secret exports.
