#!/usr/bin/env python3
"""Container-based kind profile for development; never the assessed VM topology."""
import argparse
import json
import os
import platform
import re
import select
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import manage

HERE = Path(__file__).resolve().parent
STATE = HERE / ".dev"
NAME = "bnpb-dev"
# Pin versions together; changing the node image requires recreating the dev cluster.
NODE_IMAGE = "kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a"


def count():
    value = os.getenv("DEV_NODES")
    record = STATE / "cluster.json"
    if not value and record.exists():
        value = str(len(json.loads(record.read_text())["nodes"]))
    value = value or "3"
    if value not in ("1", "2", "3"):
        raise ValueError("DEV_NODES must be 1, 2 or 3")
    return int(value)


def node_names():
    return [NAME + "-control-plane"] + [NAME + "-worker" + (str(index + 1) if index else "") for index in range(count() - 1)]


def placement():
    """Fold logical storage locations onto available development nodes only."""
    nodes = node_names()
    return {logical: nodes[min(index, len(nodes) - 1)] for index, logical in enumerate(manage.NAMES)}


def config():
    nodes = [{"role": "control-plane", "image": NODE_IMAGE}]
    nodes += [{"role": "worker", "image": NODE_IMAGE} for _ in range(count() - 1)]
    return {"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4", "nodes": nodes}


def render(tag="dev"):
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("Invalid image tag")
    mapping = placement()
    patches = []
    for logical, paths in manage.PATHS.items():
        for path in paths:
            patches.append({"target": {"kind": "PersistentVolume", "name": "bnpb-" + path},
                            "patch": json.dumps([{"op": "replace", "path": "/spec/nodeAffinity/required/nodeSelectorTerms/0/matchExpressions/0/values/0", "value": mapping[logical]}])})
    # NodePort is omitted: dev access is via port-forward, avoiding host port collisions.
    patches.append({"target": {"kind": "Service", "name": "client-api"}, "patch": json.dumps([
        {"op": "replace", "path": "/spec/type", "value": "ClusterIP"}, {"op": "remove", "path": "/spec/ports/0/nodePort"}])})
    if os.getenv("CANONICAL_STORAGE") == "postgres":
        if count() != 3:
            raise ValueError("Canonical replicas require DEV_NODES=3; do not fold replicas onto one node")
        import canonical
        return canonical.render(tag, patches)
    overlay = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "resources": ["../base"],
               "patches": patches, "images": [{"name": "bnpb.local/" + name, "newTag": tag} for name in manage.APPS]}
    with tempfile.TemporaryDirectory(prefix=".dev-render-", dir=manage.ROOT / "deploy/kubernetes") as directory:
        Path(directory, "kustomization.yaml").write_text(json.dumps(overlay))
        return manage.run([os.getenv("KUBECTL") or "kubectl", "kustomize", directory], capture=True,
                          env={**os.environ, "KUBECONFIG": os.devnull}).stdout


def kube_command(*args):
    return [os.getenv("KUBECTL") or "kubectl", "--kubeconfig", str(STATE / "kubeconfig"), *args]


def kube(*args, **kwargs):
    return manage.run(kube_command(*args), **kwargs)


def wait_control_plane():
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        result = subprocess.run(kube_command("--request-timeout=5s", "get", "node", node_names()[0], "-o", "json"),
                                capture_output=True, text=True)
        if result.returncode == 0:
            node = json.loads(result.stdout)
            if any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"].get("conditions", [])):
                return node
        time.sleep(2)
    raise ValueError("Development control plane did not become Ready within 180 seconds")


def docker_ready():
    info = json.loads(manage.run(["docker", "info", "--format", "{{json .}}"], capture=True).stdout)
    # Budget application requests, six telemetry pods, policy enforcement and Kubernetes.
    minimum = {1: 3584, 2: 3584, 3: 4608}[count()] * 1024 * 1024
    if os.getenv("CANONICAL_STORAGE") == "postgres":
        minimum += 3584 * 1024 * 1024
    if info.get("MemTotal", 0) < minimum:
        setting = 9 if os.getenv("CANONICAL_STORAGE") == "postgres" else (4 if count() <= 2 else 5)
        raise ValueError(f"Docker has too little memory for this profile. Allocate at least {setting} GiB in Docker Desktop Resources, apply/restart, and retry")


def network():
    """Add policy enforcement to kind without replacing its CNI or node data."""
    kube('get', 'nodes', capture=True)
    kube('apply', '-f', manage.ROOT / 'deploy/kubernetes/dev/network-policy.yaml')
    kube('-n', 'kube-system', 'rollout', 'status', 'daemonset/bnpb-network-policy', '--timeout=60s')


def up():
    for tool in ("kind", "docker", os.getenv("KUBECTL") or "kubectl"):
        if not shutil.which(tool):
            raise ValueError("Install " + tool + " first; see deploy/README.md")
    # Check Docker before writing cluster state or attempting builds.
    docker_ready()
    architecture = platform.machine()
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(architecture)
    if not arch:
        raise ValueError("Unsupported development host architecture")
    if os.getenv("BUILD_IMAGES") == "1":
        manage.images(architecture=arch)
    archive, tag = manage.image_bundle({"architecture": arch})
    manifests = render(tag)
    plan = config()
    clusters = manage.run(["kind", "get", "clusters"], capture=True).stdout.splitlines()
    record = STATE / "cluster.json"
    if NAME in clusters and (not record.exists() or json.loads(record.read_text()) != plan):
        raise ValueError("Existing kind cluster is unmanaged or has a different node count/image. Use make dev-down explicitly")
    STATE.mkdir(mode=0o700, exist_ok=True)
    os.chmod(STATE, 0o700)
    config_path = STATE / "kind.json"
    config_path.write_text(json.dumps(plan, indent=2))
    if NAME not in clusters:
        manage.run(["kind", "create", "cluster", "--name", NAME, "--config", config_path, "--kubeconfig", STATE / "kubeconfig", "--wait", "180s"])
        record.write_text(json.dumps(plan, indent=2))
    else:
        for node in node_names():
            running = manage.run(["docker", "inspect", "--format", "{{.State.Running}}", node], capture=True).stdout.strip()
            if running != "true":
                print("Resuming stopped development node " + node, flush=True)
                manage.run(["docker", "start", node])
        result = manage.run(["kind", "get", "kubeconfig", "--name", NAME], capture=True).stdout
        if not (STATE / "kubeconfig").exists():
            manage.private_write(STATE / "kubeconfig", result)
        else:
            (STATE / "kubeconfig").write_text(result)
    os.chmod(STATE / "kubeconfig", 0o600)
    # The native baseline schedules on its control plane. Match that behavior in kind.
    control = node_names()[0]
    info = wait_control_plane()
    from canonical import check_backend
    check_backend(kube)
    taints = [t for t in info.get("spec", {}).get("taints", []) if t["key"] != "node-role.kubernetes.io/control-plane"]
    kube("patch", "node", control, "--type=merge", "-p", json.dumps({"spec": {"taints": taints}}))
    manage.run(["kind", "load", "image-archive", archive, "--name", NAME])
    mapping = placement()
    for logical, paths in manage.PATHS.items():
        node = mapping[logical]
        manage.run(["docker", "exec", node, "mkdir", "-p", *["/var/lib/bnpb/" + path for path in paths]])
        manage.run(["docker", "exec", node, "chown", "1000:1000", *["/var/lib/bnpb/" + path for path in paths]])
        manage.run(["docker", "exec", node, "chmod", "0750", *["/var/lib/bnpb/" + path for path in paths]])
    for index, node in enumerate(node_names()):
        kube("label", "node", node, "store-eligible=true", "aggregator-eligible=true",
             "app-eligible=" + ("false" if index == 2 else "true"), "--overwrite")
    network()
    manage.init()  # Credentials shared with local application development, never kubeconfig.
    # Credential creation is transport-independent; use the isolated dev kubeconfig.
    kube("apply", "-f", "-", data=json.dumps({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": manage.NS}}))
    from security import install
    install(kube)
    manage.apply_secrets(manage.credentials(), apply=kube)
    if os.getenv("CANONICAL_STORAGE") == "postgres":
        from canonical import apply_secrets
        apply_secrets(kube)
    kube("apply", "-f", "-", data=manifests)
    if os.getenv("CANONICAL_STORAGE") == "postgres":
        from canonical import initialize
        initialize(kube, tag)
    kube("-n", manage.NS, "rollout", "status", "deployment/kafka", "--timeout=240s")
    kube("-n", manage.NS, "delete", "job", "topic-init", "--ignore-not-found", "--wait=true")
    kube("apply", "-f", manage.ROOT / "deploy/kubernetes/base/topic-init.yaml")
    kube("-n", manage.NS, "wait", "--for=condition=complete", "job/topic-init", "--timeout=240s")
    for name in manage.APPS:
        kube("-n", manage.NS, "rollout", "status", "deployment/" + name, "--timeout=240s")
    from observability import install as install_observability
    install_observability(kube)
    print("Development cluster is ready. Use the dedicated kubeconfig in deploy/k3s/.dev/kubeconfig.")


def down():
    if not (STATE / "cluster.json").exists():
        raise ValueError("No managed development cluster record; refusing deletion")
    manage.run(["kind", "delete", "cluster", "--name", NAME])
    (STATE / "cluster.json").unlink()
    print("Development cluster deleted, including its container-local data. Native k3s is unchanged.")


def forward():
    if not (STATE / "cluster.json").exists():
        raise ValueError("Create the development cluster with make dev-up first")
    kube("-n", manage.NS, "port-forward", "--address", "127.0.0.1", "service/client-api", "9080:8080")


def api_request(base, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, None


def smoke():
    if not (STATE / "cluster.json").exists():
        raise ValueError("Create the development cluster with make dev-up first")
    credentials = manage.credentials()
    command = [os.getenv("KUBECTL") or "kubectl", "--kubeconfig", str(STATE / "kubeconfig"), "-n", manage.NS,
               "port-forward", "--address", "127.0.0.1", "service/client-api", ":8080"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline, base = time.monotonic() + 30, None
        while time.monotonic() < deadline and process.poll() is None:
            if not select.select([process.stdout], [], [], 1)[0]:
                continue
            match = re.search(r"Forwarding from 127\.0\.0\.1:(\d+)", process.stdout.readline())
            if match:
                base = "http://127.0.0.1:" + match[1]
                break
        if base is None:
            raise ValueError("Development API port-forward did not become ready")
        def login(username, password):
            status, result = api_request(base, "/auth/login", {"username": username, "password": password})
            token = result.get("data", {}).get("accessToken") if isinstance(result, dict) else None
            if status != 200 or not token:
                raise ValueError(f"Login failed for {username}: HTTP {status}")
            return token
        field = login("field-user", credentials["FIELD_USER_PASSWORD"])
        deadline, items = time.monotonic() + 30, []
        while time.monotonic() < deadline:
            status, items = api_request(base, "/hazards?source=BMKG&limit=1", token=field)
            if status == 200 and isinstance(items, list) and items:
                break
            time.sleep(2)
        if not items or status != 200 or "latitude" not in items[0]:
            raise ValueError("Field-team query did not return ingested BMKG data with full fields")
        media = login("media-user", credentials["MEDIA_USER_PASSWORD"])
        status, summary = api_request(base, "/hazards?source=BMKG&limit=1", token=media)
        if status != 200 or not summary or "latitude" in summary[0] or "attributes" in summary[0]:
            raise ValueError("Media summary scope check failed")
        if api_request(base, "/hazards?fields=latitude", token=media)[0] != 403:
            raise ValueError("Media raw-field request was not rejected with HTTP 403")
        if api_request(base, "/hazards")[0] != 401:
            raise ValueError("Unauthenticated hazard request was not rejected with HTTP 401")
        print("Smoke checks passed: login, ingested BMKG data, full and summary scopes, Media raw-field rejection, and missing-token rejection.")
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        process.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("up", "down", "render", "smoke", "forward", "network"))
    args = parser.parse_args()
    try:
        if args.action == "render":
            print(render(os.getenv("IMAGE_TAG") or "dev"), end="")
        else:
            globals()[args.action]()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
        print("Development profile failed: " + str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
