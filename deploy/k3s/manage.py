#!/usr/bin/env python3
"""Build, provision and deploy the BNPB Kubernetes baseline. Python stdlib only."""
import argparse
import base64
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
NS = "sistem-informasi-kebencanaan-bnpb"
NAMES = ("node-control", "node-app", "node-store")
APPS = ("bmkg", "pvmbg", "auth", "bnpb", "client-api", "dashboard-updater", "notifier", "analytics-consumer")
KAFKA = "docker.io/apache/kafka:3.9.0"
SECRET_KEYS = ("K3S_TOKEN", "BMKG_API_KEY", "PVMBG_TOKEN", "ADMIN_PASSWORD", "JWT_SECRET",
               "MEDIA_USER_PASSWORD", "FIELD_USER_PASSWORD", "INTERNAL_USER_PASSWORD", "INTERNAL_API_KEY")
PATHS = {
    "node-control": ("bnpb-m1", "auth", "canonical-a-primary", "canonical-c-replica"),
    "node-app": ("bmkg", "kafka", "canonical-a-replica", "canonical-b-primary"),
    "node-store": ("pvmbg", "canonical-b-replica", "canonical-c-primary"),
}


def run(args, *, data=None, capture=False, binary=False, env=None):
    """Never print stdin: provisioning config and Secret manifests contain secrets."""
    return subprocess.run([str(a) for a in args], input=data, check=True,
                          text=not binary, stdout=subprocess.PIPE if capture else None, env=env)


def private_write(path, content):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        stream.write(content)


def init():
    inventory = HERE / "inventory.json"
    if not inventory.exists():
        private_write(inventory, (HERE / "inventory.example.json").read_text())
    env = HERE / ".env"
    if not env.exists():
        private_write(env, "".join(f"{key}={secrets.token_hex(32)}\n" for key in SECRET_KEYS))
    print("Local inventory and credentials are ready. Fill inventory.json before provisioning.")


def inventory():
    path = HERE / "inventory.json"
    if not path.exists():
        raise ValueError("Run make config and fill deploy/k3s/inventory.json first")
    result = json.loads(path.read_text())
    if not re.fullmatch(r"v\d+\.\d+\.\d+\+k3s\d+", result.get("k3s_version", "")):
        raise ValueError("Pin k3s_version to an explicit k3s release")
    if result.get("architecture") not in ("amd64", "arm64"):
        raise ValueError("architecture must be amd64 or arm64; all nodes must match")
    nodes = result.get("nodes", [])
    if len(nodes) != 3 or {n.get("name") for n in nodes} != set(NAMES):
        raise ValueError("Inventory must declare exactly node-control, node-app and node-store")
    for node in nodes:
        if node.get("kind") not in ("native", "vm"):
            raise ValueError("Demo nodes must use native Linux or VMs with separate kernels")
        if node.get("ssh") == "local" and node["name"] != "node-control":
            raise ValueError("Only the native control-plane node can use local transport")
        if node.get("ssh") != "local" and not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9_][A-Za-z0-9_.-]*", node.get("ssh", "")):
            raise ValueError("Set each SSH destination as user@host (IPv4 or DNS)")
        ipaddress.IPv4Address(node.get("ip", ""))
        for field in ("physical_host", "host_cpu"):
            if not node.get(field) or "CHANGE_ME" in str(node[field]):
                raise ValueError(f"Fill topology field {field}")
        for field in ("host_ram_gib", "node_cpu", "node_ram_gib"):
            if type(node.get(field)) not in (int, float) or node[field] <= 0:
                raise ValueError(f"Fill positive topology field {field}")
    if len({n["ip"] for n in nodes}) != 3:
        raise ValueError("Each node requires a distinct stable private IP")
    if type(result.get("manage_control_plane", False)) is not bool:
        raise ValueError("manage_control_plane must be true or false")
    result["nodes"] = sorted(nodes, key=lambda n: NAMES.index(n["name"]))
    return result


def credentials():
    path = HERE / ".env"
    if not path.exists():
        raise ValueError("Run make config to create credentials")
    if path.stat().st_mode & 0o077:
        raise ValueError("Credentials must be private: chmod 600 deploy/k3s/.env")
    values = {}
    for line in path.read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            key, value = line.split("=", 1)
            if not value or "\x00" in value:
                raise ValueError("Empty or invalid credential value")
            values[key] = value
    if any(key not in values for key in SECRET_KEYS):
        raise ValueError("Missing required credential key; see deployment documentation")
    return values


def ssh_prefix(node):
    args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10"]
    if node.get("identity_file"):
        args += ["-i", node["identity_file"], "-o", "IdentitiesOnly=yes"]
    if node.get("known_hosts_file"):
        args += ["-o", "UserKnownHostsFile=" + node["known_hosts_file"], "-o", "StrictHostKeyChecking=yes"]
    return args + [node["ssh"]]


def ssh(node, command, *, data=None, capture=False, binary=False):
    if node["ssh"] == "local":
        return run(["bash", "-c", command], data=data, capture=capture, binary=binary)
    return run([*ssh_prefix(node), command],
               data=data, capture=capture, binary=binary)


def kubectl(*args, data=None, capture=False):
    config = Path(os.getenv("KUBECONFIG") or HERE / "kubeconfig")
    if not config.exists():
        raise ValueError("Missing dedicated kubeconfig; run make provision or install it at deploy/k3s/kubeconfig")
    return run([os.getenv("KUBECTL", "kubectl"), "--kubeconfig", config, *args], data=data, capture=capture)


def prepare_disks(node):
    # Only dedicated application directories are created. Never format or erase disks.
    paths = " ".join(shlex.quote("/var/lib/bnpb/" + p) for p in PATHS[node["name"]])
    script = f"set -euo pipefail\ninstall -d -o 1000 -g 1000 -m 0750 {paths}\n"
    ssh(node, "sudo -n bash -s", data=script)


def provision():
    inv, creds = inventory(), credentials()
    control = inv["nodes"][0]
    if not inv.get("manage_control_plane", False):
        # Reuse the installed server without changing its flags, token or system service.
        creds["K3S_TOKEN"] = ssh(control, "sudo -n cat /var/lib/rancher/k3s/server/node-token", capture=True).stdout.strip()
        if not creds["K3S_TOKEN"]:
            raise ValueError("Existing control plane returned an empty join token")
    for node in inv["nodes"]:
        name = node["name"]
        if name == "node-control" and not inv.get("manage_control_plane", False):
            print("Reusing existing node-control; its installation is unchanged", flush=True)
            prepare_disks(node)
            continue
        labels = ["store-eligible=true", "aggregator-eligible=true"]
        if name != "node-store":
            labels.append("app-eligible=true")
        config = {"node-name": name, "node-ip": node["ip"], "node-label": labels,
                  "token": creds["K3S_TOKEN"]}
        mode = "server" if name == "node-control" else "agent"
        if mode == "server":
            config.update({"tls-san": [control["ip"]], "advertise-address": control["ip"],
                           "write-kubeconfig-mode": "0600", "disable": ["traefik", "servicelb", "local-storage"]})
        else:
            config["server"] = f"https://{control['ip']}:6443"
        # JSON is valid YAML; a quoted heredoc keeps secrets out of shell expansion.
        payload = json.dumps(config, sort_keys=True, indent=2)
        delimiter = "BNPB_CONFIG_" + secrets.token_hex(16)
        script = f'''set -euo pipefail
command -v curl >/dev/null
test "$(uname -m)" = {shlex.quote('x86_64' if inv['architecture'] == 'amd64' else 'aarch64')}
if command -v k3s >/dev/null && [ ! -f /etc/rancher/k3s/bnpb-managed ]; then
  echo 'Refusing to take over an existing unmanaged k3s installation' >&2; exit 1
fi
install -d -m 0755 /etc/rancher/k3s
cfg=$(mktemp)
installer=$(mktemp)
trap 'rm -f "$cfg" "$installer"' EXIT
chmod 0600 "$cfg"
cat > "$cfg" <<'{delimiter}'
{payload}
{delimiter}
if command -v k3s >/dev/null; then
  k3s --version | head -n 1 | grep -F {shlex.quote(inv['k3s_version'])} >/dev/null
  cmp -s "$cfg" /etc/rancher/k3s/config.yaml || {{ echo 'Managed k3s config changed; reconcile explicitly' >&2; exit 1; }}
else
  install -m 0600 "$cfg" /etc/rancher/k3s/config.yaml
  curl --fail --silent --show-error --location https://get.k3s.io -o "$installer"
  INSTALL_K3S_VERSION={shlex.quote(inv['k3s_version'])} INSTALL_K3S_EXEC={shlex.quote(mode)} sh "$installer"
  touch /etc/rancher/k3s/bnpb-managed
fi
'''
        print(f"Provisioning {name}", flush=True)
        ssh(node, "sudo -n bash -s", data=script)
        prepare_disks(node)
    text = ssh(control, "sudo -n cat /etc/rancher/k3s/k3s.yaml", capture=True).stdout
    address = "https://127.0.0.1:6443" if control["ssh"] == "local" else f"https://{control['ip']}:6443"
    if "https://127.0.0.1:6443" not in text:
        raise ValueError("Unexpected k3s kubeconfig endpoint; install dedicated kubeconfig explicitly")
    path = HERE / "kubeconfig"
    if path.exists():
        os.chmod(path, 0o600)
        path.write_text(text.replace("https://127.0.0.1:6443", address))
    else:
        private_write(path, text.replace("https://127.0.0.1:6443", address))
    for name in NAMES:
        # Wait for registration first; kubectl wait itself fails if the node does not yet exist.
        deadline = time.monotonic() + 180
        while True:
            try:
                kubectl("get", "node", name, capture=True)
                break
            except subprocess.CalledProcessError:
                if time.monotonic() >= deadline:
                    raise ValueError(f"Node {name} did not register") from None
                time.sleep(3)
        kubectl("wait", "--for=condition=Ready", "node/" + name, "--timeout=180s")
    cluster_check(inv)
    print("Three nodes provisioned. Run make deploy for application workloads.")


def cluster_check(inv):
    data = json.loads(kubectl("get", "nodes", "-o", "json", capture=True).stdout)
    nodes = {n["metadata"]["name"]: n for n in data["items"]}
    for node in inv["nodes"]:
        actual = nodes.get(node["name"])
        if not actual or not any(c["type"] == "Ready" and c["status"] == "True" for c in actual["status"]["conditions"]):
            raise ValueError("All three inventory nodes must be Ready before deployment")
        expected_arch = inv["architecture"]
        if actual["status"]["nodeInfo"]["architecture"] != expected_arch:
            raise ValueError("Cluster node architecture does not match image architecture")
        if actual["metadata"]["labels"].get("kubernetes.io/hostname") != node["name"]:
            raise ValueError("Node hostname label must match its inventory name for local PV placement")
        if node["ip"] not in [a["address"] for a in actual["status"]["addresses"] if a["type"] == "InternalIP"]:
            raise ValueError("Inventory IP does not match the node's InternalIP")
        labels = ["store-eligible=true", "aggregator-eligible=true"]
        if node["name"] != "node-store":
            labels.append("app-eligible=true")
        else:
            # Keep this node out of ordinary application scheduling.
            labels.append("app-eligible=false")
        kubectl("label", "node", node["name"], *labels, "--overwrite")


def render(tag="dev"):
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("Invalid image tag")
    parent = ROOT / "deploy/kubernetes"
    with tempfile.TemporaryDirectory(prefix=".render-", dir=parent) as directory:
        overlay = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
                   "resources": ["../base"],
                   "images": [{"name": "bnpb.local/" + name, "newTag": tag} for name in APPS]}
        Path(directory, "kustomization.yaml").write_text(json.dumps(overlay))
        return run([os.getenv("KUBECTL", "kubectl"), "kustomize", directory], capture=True,
                   env={**os.environ, "KUBECONFIG": os.devnull}).stdout


def apply_secrets(creds, apply=None):
    apply = apply or kubectl
    definitions = {
        "bnpb-bmkg-auth": {"api-key": creds["BMKG_API_KEY"]},
        "bnpb-pvmbg-auth": {"token": creds["PVMBG_TOKEN"]},
        "bnpb-pvmbg-admin": {"username": "admin", "password": creds["ADMIN_PASSWORD"]},
        "bnpb-jwt-auth": {"signing-key": creds["JWT_SECRET"]},
        "bnpb-m1-internal-auth": {"key": creds["INTERNAL_API_KEY"]},
        "bnpb-user-auth": {"media-password": creds["MEDIA_USER_PASSWORD"],
                           "field-password": creds["FIELD_USER_PASSWORD"], "internal-password": creds["INTERNAL_USER_PASSWORD"]},
    }
    manifest = {"apiVersion": "v1", "kind": "List", "items": [
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": NS},
         "type": "Opaque", "data": {key: base64.b64encode(value.encode()).decode() for key, value in values.items()}}
        for name, values in definitions.items()]}
    apply("apply", "--server-side", "--field-manager=bnpb-platform", "-f", "-", data=json.dumps(manifest))


def archive_path():
    return Path(os.getenv("IMAGE_ARCHIVE") or HERE / "images.tar").resolve()


def file_sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def images(architecture=None):
    settings = HERE / "inventory.json"
    if not settings.exists():
        settings = HERE / "inventory.example.json"
    architecture = architecture or os.getenv("IMAGE_ARCH") or json.loads(settings.read_text())["architecture"]
    if architecture not in ("amd64", "arm64"):
        raise ValueError("IMAGE_ARCH must be amd64 or arm64")
    revision = run(["git", "-C", ROOT, "rev-parse", "--short", "HEAD"], capture=True).stdout.strip()
    tag = os.getenv("IMAGE_TAG") or f"{revision}-{time.time_ns()}"
    render(tag)  # Check tag and Kustomization before builds.
    built = []
    for name in APPS:
        image = f"bnpb.local/{name}:{tag}"
        directory = ROOT / "services" / name
        context = ROOT / "services" if name == "bnpb" else directory
        print(f"Building {name}", flush=True)
        run(["docker", "build", "--platform", "linux/" + architecture, "-t", image,
             "-f", directory / "Dockerfile", context])
        built.append(image)
    run(["docker", "pull", "--platform", "linux/" + architecture, KAFKA])
    built.append(KAFKA)
    archive = archive_path()
    # Containerd image stores may retain multi-platform indexes after a single-
    # platform pull. Export only complete target-platform manifests for ctr/kind.
    run(["docker", "image", "save", "--platform", "linux/" + architecture, "-o", archive, *built])
    archive.with_suffix(".json").write_text(json.dumps({"tag": tag, "architecture": architecture,
                                                     "images": built, "sha256": file_sha256(archive)}, indent=2) + "\n")
    print(f"Image bundle saved: {archive}. Docker can be stopped before deployment.")


def image_bundle(inv):
    archive = archive_path()
    if not archive.is_file() or not archive.with_suffix(".json").is_file():
        raise ValueError("Missing image bundle. Run make images on a build machine, then copy images.tar and images.json here")
    metadata = json.loads(archive.with_suffix(".json").read_text())
    tag = metadata.get("tag", "")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("Invalid image bundle tag")
    if metadata.get("architecture") != inv["architecture"]:
        raise ValueError("Image bundle architecture does not match inventory")
    expected = {f"bnpb.local/{name}:{tag}" for name in APPS} | {KAFKA}
    if set(metadata.get("images", [])) != expected:
        raise ValueError("Image bundle must include all application images and Kafka")
    if metadata.get("sha256") != file_sha256(archive):
        raise ValueError("Image bundle checksum mismatch")
    return archive, tag


def deploy():
    inv, creds = inventory(), credentials()
    if os.getenv("BUILD_IMAGES") == "1":
        images()
    archive, tag = image_bundle(inv)
    manifests = render(tag)
    cluster_check(inv)
    for node in inv["nodes"]:
        prepare_disks(node)
        print(f"Importing images on {node['name']}", flush=True)
        args = ["sudo", "-n", "k3s", "ctr", "--namespace", "k8s.io", "images", "import", "-"]
        if node["ssh"] != "local":
            args = [*ssh_prefix(node), shlex.join(args)]
        with archive.open("rb") as stream:
            subprocess.run(args, stdin=stream, check=True)
    namespace = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}}
    kubectl("apply", "-f", "-", data=json.dumps(namespace))
    apply_secrets(creds)
    kubectl("apply", "-f", "-", data=manifests)
    kubectl("-n", NS, "rollout", "status", "deployment/kafka", "--timeout=240s")
    # Re-run idempotent topic creation after every deploy, including broker disk replacement.
    kubectl("-n", NS, "delete", "job", "topic-init", "--ignore-not-found", "--wait=true")
    kubectl("apply", "-f", ROOT / "deploy/kubernetes/base/topic-init.yaml")
    kubectl("-n", NS, "wait", "--for=condition=complete", "job/topic-init", "--timeout=240s")
    for name in APPS:
        kubectl("-n", NS, "rollout", "status", "deployment/" + name, "--timeout=240s")
    kubectl("-n", NS, "get", "pods", "-o", "wide")
    print(f"Baseline deployed. Client API: http://{inv['nodes'][0]['ip']}:30080")


def status():
    kubectl("get", "nodes", "-o", "wide")
    kubectl("-n", NS, "get", "pods,services,pvc", "-o", "wide")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "provision", "images", "deploy", "render", "status", "topology"))
    args = parser.parse_args()
    try:
        if args.action == "render":
            print(render(os.getenv("IMAGE_TAG") or "dev"), end="")
        else:
            globals()[args.action]()
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as exc:
        # CalledProcessError includes argv only: secret content is carried on stdin.
        print(f"Platform action failed: {exc}", file=sys.stderr)
        return 1
    return 0


def topology():
    inv = inventory()
    print("| Node | Type | Physical host | Host CPU | Host RAM (GiB) | Node vCPU | Node RAM (GiB) |")
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for node in inv["nodes"]:
        values = [node[k] for k in ("name", "kind", "physical_host", "host_cpu", "host_ram_gib", "node_cpu", "node_ram_gib")]
        print("| " + " | ".join(str(v).replace("|", "/").replace("\n", " ") for v in values) + " |")


if __name__ == "__main__":
    sys.exit(main())
