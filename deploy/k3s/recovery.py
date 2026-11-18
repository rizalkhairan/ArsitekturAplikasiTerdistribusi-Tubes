#!/usr/bin/env python3
"""Operate the existing six-slot Patroni formation without deleting data."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

import manage


def error_text(exc):
    if isinstance(exc, subprocess.CalledProcessError) and exc.stderr:
        return exc.stderr.strip()
    return str(exc) if isinstance(exc, ValueError) else type(exc).__name__


def members(kube):
    pods = json.loads(kube("-n", manage.NS, "get", "pods", "-l", "canonical-store=true", "-o", "json", capture=True).stdout)["items"]
    result = []
    for pod in pods:
        labels = pod["metadata"]["labels"]
        node = pod["spec"].get("nodeName")
        if not node:
            # A replacement may still be Pending because its local hosting
            # node is absent. The bound PV retains that slot's placement.
            claims = [v["persistentVolumeClaim"]["claimName"] for v in pod["spec"].get("volumes", []) if "persistentVolumeClaim" in v]
            if len(claims) == 1:
                claim = json.loads(kube("-n", manage.NS, "get", "pvc", claims[0], "-o", "json", capture=True).stdout)
                volume = claim["spec"].get("volumeName")
                if volume:
                    pv = json.loads(kube("get", "pv", volume, "-o", "json", capture=True).stdout)
                    terms = pv["spec"].get("nodeAffinity", {}).get("required", {}).get("nodeSelectorTerms", [])
                    if len(terms) == 1:
                        hosts = [v for e in terms[0].get("matchExpressions", [])
                                 if e["key"] == "kubernetes.io/hostname" and e["operator"] == "In" for v in e["values"]]
                        if len(hosts) == 1:
                            node = hosts[0]
        result.append({"name": pod["metadata"]["name"], "group": int(labels["citus-group"]),
                       "node": node, "ip": pod["status"].get("podIP"),
                       "primary": labels.get("postgres-role") == "primary"})
    if len(result) != 6 or {p["group"] for p in result} != {0, 1, 2}:
        raise ValueError("Expected the six existing Patroni members")
    for group in range(3):
        pair = [p for p in result if p["group"] == group]
        if len(pair) != 2 or None in {p["node"] for p in pair} or len({p["node"] for p in pair}) != 2:
            raise ValueError("Each group must have two slots on distinct nodes")
    return result


def operate(kube, action, *, node=None, group=None):
    pods = members(kube)
    leaders = [p for p in pods if p["group"] == 0 and p["primary"] and p["node"] != node]
    if len(leaders) != 1:
        raise ValueError("Wait for the coordinator's safe Patroni election")
    request = {"action": action, "pods": pods, "node": node, "group": group}
    return kube("-n", manage.NS, "exec", "-i", leaders[0]["name"], "--",
                "/opt/patroni/bin/python", "/opt/canonical/recovery.py",
                data=json.dumps(request), capture=True).stdout


def check_fence(kube, node, executable, *, development=False):
    if not node or (not development and not executable):
        raise ValueError("degrade requires --node and an absolute --fence-check executable")
    info = json.loads(kube("get", "node", node, "-o", "json", capture=True).stdout)
    labels = info["metadata"].get("labels", {})
    if any(key in labels for key in ("node-role.kubernetes.io/control-plane", "node-role.kubernetes.io/master")):
        raise ValueError("Recovery scenarios must retain the control plane")
    if node not in {p["node"] for p in members(kube)}:
        raise ValueError("Node has no declared database member")
    if development and not executable:
        import dev
        if node not in dev.node_names():
            raise ValueError("Only the dedicated development worker can use Docker fencing")
        result = subprocess.run(["docker", "inspect", "--format", "{{json .State}}\n{{json .Config.Labels}}", node],
                                check=True, text=True, capture_output=True, timeout=10)
        state, labels = [json.loads(line) for line in result.stdout.splitlines()]
        if state.get("Running") is not False or state.get("Status") != "exited" or labels.get("io.x-k8s.kind.cluster") != dev.NAME:
            raise ValueError("The development worker container must remain stopped")
        return
    if not Path(executable).is_absolute() or not Path(executable).is_file():
        raise ValueError("Use an absolute --fence-check executable")
    # This is a trusted administrator's authoritative power/fencing probe, not
    # a Kubernetes Ready condition or failed ping. It must succeed only while
    # this specific node cannot run its old primary or reach the storage network.
    subprocess.run([executable, node], check=True, timeout=15, stdout=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "check", "configure", "degrade", "repair", "drain", "enable", "plan", "rebalance"))
    parser.add_argument("--dev", action="store_true")
    parser.add_argument("--node")
    parser.add_argument("--fence-check")
    parser.add_argument("--group", type=int, choices=(1, 2))
    parser.add_argument("--timeout", type=int, default=180)
    args = parser.parse_args()
    if args.timeout < 1 or args.timeout > 1800:
        parser.error("timeout must be between 1 and 1800 seconds")
    if args.action in ("drain", "enable") and args.group is None:
        parser.error("drain/enable requires --group 1 or 2")
    if args.dev:
        import dev
        kube = dev.kube
    else:
        kube = manage.kubectl
    if args.action == "degrade":
        check_fence(kube, args.node, args.fence_check, development=args.dev)
    # Repair waits for Patroni to clone/replay an empty replacement; it never
    # requests /reinitialize, which would erase an existing member's data.
    deadline = time.monotonic() + args.timeout
    while True:
        try:
            print(operate(kube, args.action, node=args.node, group=args.group), end="", flush=True)
            return
        except (subprocess.CalledProcessError, ValueError) as exc:
            if args.action != "repair" or time.monotonic() >= deadline:
                raise
            print("Waiting for repair: " + error_text(exc) + " (retrying in 5s)", file=sys.stderr, flush=True)
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        print("Recovery action failed: " + error_text(exc), file=sys.stderr)
        sys.exit(1)
