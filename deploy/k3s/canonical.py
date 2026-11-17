#!/usr/bin/env python3
"""Render/install/check the opt-in A07 canonical storage overlay."""
import argparse
import base64
import json
import os
import re
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
from urllib.parse import quote

import manage

IMAGE = "bnpb.local/canonical:pg16-citus13.0.1-patroni4.1.5-a08"
PAIRS = {"a": ("node-control", "node-app"), "b": ("node-app", "node-store"),
         "c": ("node-store", "node-control")}
ACCOUNTS = [f"canonical-{group}-{slot}" for group in PAIRS for slot in ("primary", "replica")]
KEYS = ("CANONICAL_APP_PASSWORD", "PATRONI_SUPERUSER_PASSWORD", "PATRONI_REPLICATION_PASSWORD",
        "PATRONI_API_PASSWORD", "AUTHORITY_TOKEN", "POLLER_0_TOKEN", "POLLER_1_TOKEN", "POLLER_2_TOKEN", "FIELD_TOKEN", "SATELLITE_TOKEN")


def object_(kind, name, **fields):
    return {"apiVersion": "v1", "kind": kind, "metadata": {"name": name, "namespace": manage.NS}, **fields}


def resources():
    docs = []
    for index, (group, nodes) in enumerate(PAIRS.items()):
        docs.append(object_("Service", f"canonical-{group}", spec={
            "selector": {"canonical-group": group, "postgres-role": "primary"},
            "ports": [{"name": "postgres", "port": 5432, "targetPort": "postgres"}]}))
        for slot, node in zip(("primary", "replica"), nodes):
            name = f"canonical-{group}-{slot}"
            labels = {"app.kubernetes.io/name": name, "app.kubernetes.io/part-of": "bnpb", "role": "store",
                      "canonical-store": "true", "canonical-group": group, "patroni-scope": "bnpb-canonical",
                      "citus-group": str(index)}
            docs.append(object_("ServiceAccount", name, automountServiceAccountToken=False))
            docs.append(object_("Service", name, spec={"clusterIP": "None", "publishNotReadyAddresses": True,
                "selector": {"app.kubernetes.io/name": name}, "ports": [{"name": "postgres", "port": 5432}]}))
            env = [{"name": "CITUS_GROUP", "value": str(index)}, {"name": "INITIAL_REPLICA", "value": str(slot == "replica").lower()}]
            for key, field in (("POD_NAME", "metadata.name"), ("POD_IP", "status.podIP"), ("POD_NAMESPACE", "metadata.namespace")):
                env.append({"name": key, "valueFrom": {"fieldRef": {"fieldPath": field}}})
            for key in KEYS[:4]:
                env.append({"name": key, "valueFrom": {"secretKeyRef": {"name": "bnpb-canonical-cluster", "key": key}}})
            pod = {"serviceAccountName": name, "automountServiceAccountToken": False,
                "nodeSelector": {"store-eligible": "true"},
                "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "runAsNonRoot": True,
                                    "fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
                "terminationGracePeriodSeconds": 60,
                "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{
                    "labelSelector": {"matchLabels": {"canonical-group": group}},
                    "topologyKey": "kubernetes.io/hostname"}]}},
                "containers": [{"name": "postgres", "image": IMAGE, "imagePullPolicy": "IfNotPresent", "env": env,
                    "resources": {"requests": {"memory": "512Mi"}, "limits": {"memory": "1Gi"}},
                    "ports": [{"name": "postgres", "containerPort": 5432}, {"name": "patroni", "containerPort": 8008}],
                    "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                        "capabilities": {"drop": ["ALL"]}},
                    "startupProbe": {"httpGet": {"path": "/liveness", "port": "patroni"}, "periodSeconds": 5, "failureThreshold": 60},
                    "livenessProbe": {"httpGet": {"path": "/liveness", "port": "patroni"}, "periodSeconds": 10, "failureThreshold": 6},
                    "readinessProbe": {"httpGet": {"path": "/readiness", "port": "patroni"}, "periodSeconds": 5},
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}, {"name": "tmp", "mountPath": "/tmp"},
                                     {"name": "kube-api", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": True}]}],
                "volumes": [{"name": "data", "persistentVolumeClaim": {"claimName": name + "-data"}},
                            {"name": "tmp", "emptyDir": {}},
                            {"name": "kube-api", "projected": {"sources": [
                                {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
                                {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                                {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"fieldPath": "metadata.namespace"}}]}}]}}]}
            docs.append({"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": {"name": name, "namespace": manage.NS},
                "spec": {"replicas": 1, "serviceName": name, "selector": {"matchLabels": {"app.kubernetes.io/name": name}},
                         "template": {"metadata": {"labels": labels}, "spec": pod}}})
    # Patroni requires list/watch and pod label updates within this namespace.
    # It has no Secret/PV/exec privileges. Endpoint DCS records share one scope.
    docs.extend([
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "canonical-patroni", "namespace": manage.NS},
         "rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch", "patch", "update"]},
                   {"apiGroups": [""], "resources": ["endpoints", "configmaps"], "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"]}]},
        {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "canonical-patroni", "namespace": manage.NS},
         "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "canonical-patroni"},
         "subjects": [{"kind": "ServiceAccount", "name": name, "namespace": manage.NS} for name in ACCOUNTS]},
        object_("ServiceAccount", "canonical-admin", automountServiceAccountToken=False),
        {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "canonical-admin", "namespace": manage.NS},
         "spec": {"podSelector": {"matchLabels": {"canonical-store": "true"}}, "policyTypes": ["Ingress"],
                  "ingress": [{"from": [{"podSelector": {"matchLabels": {"app.kubernetes.io/name": "canonical-admin"}}}],
                               "ports": [{"protocol": "TCP", "port": 5432}]}]}},
        {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {"name": "canonical-poller-http", "namespace": manage.NS},
         "spec": {"podSelector": {"matchLabels": {"app.kubernetes.io/name": "bnpb", "app.kubernetes.io/part-of": "bnpb"}},
                  "policyTypes": ["Ingress"], "ingress": [{"from": [{"podSelector": {"matchLabels": {
                      "app.kubernetes.io/name": "bnpb", "app.kubernetes.io/part-of": "bnpb"}}}],
                      "ports": [{"protocol": "TCP", "port": 8085}]}]}}])
    return docs


def patches():
    # Separate overlay: no migration, credential or PVC ownership changes to M1.
    result = [{"target": {"kind": "Deployment", "name": "bnpb"}, "patch": json.dumps([
        {"op": "replace", "path": "/spec/replicas", "value": 3},
        {"op": "replace", "path": "/spec/strategy", "value": {"type": "RollingUpdate"}},
        {"op": "remove", "path": "/spec/template/spec/volumes/1"},
        {"op": "remove", "path": "/spec/template/spec/containers/0/volumeMounts/1"},
        {"op": "replace", "path": "/spec/template/spec/containers/0/readinessProbe/httpGet", "value": {"path": "/ready", "port": "http"}},
        {"op": "add", "path": "/spec/template/spec/topologySpreadConstraints", "value": [{"maxSkew": 1,
            "topologyKey": "kubernetes.io/hostname", "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": {"app.kubernetes.io/name": "bnpb"}}}]},
        {"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {"name": "CANONICAL_STORAGE", "value": "postgres"}},
        *[{"op": "add", "path": "/spec/template/spec/containers/0/env/-", "value": {"name": key,
            "valueFrom": {"secretKeyRef": {"name": "bnpb-canonical-app", "key": key}}}} for key in ("CANONICAL_DSN", "CANONICAL_CREDENTIALS")],
    ])}]
    text = (manage.ROOT / "deploy/kubernetes/base/admission.yaml").read_text()
    operations = []
    for name, expression in re.findall(r"  - name: (claims|secrets|roles)\n    expression: '(.*?)'\n(?=  - name:)", text, re.S):
        mapping = json.loads(expression)
        if name == "claims":
            mapping.update({account: [account + "-data"] for account in ACCOUNTS})
            mapping["canonical-admin"] = []
        elif name == "secrets":
            mapping["bnpb"].append("bnpb-canonical-app")
            mapping.update({account: ["bnpb-canonical-cluster"] for account in ACCOUNTS})
            mapping["canonical-admin"] = ["bnpb-canonical-admin"]
        else:
            mapping.update({account: "store" for account in ACCOUNTS})
            mapping["canonical-admin"] = "other"
        index = {"claims": 1, "secrets": 2, "roles": 3}[name]
        operations.append({"op": "replace", "path": f"/spec/variables/{index}/expression", "value": json.dumps(mapping)})
    if len(operations) != 3:
        raise ValueError("Admission variables changed; review canonical ownership patch")
    projection = ('!has(object.spec.volumes) || object.spec.volumes.all(v, '
        '((variables.owner == "alloy" || variables.owner in ' + json.dumps(ACCOUNTS) + ') && v.name == "kube-api" '
        '&& has(v.projected) && v.projected.sources.all(s, '
        '(has(s.serviceAccountToken) && s.serviceAccountToken.path == "token" && s.serviceAccountToken.expirationSeconds <= 3600) '
        '|| (has(s.configMap) && s.configMap.name == "kube-root-ca.crt") '
        '|| (has(s.downwardAPI) && s.downwardAPI.items.all(i, i.path == "namespace" && has(i.fieldRef) && i.fieldRef.fieldPath == "metadata.namespace")))) '
        '|| has(v.emptyDir) || has(v.configMap) || (has(v.persistentVolumeClaim) && v.persistentVolumeClaim.claimName in variables.claims[variables.owner]))')
    operations.append({"op": "replace", "path": "/spec/validations/1/expression", "value": projection})
    result.append({"target": {"kind": "ValidatingAdmissionPolicy", "name": "bnpb-private-storage"}, "patch": json.dumps(operations)})
    return result


def render(tag="dev", extra_patches=None):
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("Invalid image tag")
    parent = manage.ROOT / "deploy/kubernetes"
    with tempfile.TemporaryDirectory(prefix=".canonical-render-", dir=parent) as directory:
        Path(directory, "resources.json").write_text(json.dumps({"apiVersion": "v1", "kind": "List", "items": resources()}))
        overlay = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
            "resources": ["../base", "resources.json"], "patches": patches() + (extra_patches or []),
            "images": [{"name": "bnpb.local/" + name, "newTag": tag} for name in manage.APPS]}
        Path(directory, "kustomization.yaml").write_text(json.dumps(overlay))
        return manage.run([os.getenv("KUBECTL", "kubectl"), "kustomize", directory], capture=True,
                          env={**os.environ, "KUBECONFIG": os.devnull}).stdout


def apply_secrets(apply):
    path = manage.HERE / ".canonical.env"
    if not path.exists():
        existing = apply("-n", manage.NS, "get", "secret", "bnpb-canonical-cluster", "--ignore-not-found", "-o", "name", capture=True).stdout.strip()
        if existing:
            raise ValueError("Restore .canonical.env for the existing database; refusing to regenerate credentials")
        manage.private_write(path, "".join(f"{key}={secrets.token_hex(32)}\n" for key in KEYS))
    if path.stat().st_mode & 0o077:
        raise ValueError("Canonical credentials require mode 600")
    values = dict(line.split("=", 1) for line in path.read_text().splitlines() if line and not line.startswith("#"))
    if any(not values.get(key) for key in KEYS):
        raise ValueError("Missing canonical credential; never regenerate credentials for existing databases")
    if len({values[key] for key in KEYS}) != len(KEYS) or any(len(values[key]) < 32 for key in KEYS):
        raise ValueError("Canonical passwords/tokens must be distinct and at least 32 characters")
    principals = {
        values["AUTHORITY_TOKEN"]: {"owner": "coordination-authority", "authority": True},
        values["FIELD_TOKEN"]: {"owner": "field-report-consumer", "assignments": {"field-reports": ["BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"]}},
        values["SATELLITE_TOKEN"]: {"owner": "satellite-processor", "assignments": {"SATELIT": ["SATELIT"]}},
    }
    for index in range(3):
        principals[values[f"POLLER_{index}_TOKEN"]] = {"owner": f"poller-{index}", "assignments": {
            "BMKG": ["BMKG"], "PVMBG": ["PVMBG"], "SENSOR_PANTAI": ["SENSOR_PANTAI"]}}
    host = "canonical-a:5432/canonical?sslmode=disable&connect_timeout=5"
    definitions = {
        "bnpb-canonical-cluster": {key: values[key] for key in KEYS[:4]},
        "bnpb-canonical-admin": {"CANONICAL_ADMIN_DSN": "postgresql://patroni:" + quote(values["PATRONI_SUPERUSER_PASSWORD"], safe="") + "@" + host},
        "bnpb-canonical-app": {"CANONICAL_DSN": "postgresql://canonical_app:" + quote(values["CANONICAL_APP_PASSWORD"], safe="") + "@" + host,
                               "CANONICAL_CREDENTIALS": json.dumps(principals)},
    }
    docs = [object_("Secret", name, type="Opaque", data={k: base64.b64encode(v.encode()).decode() for k, v in data.items()})
            for name, data in definitions.items()]
    apply("apply", "--server-side", "--field-manager=bnpb-canonical", "-f", "-", data=json.dumps({"apiVersion": "v1", "kind": "List", "items": docs}))


def check_backend(kube):
    result = kube("-n", manage.NS, "get", "deployment", "bnpb", "--ignore-not-found", "-o", "json", capture=True).stdout.strip()
    if not result:
        return
    deployment = json.loads(result)
    env = deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])
    current = next((e.get("value") for e in env if e["name"] == "CANONICAL_STORAGE"), "sqlite")
    if current == "postgres" and os.getenv("CANONICAL_STORAGE") != "postgres":
        raise ValueError("This deployment uses PostgreSQL; set CANONICAL_STORAGE=postgres to avoid reverting to SQLite")


def initialize(apply, tag):
    for name in ACCOUNTS:
        apply("-n", manage.NS, "rollout", "status", "statefulset/" + name, "--timeout=240s")
    # Migration job is immutable; remove only this disposable job, never data.
    apply("-n", manage.NS, "delete", "job", "canonical-schema", "--ignore-not-found", "--wait=true")
    pod = {"serviceAccountName": "canonical-admin", "automountServiceAccountToken": False, "restartPolicy": "Never",
           "nodeSelector": {"app-eligible": "true"},
           "securityContext": {"runAsUser": 1000, "runAsGroup": 1000, "runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
           "containers": [{"name": "schema", "image": f"bnpb.local/bnpb:{tag}", "imagePullPolicy": "IfNotPresent",
                "command": ["/app/canonical-admin", "migrate"],
                "resources": {"requests": {"memory": "128Mi"}, "limits": {"memory": "256Mi"}},
                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                "env": [{"name": "CANONICAL_ADMIN_DSN", "valueFrom": {"secretKeyRef": {"name": "bnpb-canonical-admin", "key": "CANONICAL_ADMIN_DSN"}}}]}]}
    job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "canonical-schema", "namespace": manage.NS},
           "spec": {"backoffLimit": 3, "template": {"metadata": {"labels": {"app.kubernetes.io/name": "canonical-admin",
                   "app.kubernetes.io/part-of": "bnpb", "role": "other"}}, "spec": pod}}}
    apply("apply", "-f", "-", data=json.dumps(job))
    apply("-n", manage.NS, "wait", "--for=condition=complete", "job/canonical-schema", "--timeout=240s")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("render", "check"))
    parser.add_argument("--dev", action="store_true")
    args = parser.parse_args()
    if args.action == "render":
        print(render(os.getenv("IMAGE_TAG", "dev")), end="")
        return
    if args.dev:
        import dev
        kube = dev.kube
    else:
        kube = manage.kubectl
    kube("-n", manage.NS, "get", "pods", "-l", "canonical-store=true", "-o", "wide")
    pods = json.loads(kube("-n", manage.NS, "get", "pods", "-l", "canonical-store=true", "-o", "json", capture=True).stdout)["items"]
    if len(pods) != 6:
        raise ValueError("Expected six canonical database instances")
    for group in PAIRS:
        pair = [p for p in pods if p["metadata"]["labels"].get("canonical-group") == group]
        nodes = {p["spec"].get("nodeName") for p in pair}
        if len(pair) != 2 or len(nodes) != 2 or None in nodes:
            raise ValueError("Each database group requires two distinct hosting nodes")
        if any(not any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", [])) for p in pair):
            raise ValueError("Every canonical instance must be Ready")
    # SQL is fixed and credentials stay inside the database container.
    leader = json.loads(kube("-n", manage.NS, "get", "pods", "-l", "canonical-group=a,postgres-role=primary", "-o", "json", capture=True).stdout)["items"]
    if len(leader) != 1:
        raise ValueError("Expected one coordinator primary")
    query = "SELECT canonical.replication_ready(); SELECT groupid,nodename,nodeport,shouldhaveshards FROM pg_dist_node WHERE noderole='primary'; SELECT nodename,count(*) FROM citus_shards WHERE table_name='canonical.events'::regclass GROUP BY nodename;"
    result = kube("-n", manage.NS, "exec", leader[0]["metadata"]["name"], "--", "psql", "-h", "/tmp", "-U", "patroni", "-d", "canonical", "-v", "ON_ERROR_STOP=1", "-Atc", query, capture=True).stdout
    print(result, end="")
    if result.splitlines()[0] != "t":
        raise ValueError("Cross-node synchronous replication is not ready")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        print(f"Canonical action failed: {exc}", file=sys.stderr)
        sys.exit(1)
