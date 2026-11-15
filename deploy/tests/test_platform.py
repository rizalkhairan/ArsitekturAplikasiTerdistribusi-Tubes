import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import subprocess
import types
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("platform_manage", ROOT / "deploy/k3s/manage.py")
manage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(manage)


def example_inventory():
    nodes = []
    for index, name in enumerate(manage.NAMES):
        nodes.append({"name": name, "ssh": "local" if index == 0 else "user@vm" + str(index),
                      "ip": "192.168.50." + str(10 + index), "kind": "native" if index == 0 else "vm",
                      "physical_host": "host", "host_cpu": "test CPU", "host_ram_gib": 32,
                      "node_cpu": 2, "node_ram_gib": 4})
    return {"nodes": nodes, "architecture": "amd64", "k3s_version": "v1.34.1+k3s1", "manage_control_plane": False}


class ManifestTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docs = list(yaml.safe_load_all(manage.render("contract-test")))
        cls.job = yaml.safe_load((ROOT / "deploy/kubernetes/base/topic-init.yaml").read_text())
        cls.workloads = [d for d in cls.docs if d["kind"] == "Deployment"] + [cls.job]
        cls.by_kind = {}
        for doc in cls.docs:
            cls.by_kind.setdefault(doc["kind"], {})[doc["metadata"]["name"]] = doc

    def test_identity_and_memory_classes(self):
        classes = {"aggregator": ("256Mi", "512Mi"), "store": ("512Mi", "1Gi"),
                   "broker": ("512Mi", "1Gi"), "mock": ("64Mi", "128Mi"),
                   "consumer": ("128Mi", "256Mi"), "api": ("128Mi", "256Mi"), "other": ("128Mi", "256Mi")}
        for workload in self.workloads:
            with self.subTest(workload=workload["metadata"]["name"]):
                self.assertEqual(workload["metadata"]["namespace"], manage.NS)
                template = workload["spec"]["template"]
                role = template["metadata"]["labels"]["role"]
                request, limit = classes[role]
                self.assertFalse(template["spec"]["automountServiceAccountToken"])
                for container in template["spec"]["containers"]:
                    self.assertEqual(container["resources"]["requests"]["memory"], request)
                    self.assertEqual(container["resources"]["limits"]["memory"], limit)
                    self.assertEqual(container["imagePullPolicy"], "IfNotPresent")

    def test_probes_and_nonoverlapping_sqlite_writers(self):
        deployments = self.by_kind["Deployment"]
        self.assertEqual(set(deployments), set(manage.APPS) | {"kafka"})
        for name, deployment in deployments.items():
            template = deployment["spec"]["template"]
            for key, value in deployment["spec"]["selector"]["matchLabels"].items():
                self.assertEqual(template["metadata"]["labels"][key], value)
            for container in template["spec"]["containers"]:
                for probe in ("startupProbe", "readinessProbe", "livenessProbe"):
                    self.assertIn(probe, container)
            if any("persistentVolumeClaim" in v for v in template["spec"].get("volumes", [])):
                self.assertEqual(deployment["spec"]["replicas"], 1)
                self.assertEqual(deployment["spec"]["strategy"]["type"], "Recreate")

    def test_volume_paths_placement_and_exclusive_mounts(self):
        pvs = self.by_kind["PersistentVolume"]
        pvcs = self.by_kind["PersistentVolumeClaim"]
        consumers = {}
        eligibility = {"node-control": {"app-eligible": "true", "store-eligible": "true", "aggregator-eligible": "true"},
                       "node-app": {"app-eligible": "true", "store-eligible": "true", "aggregator-eligible": "true"},
                       "node-store": {"app-eligible": "false", "store-eligible": "true", "aggregator-eligible": "true"}}
        self.assertEqual(len(pvs), 11)
        self.assertEqual(len(pvcs), 11)
        for claim_name, claim in pvcs.items():
            volume = pvs[claim["spec"]["volumeName"]]["spec"]
            self.assertEqual(volume["claimRef"], {"namespace": manage.NS, "name": claim_name})
            self.assertEqual(volume["persistentVolumeReclaimPolicy"], "Retain")
            self.assertEqual(volume["accessModes"], ["ReadWriteOnce"])
            path = volume["local"]["path"]
            node = volume["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0]
            self.assertIn(path.removeprefix("/var/lib/bnpb/"), manage.PATHS[node])
        for workload in self.workloads:
            pod = workload["spec"]["template"]["spec"]
            for volume in pod.get("volumes", []):
                if "persistentVolumeClaim" not in volume:
                    continue
                claim = volume["persistentVolumeClaim"]["claimName"]
                self.assertNotIn(claim, consumers, "A private claim is mounted by multiple services")
                consumers[claim] = workload["metadata"]["name"]
                pv = pvs[pvcs[claim]["spec"]["volumeName"]]["spec"]
                node = pv["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0]
                for key, value in pod["nodeSelector"].items():
                    self.assertEqual(eligibility[node][key], value, "Pod cannot run on its volume's node")
        self.assertFalse(any(name.startswith("canonical-") for name in consumers), "Future database slots must not be fake running stores")

    def test_service_and_configuration_references(self):
        configmaps = self.by_kind["ConfigMap"]
        secret_docs = []
        with patch.object(manage, "kubectl", side_effect=lambda *a, **kw: secret_docs.append(json.loads(kw["data"]))):
            manage.apply_secrets({key: "test-" + key for key in manage.SECRET_KEYS})
        secrets = {d["metadata"]["name"]: d["data"] for d in secret_docs[0]["items"]}
        for workload in self.workloads:
            for container in workload["spec"]["template"]["spec"]["containers"]:
                for ref in container.get("envFrom", []):
                    self.assertIn(ref["configMapRef"]["name"], configmaps)
                for env in container.get("env", []):
                    refs = env.get("valueFrom", {})
                    if "configMapKeyRef" in refs:
                        ref = refs["configMapKeyRef"]
                        self.assertIn(ref["key"], configmaps[ref["name"]]["data"])
                    if "secretKeyRef" in refs:
                        ref = refs["secretKeyRef"]
                        self.assertIn(ref["key"], secrets[ref["name"]])
                    if any(word in env["name"] for word in ("PASSWORD", "TOKEN", "API_KEY", "SECRET")):
                        self.assertIn("secretKeyRef", refs)
        for service in self.by_kind["Service"].values():
            matches = [w for w in self.workloads if all(w["spec"]["template"]["metadata"]["labels"].get(k) == v
                                                      for k, v in service["spec"]["selector"].items())]
            self.assertEqual(len(matches), 1)
            ports = {p["name"] for c in matches[0]["spec"]["template"]["spec"]["containers"] for p in c.get("ports", [])}
            for port in service["spec"]["ports"]:
                self.assertIn(port["targetPort"], ports)

    def test_render_is_private_and_tags_all_application_images(self):
        self.assertFalse(any(d["kind"] == "Secret" for d in self.docs))
        for name in manage.APPS:
            image = self.by_kind["Deployment"][name]["spec"]["template"]["spec"]["containers"][0]["image"]
            self.assertEqual(image, f"bnpb.local/{name}:contract-test")
        with self.assertRaises(ValueError):
            manage.render("bad tag/with spaces")

    def test_make_render_handles_unset_exported_options(self):
        result = subprocess.run(["make", "--no-print-directory", "render"], cwd=ROOT,
                                capture_output=True, text=True, check=True,
                                env={**os.environ, "IMAGE_TAG": ""})
        docs = list(yaml.safe_load_all(result.stdout))
        self.assertTrue(any(d["kind"] == "Deployment" for d in docs))


class ProvisioningTest(unittest.TestCase):
    def test_existing_control_plane_is_not_reinstalled_and_its_token_is_used(self):
        commands = []
        def ssh(node, command, **kwargs):
            commands.append((node["name"], command, kwargs.get("data", "")))
            if "node-token" in command:
                return types.SimpleNamespace(stdout="existing-join-token\n")
            return types.SimpleNamespace(stdout="server: https://127.0.0.1:6443\n")
        with tempfile.TemporaryDirectory() as tmp, patch.object(manage, "HERE", Path(tmp)), \
             patch.object(manage, "inventory", return_value=example_inventory()), \
             patch.object(manage, "credentials", return_value={"K3S_TOKEN": "new-unused-token"}), \
             patch.object(manage, "ssh", side_effect=ssh), patch.object(manage, "kubectl"), \
             patch.object(manage, "cluster_check"), contextlib.redirect_stdout(io.StringIO()):
            manage.provision()
            self.assertEqual(Path(tmp, "kubeconfig").stat().st_mode & 0o777, 0o600)
        control_scripts = [script for node, _, script in commands if node == "node-control"]
        self.assertFalse(any("get.k3s.io" in script or "config.yaml" in script for script in control_scripts))
        workers = [script for node, _, script in commands if node != "node-control" and "get.k3s.io" in script]
        self.assertEqual(len(workers), 2)
        self.assertTrue(all("existing-join-token" in script and "new-unused-token" not in script for script in workers))
        self.assertFalse(any("existing-join-token" in command for _, command, _ in commands))
        for script in workers:
            subprocess.run(["bash", "-n"], input=script, text=True, check=True)

    def test_init_preserves_credentials_and_private_modes(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(manage, "HERE", Path(tmp)), contextlib.redirect_stdout(io.StringIO()):
            Path(tmp, "inventory.example.json").write_text(json.dumps(example_inventory()))
            manage.init()
            original = Path(tmp, ".env").read_bytes()
            manage.init()
            self.assertEqual(Path(tmp, ".env").read_bytes(), original)
            self.assertEqual(Path(tmp, ".env").stat().st_mode & 0o777, 0o600)
            self.assertEqual(set(manage.credentials()), set(manage.SECRET_KEYS))
            self.assertEqual(len(manage.inventory()["nodes"]), 3)

    def test_inventory_rejects_container_nodes_and_unsafe_ssh(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(manage, "HERE", Path(tmp)):
            for field, value in (("kind", "container"), ("ssh", "-oProxyCommand=x@host"), ("node_ram_gib", 0)):
                inv = example_inventory()
                inv["nodes"][1][field] = value
                Path(tmp, "inventory.json").write_text(json.dumps(inv))
                with self.assertRaises(ValueError):
                    manage.inventory()

    def test_bundle_rejects_wrong_architecture_and_corruption(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"IMAGE_ARCHIVE": str(Path(tmp, "images.tar"))}):
            path = Path(tmp, "images.tar")
            path.write_bytes(b"test bundle")
            metadata = {"tag": "test", "architecture": "amd64", "sha256": manage.file_sha256(path),
                        "images": [f"bnpb.local/{name}:test" for name in manage.APPS] + [manage.KAFKA]}
            path.with_suffix(".json").write_text(json.dumps(metadata))
            self.assertEqual(manage.image_bundle({"architecture": "amd64"}), (path, "test"))
            with self.assertRaises(ValueError):
                manage.image_bundle({"architecture": "arm64"})
            path.write_bytes(b"corrupted bundle")
            with self.assertRaises(ValueError):
                manage.image_bundle({"architecture": "amd64"})


if __name__ == "__main__":
    unittest.main()
