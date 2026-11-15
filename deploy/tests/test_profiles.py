import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
import xml.etree.ElementTree as ET

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/k3s"))
import bootstrap
import dev


class BootstrapTest(unittest.TestCase):
    def test_cloud_init_has_key_auth_os_packages_and_network_reservations(self):
        cfg = bootstrap.read_config()
        key = "ssh-ed25519 AAAATEST unit-test"
        config = yaml.safe_load(bootstrap.cloud_config(cfg, cfg["workers"][0], key))
        self.assertFalse(config["ssh_pwauth"])
        self.assertTrue(config["users"][0]["lock_passwd"])
        self.assertEqual(config["users"][0]["ssh_authorized_keys"], [key])
        self.assertIn("qemu-guest-agent", config["packages"])
        self.assertIn("curl", config["packages"])
        network = ET.fromstring(bootstrap.network_xml(cfg, str(uuid.uuid4())))
        self.assertEqual(network.find("forward").get("mode"), "nat")
        self.assertEqual({(h.get("mac"), h.get("ip")) for h in network.findall("./ip/dhcp/host")},
                         {(w["mac"], w["ip"]) for w in cfg["workers"]})

    def test_checksum_selects_exact_filename_and_rejects_missing(self):
        digest = hashlib.sha512(b"verified OS").hexdigest()
        self.assertEqual(bootstrap.checksum_for(digest + "  image.qcow2\n", "image.qcow2"), digest)
        with self.assertRaises(ValueError):
            bootstrap.checksum_for(digest + "  wrong-image.qcow2\n", "image.qcow2")

    def test_reused_cache_is_verified_and_never_silently_redownloaded(self):
        cfg = bootstrap.read_config()
        with tempfile.TemporaryDirectory() as tmp, patch.object(bootstrap, "STATE", Path(tmp)), \
             patch.object(bootstrap.urllib.request, "urlopen", side_effect=AssertionError("unexpected network request")):
            image = Path(tmp, "debian.qcow2")
            image.write_bytes(b"verified image")
            digest = hashlib.sha512(image.read_bytes()).hexdigest()
            Path(tmp, "image.json").write_text(json.dumps({"url": cfg["image_url"], "checksums_url": cfg["checksums_url"], "sha512": digest}))
            self.assertEqual(bootstrap.verified_image(cfg), image)
            image.write_bytes(b"corrupt")
            with self.assertRaises(ValueError):
                bootstrap.verified_image(cfg)

    def test_wsl_is_rejected_before_vm_actions(self):
        with patch.object(bootstrap.platform, "system", return_value="Linux"), \
             patch.object(bootstrap.platform, "release", return_value="microsoft-standard-WSL2"):
            with self.assertRaisesRegex(ValueError, "native Linux"):
                bootstrap.preflight(bootstrap.read_config())

    def test_memory_guard_prevents_overcommitting_vm_boot(self):
        cfg = bootstrap.read_config()
        original_exists = Path.exists
        def exists(path):
            return True if str(path) == "/dev/kvm" else original_exists(path)
        with patch.object(bootstrap.platform, "release", return_value="linux"), \
             patch.object(bootstrap.platform, "machine", return_value="x86_64"), \
             patch.object(bootstrap.shutil, "which", return_value="/bin/tool"), \
             patch.object(Path, "exists", exists), \
             patch.object(Path, "read_text", return_value="MemTotal: 7400000 kB\nMemAvailable: 2000000 kB\n"):
            with self.assertRaisesRegex(ValueError, "available"):
                bootstrap.preflight(cfg, set())

    def test_unmanaged_network_is_never_replaced(self):
        cfg = bootstrap.read_config()
        xml = bootstrap.network_xml(cfg, str(uuid.uuid4()))
        with patch.object(bootstrap, "names", return_value={cfg["network"]}), \
             patch.object(bootstrap, "virsh", return_value=xml) as command:
            with self.assertRaisesRegex(ValueError, "unmanaged"):
                bootstrap.ensure_network(cfg, {"network_uuid": str(uuid.uuid4())})
            self.assertEqual(command.call_count, 1)

    def test_stop_refuses_unowned_vm(self):
        with patch.object(bootstrap, "load_state", return_value={"workers": {}}), \
             patch.object(bootstrap, "names", return_value={"node-app"}), \
             patch.object(bootstrap, "virsh", return_value=str(uuid.uuid4())) as command:
            with self.assertRaisesRegex(ValueError, "unmanaged"):
                bootstrap.stop()
            self.assertEqual(command.call_count, 1)


class DevProfileTest(unittest.TestCase):
    def test_existing_node_count_is_remembered_for_repeat_commands(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(dev, "STATE", Path(tmp)), patch.dict(os.environ, {"DEV_NODES": ""}):
            Path(tmp, "cluster.json").write_text(json.dumps({"nodes": [{"role": "control-plane"}]}))
            self.assertEqual(dev.count(), 1)
            with patch.dict(os.environ, {"DEV_NODES": "3"}):
                self.assertEqual(dev.count(), 3)

    def test_memory_check_fails_before_cluster_creation(self):
        with patch.dict(os.environ, {"DEV_NODES": "1"}), patch.object(dev.manage, "run") as command:
            command.return_value.stdout = json.dumps({"MemTotal": 1800 * 1024 * 1024})
            with self.assertRaisesRegex(ValueError, "at least 3 GiB"):
                dev.docker_ready()
            self.assertEqual(command.call_count, 1)

    def test_three_node_profile_maps_every_local_pv_to_a_kind_node(self):
        with patch.dict(os.environ, {"DEV_NODES": "3"}):
            docs = list(yaml.safe_load_all(dev.render("profile-test")))
            self.assertEqual(len(dev.config()["nodes"]), 3)
            seen = set()
            for doc in docs:
                if doc["kind"] == "PersistentVolume":
                    node = doc["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0]
                    self.assertIn(node, dev.node_names())
                    seen.add(node)
            self.assertEqual(seen, set(dev.node_names()))
            client = next(d for d in docs if d["kind"] == "Service" and d["metadata"]["name"] == "client-api")
            self.assertEqual(client["spec"]["type"], "ClusterIP")
            self.assertNotIn("nodePort", client["spec"]["ports"][0])

    def test_single_node_profile_consolidates_all_volume_affinities(self):
        with patch.dict(os.environ, {"DEV_NODES": "1"}):
            docs = list(yaml.safe_load_all(dev.render()))
            self.assertEqual(len(dev.config()["nodes"]), 1)
            nodes = {d["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0]
                     for d in docs if d["kind"] == "PersistentVolume"}
            self.assertEqual(nodes, {"bnpb-dev-control-plane"})

    def test_two_node_profile_keeps_control_data_and_combines_worker_locations(self):
        with patch.dict(os.environ, {"DEV_NODES": "2"}):
            plan = dev.config()
            self.assertEqual([node["role"] for node in plan["nodes"]], ["control-plane", "worker"])
            self.assertEqual(dev.placement(), {"node-control": "bnpb-dev-control-plane",
                                             "node-app": "bnpb-dev-worker", "node-store": "bnpb-dev-worker"})
            docs = list(yaml.safe_load_all(dev.render("two-node-test")))
            volumes = {d["metadata"]["name"]: d for d in docs if d["kind"] == "PersistentVolume"}
            for logical, paths in dev.manage.PATHS.items():
                for path in paths:
                    node = volumes["bnpb-" + path]["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0]
                    self.assertEqual(node, dev.placement()[logical])
            for deployment in (d for d in docs if d["kind"] == "Deployment"):
                for container in deployment["spec"]["template"]["spec"]["containers"]:
                    self.assertIn("memory", container["resources"]["requests"])

    def test_two_node_memory_guard_accepts_three_gib_desktop_usable_memory(self):
        with patch.dict(os.environ, {"DEV_NODES": "2"}), patch.object(dev.manage, "run") as command:
            command.return_value.stdout = json.dumps({"MemTotal": 3045281792})
            dev.docker_ready()

    def test_default_make_shows_help_and_never_bootstraps(self):
        result = subprocess.run(["make", "--no-print-directory"], cwd=ROOT, capture_output=True, text=True, check=True)
        self.assertIn("make bootstrap", result.stdout)
        self.assertNotIn("bootstrap.py up", result.stdout)

    def test_kind_teardown_refuses_unmanaged_cluster(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(dev, "STATE", Path(tmp)), patch.object(dev.manage, "run") as command:
            with self.assertRaisesRegex(ValueError, "refusing deletion"):
                dev.down()
            command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
