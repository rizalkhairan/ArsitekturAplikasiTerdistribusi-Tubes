import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/k3s"))
import canonical
import dev


class CanonicalOverlayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docs = list(yaml.safe_load_all(canonical.render("test")))

    def test_pairs_use_six_private_volumes_on_distinct_nodes(self):
        stores = [d for d in self.docs if d["kind"] == "StatefulSet"]
        self.assertEqual(len(stores), 6)
        pvs = {d["metadata"]["name"]: d["spec"] for d in self.docs if d["kind"] == "PersistentVolume"}
        claims = {d["metadata"]["name"]: d["spec"] for d in self.docs if d["kind"] == "PersistentVolumeClaim"}
        seen = set()
        for group, expected_nodes in canonical.PAIRS.items():
            actual_nodes = []
            for slot in ("primary", "replica"):
                name = f"canonical-{group}-{slot}"
                store = next(s for s in stores if s["metadata"]["name"] == name)
                pod = store["spec"]["template"]["spec"]
                self.assertEqual(pod["serviceAccountName"], name)
                volume = next(v for v in pod["volumes"] if "persistentVolumeClaim" in v)
                claim = volume["persistentVolumeClaim"]["claimName"]
                self.assertNotIn(claim, seen)
                seen.add(claim)
                pv = pvs[claims[claim]["volumeName"]]
                actual_nodes.append(pv["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0])
                self.assertIn("requiredDuringSchedulingIgnoredDuringExecution", pod["affinity"]["podAntiAffinity"])
                container = pod["containers"][0]
                self.assertEqual(container["resources"], {"requests": {"memory": "512Mi"}, "limits": {"memory": "1Gi"}})
                self.assertFalse(pod["automountServiceAccountToken"])
                self.assertEqual(store["spec"]["template"]["metadata"]["labels"]["role"], "store")
            self.assertEqual(tuple(actual_nodes), expected_nodes)

    def test_admission_grants_only_owned_claims_and_secrets(self):
        policy = next(d for d in self.docs if d["kind"] == "ValidatingAdmissionPolicy")
        variables = {v["name"]: json.loads(v["expression"]) for v in policy["spec"]["variables"] if v["name"] in ("claims", "secrets", "roles")}
        for account in canonical.ACCOUNTS:
            self.assertEqual(variables["claims"][account], [account + "-data"])
            self.assertEqual(variables["secrets"][account], ["bnpb-canonical-cluster"])
        self.assertNotIn("bnpb-canonical-cluster", variables["secrets"]["bnpb"])
        self.assertNotIn("bnpb-canonical-admin", variables["secrets"]["bnpb"])
        role = next(d for d in self.docs if d["kind"] == "Role" and d["metadata"]["name"] == "canonical-patroni")
        self.assertFalse(any("secrets" in r["resources"] or "persistentvolumes" in r["resources"] for r in role["rules"]))

    def test_aggregator_has_no_sqlite_claim_and_can_scale_above_node_count(self):
        aggregator = next(d for d in self.docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "bnpb")
        self.assertEqual(aggregator["spec"]["replicas"], 3)
        pod = aggregator["spec"]["template"]["spec"]
        self.assertFalse(any("persistentVolumeClaim" in v for v in pod["volumes"]))
        self.assertNotIn("affinity", pod)
        env = {e["name"]: e for e in pod["containers"][0]["env"]}
        self.assertEqual(env["CANONICAL_STORAGE"]["value"], "postgres")
        self.assertEqual(env["CANONICAL_DSN"]["valueFrom"]["secretKeyRef"]["name"], "bnpb-canonical-app")

    def test_development_does_not_fold_canonical_replicas(self):
        with patch.dict(os.environ, {"CANONICAL_STORAGE": "postgres", "DEV_NODES": "2"}):
            with self.assertRaisesRegex(ValueError, "DEV_NODES=3"):
                dev.render()
        with patch.dict(os.environ, {"CANONICAL_STORAGE": "postgres", "DEV_NODES": "3"}):
            docs = list(yaml.safe_load_all(dev.render()))
            nodes = {d["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0]
                     for d in docs if d["kind"] == "PersistentVolume"}
            self.assertEqual(nodes, set(dev.node_names()))

    def test_patroni_configuration_has_one_scope_strict_sync_and_citus_groups(self):
        spec = importlib.util.spec_from_file_location("patroni_start", ROOT / "deploy/canonical/start.py")
        start = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(start)
        env = {key: "test-password" for key in canonical.KEYS[:4]}
        env.update({"POD_IP": "10.0.0.1", "POD_NAME": "store", "POD_NAMESPACE": canonical.manage.NS})
        for group in range(3):
            with patch.dict(os.environ, {**env, "CITUS_GROUP": str(group)}):
                config = start.config()
            self.assertEqual(config["scope"], "bnpb-canonical")
            self.assertEqual(config["citus"]["group"], group)
            self.assertEqual(config["kubernetes"]["role_label"], "postgres-role")
            self.assertTrue(config["bootstrap"]["dcs"]["synchronous_mode_strict"])
            self.assertEqual(config["bootstrap"]["dcs"]["postgresql"]["parameters"]["synchronous_commit"], "remote_apply")
            self.assertEqual(config["postgresql"]["parameters"]["unix_socket_directories"], "/tmp")

    def test_redeployment_cannot_silently_revert_to_sqlite(self):
        deployment={"spec":{"template":{"spec":{"containers":[{"env":[{"name":"CANONICAL_STORAGE","value":"postgres"}]}]}}}}
        with patch.dict(os.environ,{"CANONICAL_STORAGE":""}):
            with self.assertRaisesRegex(ValueError,"reverting to SQLite"):
                canonical.check_backend(lambda *a,**k: SimpleNamespace(stdout=json.dumps(deployment)))

    def test_missing_local_credentials_cannot_replace_existing_database_passwords(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(canonical.manage,"HERE",Path(directory)):
            with self.assertRaisesRegex(ValueError,"refusing to regenerate credentials"):
                canonical.apply_secrets(lambda *a,**k:SimpleNamespace(stdout="secret/bnpb-canonical-cluster"))
            self.assertFalse(Path(directory,".canonical.env").exists())


if __name__ == "__main__":
    unittest.main()
