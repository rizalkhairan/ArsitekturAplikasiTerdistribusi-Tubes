import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "deploy/k3s"))
import recovery as cli

spec = importlib.util.spec_from_file_location("recovery_operations", ROOT / "deploy/canonical/recovery.py")
operations = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operations)


class Connection:
    autocommit = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def commit(self):
        pass


def pods():
    layout = (("control", "app"), ("app", "store"), ("store", "control"))
    return [{"name": f"g{g}-{s}", "group": g, "node": node, "ip": f"10.0.{g}.{s+1}"}
            for g, pair in enumerate(layout) for s, node in enumerate(pair)]


def surviving_groups():
    members = pods()
    return [{"group": g, "host": members[i]["ip"], "leader": members[i], "strict": True,
             "streaming": [[members[i ^ 1]["name"], "sync"]]}
            for g, i in enumerate((0, 3, 4))]


class RecoverySafetyTest(unittest.TestCase):
    def run_action(self, action, mode="replicated", fenced=None, **request):
        events = []

        def query(connection, sql, params=None):
            events.append((sql, params))
            if "pg_try_advisory_lock" in sql:
                return [(True,)]
            if "SELECT durability,fenced_node" in sql:
                return [(mode, fenced)]
            if "replication_ready" in sql:
                return [(True,)]
            return []

        def api(host, path, body=None):
            events.append((path, body))

        with patch.object(operations, "connect", return_value=Connection()), \
                patch.object(operations, "query", side_effect=query), \
                patch.object(operations, "formation", return_value=surviving_groups()), \
                patch.object(operations, "api", side_effect=api):
            result = operations.run({"action": action, "pods": pods(), **request})
        return events, result

    def test_downgrade_is_recorded_before_relaxing_strict_replication(self):
        events, result = self.run_action("degrade", node="app")
        update = next(i for i, e in enumerate(events) if "UPDATE canonical.recovery" in e[0])
        patches = [i for i, e in enumerate(events) if e[0] == "/config"]
        self.assertEqual(len(patches), 2)
        self.assertTrue(all(i > update for i in patches))
        self.assertEqual(result["durability"], "single_copy")

    def test_second_lost_node_is_rejected_until_repair(self):
        with self.assertRaisesRegex(ValueError, "Repair the first"):
            self.run_action("degrade", mode="single_copy", fenced="store", node="app")

    def test_degradation_cannot_leave_a_primary_on_the_fenced_node(self):
        with self.assertRaisesRegex(ValueError, "safe Patroni promotion"):
            self.run_action("degrade", node="store")

    def test_repair_requires_replay_and_restores_strict_before_policy(self):
        with patch.object(operations, "verify_copies") as verify:
            events, result = self.run_action("repair", mode="single_copy", fenced="app")
        self.assertEqual(verify.call_count, 2)
        update = next(i for i, e in enumerate(events) if "UPDATE canonical.recovery" in e[0])
        patches = [i for i, e in enumerate(events) if e[0] == "/config"]
        self.assertEqual(len(patches), 3)
        self.assertTrue(all(i < update for i in patches))
        self.assertEqual(result["durability"], "replicated")

    def test_failed_replay_does_not_restore_replicated_acknowledgements(self):
        with patch.object(operations, "verify_copies", side_effect=ValueError("behind")), \
                patch.object(operations, "set_policy") as policy:
            with self.assertRaisesRegex(ValueError, "behind"):
                self.run_action("repair", mode="single_copy", fenced="app")
            policy.assert_not_called()

    def test_replica_evidence_is_bound_to_the_disk_and_replay_barrier(self):
        for row in ((False, "system", True), (True, "wrong-disk", True), (True, "system", False)):
            with self.subTest(row=row), patch.object(operations, "connect", return_value=Connection()), \
                    patch.object(operations, "api"), \
                    patch.object(operations, "query", side_effect=[[("system", "0/1234")], [row]]):
                with self.assertRaisesRegex(ValueError, "WAL barrier"):
                    operations.verify_copies(surviving_groups()[:1], pods())

    def test_colocated_replica_cannot_pass_repair(self):
        members = pods()
        members[1]["node"] = "control"
        with self.assertRaisesRegex(ValueError, "distinct hosting node"):
            operations.verify_copies(surviving_groups()[:1], members)

    def test_stale_streaming_member_does_not_substitute_for_declared_replica(self):
        groups = surviving_groups()
        groups[0]["streaming"] = [["old-member", "sync"]]
        with self.assertRaisesRegex(ValueError, "replica is absent"):
            operations.verify_copies(groups, pods())

    def test_membership_is_refused_in_degraded_mode(self):
        with self.assertRaisesRegex(ValueError, "repaired strict"):
            self.run_action("rebalance", mode="single_copy", fenced="app")

    def test_fencing_is_not_inferred_from_kubernetes_readiness(self):
        with self.assertRaisesRegex(ValueError, "fence-check"):
            cli.check_fence(None, "worker", None)
        info = {"metadata": {"labels": {"node-role.kubernetes.io/control-plane": ""}}}
        with self.assertRaisesRegex(ValueError, "retain the control plane"):
            cli.check_fence(lambda *a, **k: SimpleNamespace(stdout=json.dumps(info)), "control", "/bin/true")

    def test_development_fence_requires_a_stopped_owned_container(self):
        import dev
        node = dev.node_names()[1]
        kube = lambda *a, **k: SimpleNamespace(stdout=json.dumps({"metadata": {"labels": {}}}))
        for running, cluster in ((True, dev.NAME), (False, "unrelated"), (False, dev.NAME)):
            state = {"Running": running, "Status": "running" if running else "exited"}
            result = SimpleNamespace(stdout=json.dumps(state) + "\n" + json.dumps({"io.x-k8s.kind.cluster": cluster}))
            with self.subTest(running=running, cluster=cluster), \
                    patch.object(cli, "members", return_value=[{"node": node}]), \
                    patch.object(cli.subprocess, "run", return_value=result):
                if running or cluster != dev.NAME:
                    with self.assertRaisesRegex(ValueError, "remain stopped"):
                        cli.check_fence(kube, node, None, development=True)
                else:
                    cli.check_fence(kube, node, None, development=True)

    def test_pending_empty_slot_keeps_its_bound_pv_membership(self):
        members = pods()
        resources = [{"metadata": {"name": p["name"], "labels": {"citus-group": str(p["group"])}},
                      "spec": {"nodeName": p["node"]}, "status": {"podIP": p["ip"]}} for p in members]
        resources[1]["spec"] = {"volumes": [{"persistentVolumeClaim": {"claimName": "replica-data"}}]}
        resources[1]["status"] = {}
        claim = {"spec": {"volumeName": "replica-pv"}}
        pv = {"spec": {"nodeAffinity": {"required": {"nodeSelectorTerms": [{"matchExpressions": [
            {"key": "kubernetes.io/hostname", "operator": "In", "values": ["app"]}]}]}}}}
        responses = [{"items": resources}, claim, pv]
        with patch.object(cli, "manage") as manage:
            manage.NS = "test"
            result = cli.members(lambda *a, **k: SimpleNamespace(stdout=json.dumps(responses.pop(0))))
        self.assertEqual(result[1]["node"], "app")
        self.assertIsNone(result[1]["ip"])


if __name__ == "__main__":
    unittest.main()
