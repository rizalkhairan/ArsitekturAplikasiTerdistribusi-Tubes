import copy
import hashlib
import http.client
import json
import threading
import unittest
from http.server import ThreadingHTTPServer

from server import Failure, ROOT, State, handler


class ContractTest(unittest.TestCase):
    def setUp(self):
        self.state = State(json.loads((ROOT / "contracts/m2/fixtures.json").read_text()))
        self.body = json.loads((ROOT / "contracts/m2/write-example.json").read_text())

    def test_retry_conflict_and_authority(self):
        status, first = self.state.write("SATELIT", "new", self.body)
        self.assertEqual(status, 201)
        self.assertFalse(first["replayed"])
        retry = copy.deepcopy(self.body)
        retry["correlation_id"] = "new-trace"
        self.assertTrue(self.state.write("SATELIT", "new", retry)[1]["replayed"])
        retry["event"]["severity"] = "AWAS"
        with self.assertRaises(Failure) as ctx:
            self.state.write("SATELIT", "new", retry)
        self.assertEqual(ctx.exception.code, "version_conflict")
        retry["authority"]["epoch"] = 0
        with self.assertRaises(Failure) as ctx:
            self.state.write("SATELIT", "new", retry)
        self.assertEqual(ctx.exception.code, "stale_authority")

    def test_tombstone_blocks_delayed_replay(self):
        retract = {k: v for k, v in self.body.items() if k != "event"}
        retract.update(version=2, operation="retract")
        self.state.write("SATELIT", "new", retract)
        self.assertFalse(any(e["source_ref_id"] == "new" for e in self.state.visible()))
        with self.assertRaises(Failure) as ctx:
            self.state.write("SATELIT", "new", self.body)
        self.assertEqual(ctx.exception.code, "stale_version")
        self.body["version"] = 3
        self.state.write("SATELIT", "new", self.body)
        self.assertTrue(any(e["source_ref_id"] == "new" for e in self.state.visible()))

    def test_snapshot_stable_during_mutations(self):
        page = self.state.list_events({"limit": "2"})
        before = self.state.visible()
        self.state.write("SATELIT", "new", self.body)
        last = self.state.list_events({"cursor": page["next_cursor"], "limit": "2"})
        self.assertEqual(page["items"] + last["items"], before)
        self.assertIsNone(last["next_cursor"])
        with self.assertRaises(Failure):
            self.state.list_events({"cursor": page["next_cursor"], "source": "BMKG"})

    def test_partial_and_since_ingestion(self):
        self.state.unavailable = ["shard-B"]
        page = self.state.list_events({"since": "2026-11-12T02:50:00Z"})
        self.assertTrue(page["partial"])
        self.assertEqual(page["unavailable_units"], ["shard-B"])
        self.assertEqual(len(page["items"]), 3)
        self.assertEqual(self.state.list_events({"since": "2026-11-12T03:00:00Z"})["items"], [])

    def test_http_audit_and_scope(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler(self.state, "audit", "writer"))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def request(path, token="audit", method="GET", body=None):
            connection = http.client.HTTPConnection(*server.server_address, timeout=3)
            connection.request(method, path, body=json.dumps(body) if body is not None else None,
                               headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
            response = connection.getresponse()
            result = response.status, json.loads(response.read())
            connection.close()
            return result
        try:
            status, result = request("/internal/audit/checksum?source=SATELIT")
            self.assertEqual(status, 200)
            self.assertEqual(result["count"], 1)
            self.assertEqual(result["checksum"], hashlib.sha256(b"fixture-2").hexdigest())
            self.assertEqual(request("/internal/audit/events", "wrong")[0], 401)
            self.assertEqual(request("/internal/audit/events", "writer")[0], 403)
            self.assertEqual(request("/internal/canonical/events/SATELIT/new", method="PUT")[0], 403)
            status, receipt = request("/internal/canonical/events/SATELIT/new", "writer", "PUT", self.body)
            self.assertEqual(status, 201)
            self.assertEqual(receipt["durability"], "simulated")
            self.assertTrue(request("/internal/canonical/events/SATELIT/new", "writer", "PUT", self.body)[1]["replayed"])
            self.assertEqual(request("/internal/audit/location?hazard_id=absent")[0], 404)
            self.state.unavailable = ["shard-B"]
            self.assertEqual(request("/internal/audit/location?hazard_id=absent")[0], 503)
            self.assertEqual(request("/internal/audit/unprocessed")[0], 503)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
