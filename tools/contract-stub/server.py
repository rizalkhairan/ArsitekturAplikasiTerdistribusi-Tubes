"""Ephemeral A02 contract simulator. Never use for real accepted data."""
import argparse
import copy
import hashlib
import hmac
import json
import math
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit, parse_qs

ROOT = Path(__file__).resolve().parents[2]
SOURCES = {"BMKG", "PVMBG", "SATELIT", "SENSOR_PANTAI"}
TYPES = {"SEISMIC", "VOLCANIC", "WILDFIRE", "COASTAL"}
SEVERITIES = {"NORMAL", "WASPADA", "SIAGA", "AWAS"}


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Failure(Exception):
    def __init__(self, status, code, message):
        self.status, self.code, self.message = status, code, message


def require(condition, code="invalid_request", message="Invalid request", status=400):
    if not condition:
        raise Failure(status, code, message)


def timestamp(value):
    require(isinstance(value, str), message="Timestamp must be ISO 8601 UTC")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        require(result.tzinfo is not None and result.utcoffset().total_seconds() == 0,
                message="Timestamp must be UTC")
        return result
    except ValueError:
        raise Failure(400, "invalid_request", "Invalid timestamp")


class State:
    def __init__(self, fixtures, unavailable=()):
        self.lock = threading.RLock()
        self.records = {}
        self.snapshots = {}
        self.unavailable = sorted(set(unavailable))
        for row in fixtures:
            row = copy.deepcopy(row)
            event = {k: v for k, v in row["event"].items()
                     if k not in {"hazard_id", "source", "source_ref_id", "ingested_at"}}
            row["digest"] = self.digest(row["operation"], event if row["operation"] == "upsert" else None)
            self.records[(row["event"]["source"], row["event"]["source_ref_id"])] = row

    @staticmethod
    def digest(operation, event):
        return hashlib.sha256(json.dumps([operation, event], sort_keys=True, ensure_ascii=False,
                                        separators=(",", ":"), allow_nan=False).encode()).hexdigest()

    def visible(self, source=None, since=None):
        return sorted([copy.deepcopy(r["event"]) for r in self.records.values()
                       if r["operation"] == "upsert" and r["unit"] not in self.unavailable
                       and (not source or r["event"]["source"] == source)
                       and (not since or timestamp(r["event"]["ingested_at"]) >= timestamp(since))],
                      key=lambda e: (timestamp(e["ingested_at"]), e["hazard_id"]))

    def coverage(self):
        return {"partial": bool(self.unavailable), "unavailable_units": self.unavailable[:]}

    def list_events(self, q):
        source, since = q.get("source"), q.get("since")
        require(not source or source in SOURCES, message="Unknown source")
        if since:
            timestamp(since)
        try:
            limit = int(q.get("limit", "100"))
        except ValueError:
            raise Failure(400, "invalid_request", "Invalid limit")
        require(1 <= limit <= 1000, message="limit must be 1..1000")
        # Expire abandoned snapshots as well as consumed ones.
        self.snapshots = {k: v for k, v in self.snapshots.items() if v[0] > time.monotonic()}
        cursor = q.get("cursor")
        if cursor:
            require(cursor in self.snapshots, "cursor_expired", "Unknown or expired cursor")
            expiry, filters, rows, coverage = self.snapshots[cursor]
            require(filters == (source, since), message="Cursor filters changed")
        else:
            expiry, filters = time.monotonic() + 300, (source, since)
            rows, coverage = self.visible(source, since), self.coverage()
        next_cursor = None
        if len(rows) > limit:
            next_cursor = uuid.uuid4().hex
            self.snapshots[next_cursor] = (expiry, filters, rows[limit:], coverage)
        return {"items": rows[:limit], "next_cursor": next_cursor, **coverage}

    def write(self, source, ref, body):
        require(source in SOURCES and ref and "/" not in ref, message="Invalid event identity")
        require(isinstance(body, dict), message="Expected object")
        require(set(body) <= {"version", "operation", "authority", "correlation_id", "event"},
                message="Unknown envelope field")
        version, operation = body.get("version"), body.get("operation")
        require(type(version) is int and version > 0, message="version must be a positive integer")
        require(operation in ("upsert", "retract"), message="Unknown operation")
        require(isinstance(body.get("correlation_id"), str) and body["correlation_id"],
                message="correlation_id required")
        authority = body.get("authority")
        require(isinstance(authority, dict) and authority.get("partition") == source
                and type(authority.get("epoch")) is int and authority["epoch"] == 1,
                "stale_authority", "Stub requires source partition at epoch 1", 409)
        event = body.get("event")
        if operation == "upsert":
            fields = {"hazard_type", "severity", "area_name", "latitude", "longitude", "occurred_at", "attributes"}
            require(isinstance(event, dict) and set(event) == fields, message="Invalid event fields")
            require(event["hazard_type"] in TYPES and event["severity"] in SEVERITIES,
                    message="Invalid hazard type/severity")
            require(isinstance(event["area_name"], str) and event["area_name"], message="area_name required")
            require(isinstance(event["attributes"], dict), message="attributes must be an object")
            for name, bound in (("latitude", 90), ("longitude", 180)):
                value = event[name]
                require(type(value) in (int, float) and math.isfinite(value) and abs(value) <= bound,
                        message="Invalid coordinate")
            timestamp(event["occurred_at"])
        else:
            require("event" not in body, message="Retraction must omit event")
        digest = self.digest(operation, event)
        key = (source, ref)
        old = self.records.get(key)
        if old:
            require(version >= old["version"], "stale_version", "Older version", 409)
            if version == old["version"]:
                require(digest == old.get("digest"), "version_conflict", "Version already used", 409)
                return 200, self.receipt(old, True)
        identity = "hz-" + hashlib.sha256(json.dumps(key, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        hazard_id = old["event"]["hazard_id"] if old else identity
        ingested = old["event"]["ingested_at"] if old else now()
        canonical = {**(event or {}), "source": source, "source_ref_id": ref,
                     "hazard_id": hazard_id, "ingested_at": ingested}
        row = {"event": canonical, "version": version, "operation": operation, "digest": digest,
               "unit": "shard-A", "locations": [
                   {"node": "node-control", "unit": "shard-A", "role": "primary"},
                   {"node": "node-app", "unit": "shard-A-r1", "role": "replica"}]}
        self.records[key] = row
        return (200 if old else 201), self.receipt(row, False)

    @staticmethod
    def receipt(row, replayed):
        return {"hazard_id": row["event"]["hazard_id"], "version": row["version"],
                "operation": row["operation"], "replayed": replayed, "durability": "simulated"}


def handler(state, audit_token, write_token):
    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, value):
            payload = json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            self.dispatch()

        def do_PUT(self):
            self.dispatch()

        def do_POST(self):
            self.dispatch()

        def dispatch(self):
            try:
                with state.lock:
                    status, result = self.route()
                self.reply(status, result)
            except Failure as exc:
                self.reply(exc.status, {"error": {"code": exc.code, "message": exc.message}})
            except (ValueError, TypeError, UnicodeError, KeyError):
                self.reply(400, {"error": {"code": "invalid_request", "message": "Malformed request"}})

        def route(self):
            parsed = urlsplit(self.path)
            path = parsed.path
            if path == "/health" and self.command == "GET":
                return 200, {"status": "ok", "mode": "ephemeral-contract-stub"}
            auth = self.headers.get("Authorization", "")
            is_audit = hmac.compare_digest(auth, "Bearer " + audit_token)
            is_writer = hmac.compare_digest(auth, "Bearer " + write_token)
            require(is_audit or is_writer, "unauthorized", "Invalid or missing token", 401)
            audit = path.startswith("/internal/audit/")
            require(is_audit if audit else is_writer, "forbidden", "Insufficient scope", 403)
            q = parse_qs(parsed.query, keep_blank_values=True)
            require(all(len(v) == 1 and v[0] for v in q.values()), message="Empty or repeated parameter")
            q = {k: v[0] for k, v in q.items()}
            if path in ("/internal/audit/events", "/internal/canonical/events") and self.command == "GET":
                return 200, state.list_events(q)
            if path == "/internal/audit/checksum" and self.command == "GET":
                source = q.get("source")
                require(not source or source in SOURCES, message="Unknown source")
                rows = state.visible(source)
                refs = sorted((e["source_ref_id"].encode() for e in rows))
                return 200, {"source": source, "count": len(rows),
                             "checksum": hashlib.sha256(b"\n".join(refs)).hexdigest(),
                             "algorithm": "sha256", "as_of": now(), **state.coverage()}
            if path == "/internal/audit/location" and self.command == "GET":
                require(bool(q.get("hazard_id")), message="hazard_id required")
                for row in state.records.values():
                    if row["event"]["hazard_id"] == q["hazard_id"] and row["operation"] == "upsert":
                        locations = [x for x in row["locations"] if x["unit"] not in state.unavailable]
                        return 200, {"hazard_id": q["hazard_id"], "locations": locations,
                                     **(state.coverage() if state.unavailable else {"partial": False})}
                if state.unavailable:
                    raise Failure(503, "coverage_unavailable", "Cannot establish absence")
                raise Failure(404, "not_found", "Unknown hazard")
            if path == "/internal/audit/unprocessed" or path == "/field-reports":
                raise Failure(503, "owner_unavailable", "C-owned route is not simulated")
            prefix = "/internal/canonical/events/"
            if path.startswith(prefix) and self.command == "PUT":
                parts = path[len(prefix):].split("/")
                require(len(parts) == 2, message="Expected source/source_ref_id")
                length = int(self.headers.get("Content-Length", "0"))
                require(0 < length <= 1048576, message="Body must be 1..1048576 bytes")
                require(self.headers.get_content_type() == "application/json", message="Expected application/json")
                body = json.loads(self.rfile.read(length), parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
                return state.write(*(unquote(p) for p in parts), body)
            raise Failure(404, "not_found", "Unknown route or method")

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8095)
    parser.add_argument("--fixtures", type=Path, default=ROOT / "contracts/m2/fixtures.json")
    parser.add_argument("--unavailable-unit", action="append", default=[])
    args = parser.parse_args()
    audit, writer = os.getenv("AUDIT_TOKEN"), os.getenv("WRITE_TOKEN")
    if not audit or not writer or audit == writer:
        parser.error("Set distinct AUDIT_TOKEN and WRITE_TOKEN")
    state = State(json.loads(args.fixtures.read_text()), args.unavailable_unit)
    server = ThreadingHTTPServer((args.host, args.port), handler(state, audit, writer))
    print(f"Ephemeral contract stub: http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
