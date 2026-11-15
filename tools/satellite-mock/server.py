#!/usr/bin/env python3
"""Small deterministic SSE source used only by tools/p4-curl-test.sh."""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TOKEN = os.environ.get("SATELLITE_MOCK_TOKEN", "local-satellite-test-token")
lock = threading.Condition()
events = []
generation = 0
active_streams = 0
session = {"group": "local-test", "profile": "deterministic", "speed": 60,
           "virtual_now": "2026-11-16T00:00:00Z"}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, status, value):
        body = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def authorized(self):
        return self.headers.get("Authorization") == "Bearer " + TOKEN

    def do_GET(self):
        global active_streams
        parsed = urlparse(self.path)
        print(f"GET {parsed.path}", flush=True)
        if not self.authorized():
            self.send_json(401, {"error": {"code": "unauthorized"}})
            return
        if parsed.path == "/v1/session":
            with lock:
                value = dict(session)
                value["latest_offset"] = len(events) - 1 if events else 0
            self.send_json(200, value)
            return
        if parsed.path != "/v1/stream":
            self.send_json(404, {"error": {"code": "not_found"}})
            return
        query = parse_qs(parsed.query)
        start = int(query.get("from", ["0"])[0])
        last = self.headers.get("Last-Event-ID")
        if last:
            start = int(last) + 1
        with lock:
            if active_streams:
                self.send_json(429, {"error": {"code": "too_many_connections"}})
                return
            active_streams += 1
            stream_generation = generation
        # A completed test stream must reach EOF at the client so its
        # reconnect loop can run; BaseHTTPRequestHandler otherwise keeps the
        # HTTP/1.1 connection open for a subsequent request.
        self.close_connection = True
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        try:
            cursor = start
            while True:
                with lock:
                    if stream_generation != generation:
                        break
                    next_event = events[cursor] if cursor < len(events) else None
                    if next_event is None:
                        lock.wait(timeout=1)
                        if stream_generation != generation:
                            break
                        next_event = events[cursor] if cursor < len(events) else None
                if next_event is None:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                offset, kind, payload = next_event
                frame = (f"id: {offset}\nevent: {kind}\ndata: "
                         + json.dumps(payload, separators=(",", ":")) + "\n\n")
                self.wfile.write(frame.encode())
                self.wfile.flush()
                cursor += 1
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with lock:
                active_streams -= 1
                lock.notify_all()

    def do_POST(self):
        global generation, events
        parsed = urlparse(self.path)
        size = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(size)
        try:
            value = json.loads(body or b"{}")
        except json.JSONDecodeError:
            self.send_json(400, {"error": {"code": "invalid_json"}})
            return
        # Admin endpoints are intentionally present only in this test mock.
        if parsed.path == "/__admin/events":
            batch = value.get("events", [])
            with lock:
                first = len(events)
                for item in batch:
                    events.append((len(events), item["type"], item.get("data", {})))
                last = len(events) - 1
                lock.notify_all()
            self.send_json(201, {"first_offset": first, "latest_offset": last})
            return
        if parsed.path == "/__admin/disconnect":
            with lock:
                generation += 1
                lock.notify_all()
            self.send_json(200, {"disconnected": True})
            return
        if parsed.path == "/__admin/reset":
            with lock:
                generation += 1
                events = []
                lock.notify_all()
            self.send_json(200, {"reset": True})
            return
        if parsed.path == "/__admin/summary":
            with lock:
                self.send_json(200, {"latest_offset": len(events) - 1 if events else 0, "active_streams": active_streams})
            return
        self.send_json(404, {"error": {"code": "not_found"}})

    def log_message(self, _fmt, *_args):
        return


port = int(os.environ.get("PORT", "8098"))
ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
