#!/usr/bin/env python3
"""Verify one real Client API -> Aggregator request in metrics, Tempo and Loki."""
import argparse
import contextlib
import json
import os
import re
import secrets
import select
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import manage


@contextlib.contextmanager
def forward(service, port, dev):
    config = manage.HERE / '.dev/kubeconfig' if dev else manage.HERE / 'kubeconfig'
    if not dev:
        config = os.getenv('KUBECONFIG') or config
    command = [os.getenv('KUBECTL') or 'kubectl', '--kubeconfig', str(config),
               '-n', manage.NS, 'port-forward', '--address', '127.0.0.1', 'service/' + service, ':' + str(port)]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.monotonic() + 20
        base = None
        while time.monotonic() < deadline and process.poll() is None:
            if not select.select([process.stdout], [], [], 1)[0]:
                continue
            line = process.stdout.readline()
            match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)', line)
            if match:
                base = 'http://127.0.0.1:' + match[1]
                break
        if base is None:
            raise ValueError(f'Port-forward for {service} unavailable')
        yield base
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        process.stdout.close()


def request(base, path, body=None, headers=None):
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json', **(headers or {})}
    req = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response), response.headers


def trace_services(document):
    services = set()
    def walk(item):
        if isinstance(item, dict):
            if item.get('key') == 'service.name':
                value = item.get('value', {}).get('stringValue')
                if value:
                    services.add(value)
            for value in item.values():
                walk(value)
        elif isinstance(item, list):
            for value in item:
                walk(value)
    walk(document)
    return services


def run(dev=False):
    creds = manage.credentials()
    with contextlib.ExitStack() as stack:
        api = stack.enter_context(forward('client-api', 8080, dev))
        prometheus = stack.enter_context(forward('prometheus', 9090, dev))
        tempo = stack.enter_context(forward('tempo', 3200, dev))
        loki = stack.enter_context(forward('loki', 3100, dev))
        auth, _ = request(api, '/auth/login', {'username':'field-user', 'password':creds['FIELD_USER_PASSWORD']})
        token = auth['data']['accessToken']
        correlation = 'telemetry-' + secrets.token_hex(8)
        trace = secrets.token_hex(16)
        start = time.time_ns()
        _, headers = request(api, '/hazards?source=BMKG&limit=1', headers={
            'Authorization':'Bearer ' + token, 'X-Correlation-ID':correlation,
            'traceparent':f'00-{trace}-{secrets.token_hex(8)}-01'})
        if headers.get('X-Correlation-ID') != correlation:
            raise ValueError('Client API did not preserve request correlation ID')
        deadline = time.monotonic() + 60
        observed = set()
        logs = set()
        metric_services = set()
        while time.monotonic() < deadline:
            try:
                data, _ = request(tempo, '/api/traces/' + trace)
                observed = trace_services(data)
                query = '{namespace="' + manage.NS + '"} | json | correlation_id="' + correlation + '"'
                data, _ = request(loki, '/loki/api/v1/query_range?' + urllib.parse.urlencode({
                    'query':query, 'start':start, 'end':time.time_ns(), 'limit':100}))
                logs = set()
                for stream in data['data']['result']:
                    for _, line in stream['values']:
                        event = json.loads(line)
                        if event.get('correlation_id') == correlation and event.get('trace_id') == trace:
                            logs.add(event.get('service'))
                data, _ = request(prometheus, '/api/v1/query?' + urllib.parse.urlencode({
                    'query':'sum by (service) (bnpb_http_requests_total{route="GET /hazards"})'}))
                metric_services = {r['metric']['service'] for r in data['data']['result'] if float(r['value'][1]) >= 1}
                required = {'client-api', 'bnpb'}
                if all(required <= s for s in [observed, logs, metric_services]):
                    print(json.dumps({'correlation_id':correlation, 'trace_id':trace,
                          'trace_services':sorted(observed), 'log_services':sorted(logs),
                          'metric_services':sorted(metric_services)}))
                    return
            except (OSError, ValueError, KeyError):
                pass  # Backend ingestion is asynchronous; retry within a bounded window.
            time.sleep(2)
        raise ValueError(f'Missing telemetry evidence: trace={sorted(observed)}, logs={sorted(logs)}, metrics={sorted(metric_services)}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    try:
        run(args.dev)
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
        # Never include request bodies, credentials or tokens in diagnostics.
        print(f'Telemetry smoke failed: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
