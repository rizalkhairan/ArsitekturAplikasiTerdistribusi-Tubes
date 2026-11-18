#!/usr/bin/env python3
"""Install and inspect the single-instance operational telemetry stack."""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request

import manage

IMAGES = {
    'prometheus': 'docker.io/prom/prometheus:v3.5.0',
    'grafana': 'docker.io/grafana/grafana:12.1.0',
    'tempo': 'docker.io/grafana/tempo:2.8.2',
    'loki': 'docker.io/grafana/loki:3.5.3',
    'alloy': 'docker.io/grafana/alloy:v1.10.2',
    'otel-collector': 'docker.io/otel/opentelemetry-collector-contrib:0.131.0',
}
ROOT = manage.ROOT / 'deploy/kubernetes/observability'


def render():
    return manage.run([os.getenv('KUBECTL') or 'kubectl', 'kustomize', ROOT], capture=True,
                      env={**os.environ, 'KUBECONFIG': os.devnull}).stdout


def install(kube=manage.kubectl):
    kube('apply', '-f', '-', data=render())
    for name in IMAGES:
        kube('-n', manage.NS, 'rollout', 'status', 'deployment/' + name, '--timeout=60s')


def validate_grafana():
    # Mount the same provisioning files and run with the deployment's UID,
    # filesystem and memory restrictions. Use only disposable credentials/data.
    name = 'bnpb-grafana-check-' + secrets.token_hex(6)
    password = secrets.token_hex(24)
    with tempfile.TemporaryDirectory(prefix='bnpb-grafana-check-') as directory:
        env = Path(directory, 'env')
        env.write_text('\n'.join([
            'GF_PATHS_DATA=/data', 'GF_PATHS_LOGS=/data/logs', 'GF_PATHS_PLUGINS=/data/plugins',
            'GF_PATHS_PROVISIONING=/etc/observability/provisioning',
            'GF_SECURITY_ADMIN_USER=admin', 'GF_SECURITY_ADMIN_PASSWORD=' + password,
            'GF_ANALYTICS_REPORTING_ENABLED=false', 'GF_ANALYTICS_CHECK_FOR_UPDATES=false',
            'GF_PLUGINS_PREINSTALL_DISABLED=true',
        ]) + '\n')
        env.chmod(0o600)
        args = ['docker', 'run', '--rm', '-d', '--name', name, '--user', '1000:1000',
                '--memory', '256m', '--read-only', '--cap-drop', 'ALL',
                '--security-opt', 'no-new-privileges', '--tmpfs', '/data:uid=1000,gid=1000',
                '--tmpfs', '/tmp:uid=1000,gid=1000', '--env-file', env,
                '-p', '127.0.0.1::3000']
        files = {'datasources.yaml':'provisioning/datasources/default.yaml',
                 'dashboards.yaml':'provisioning/dashboards/default.yaml', 'services.json':'dashboards/services.json'}
        for source, target in files.items():
            args += ['--mount', f'type=bind,src={ROOT / "config" / source},dst=/etc/observability/{target},readonly']
        try:
            manage.run([*args, IMAGES['grafana']], capture=True)
            base = 'http://' + manage.run(['docker', 'port', name, '3000/tcp'], capture=True).stdout.strip()
            deadline = time.monotonic() + 60
            auth = base64.b64encode(('admin:' + password).encode()).decode()
            while True:
                try:
                    req = urllib.request.Request(base + '/api/dashboards/uid/bnpb-services', headers={'Authorization':'Basic ' + auth})
                    with urllib.request.urlopen(req, timeout=5) as response:
                        dashboard = json.load(response)['dashboard']
                    if dashboard['title'] != 'BNPB service operations':
                        raise ValueError('Unexpected Grafana dashboard')
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise ValueError('Grafana did not provision the dashboard within 60 seconds') from None
                    time.sleep(1)
            print('Grafana startup and provisioned operations dashboard verified.')
        finally:
            # The name is generated here and is never an existing application container.
            subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def validate():
    """Run the pinned backend binaries' own config validators in disposable containers."""
    cases = [
        ('prometheus', ['/bin/promtool', 'check', 'config', '/cfg/prometheus.yaml']),
        ('tempo', ['-config.file=/cfg/tempo.yaml', '-config.verify=true']),
        ('loki', ['-config.file=/cfg/loki.yaml', '-verify-config=true']),
        ('alloy', ['validate', '/cfg/alloy.alloy']),
        ('otel-collector', ['validate', '--config=/cfg/otel-collector.yaml']),
    ]
    for name, args in cases:
        command = ['docker', 'run', '--rm', '--mount', f'type=bind,src={ROOT / "config"},dst=/cfg,readonly']
        if name == 'prometheus':
            command += ['--entrypoint', args[0]]
        manage.run([*command, IMAGES[name], *(args[1:] if name == 'prometheus' else args)])
    validate_grafana()


def check(kube=manage.kubectl):
    for name in IMAGES:
        kube('-n', manage.NS, 'rollout', 'status', 'deployment/' + name, '--timeout=60s')
    # Query through Grafana's internal networking so NetworkPolicies are exercised.
    command = ['wget', '-q', '-T', '10', '-O', '-']
    result = kube('-n', manage.NS, 'exec', 'deployment/grafana', '--', *command,
                  'http://prometheus:9090/api/v1/query?query=up%7Bjob%3D%22application%22%7D', capture=True)
    targets = json.loads(result.stdout)['data']['result']
    state = {r['metric'].get('service'): r['value'][1] for r in targets}
    required = ('client-api', 'bnpb', 'auth')
    if any(state.get(s) != '1' for s in required):
        raise ValueError('Instrumented application targets are not all up: ' + json.dumps(state))
    for url in ['http://loki:3100/ready', 'http://tempo:3200/ready']:
        kube('-n', manage.NS, 'exec', 'deployment/grafana', '--', *command, url, capture=True)
    print('Backend readiness and Client API/Aggregator/Auth scrapes verified. Trace/log request evidence requires telemetry-smoke.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['render', 'install', 'check', 'forward', 'validate'])
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    kube = manage.kubectl
    if args.dev:
        from dev import kube
    try:
        if args.action == 'render':
            print(render(), end='')
        elif args.action == 'validate':
            validate()
        elif args.action == 'forward':
            kube('-n', manage.NS, 'port-forward', '--address', '127.0.0.1', 'service/grafana', '3000:3000')
        else:
            {'install': install, 'check': check}[args.action](kube)
    except (ValueError, KeyError, OSError, subprocess.CalledProcessError) as exc:
        print(f'Observability action failed: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
