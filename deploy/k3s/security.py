#!/usr/bin/env python3
"""Storage admission/network checks and unchanged assignment Tester Agent lifecycle."""
import argparse
import copy
import hashlib
import json
import subprocess
import sys

import manage

MANIFEST = manage.ROOT / 'deploy/tester-agent/manifest.yaml'
MANIFEST_SHA256 = '964c086918cc31d5e887958b70d0606070b1e0c514c0017231ca651d0efa0f39'
IMAGE = 'sustuna/tester-agent@sha256:b3143da159cf23a8744ede12b847da6208353754234d53a5a7b46d071cf277ec'


def install(kube=manage.kubectl):
    if hashlib.sha256(MANIFEST.read_bytes()).hexdigest() != MANIFEST_SHA256:
        raise ValueError('Supplied Tester Agent manifest changed; refusing installation')
    # Keep outside Kustomize: namespace/image/security/RBAC must remain supplied bytes.
    kube('apply', '-f', MANIFEST)


def allowed(kube, identity, verb, resource, namespace):
    try:
        result = kube('auth', 'can-i', verb, resource, '--as=' + identity,
                      '--as-group=system:authenticated',
                      '--as-group=system:serviceaccounts',
                      '--as-group=system:serviceaccounts:' + identity.split(':')[2],
                      '-n', namespace, capture=True)
    except subprocess.CalledProcessError as exc:
        # API/network/authentication failures are not evidence of a denied grant.
        if exc.returncode == 1 and (exc.stdout or '').strip() == 'no':
            return False
        raise
    if result.stdout.strip() not in ('yes', 'no'):
        raise ValueError('Unexpected kubectl auth can-i response')
    return result.stdout.strip() == 'yes'


def check_agent(kube=manage.kubectl):
    kube('-n', 'tester-agent', 'rollout', 'status', 'daemonset/tester-agent', '--timeout=60s')
    nodes = json.loads(kube('get', 'nodes', '-o', 'json', capture=True).stdout)['items']
    pods = json.loads(kube('-n', 'tester-agent', 'get', 'pods', '-l', 'app=tester-agent',
                           '-o', 'json', capture=True).stdout)['items']
    ready = [p for p in pods if not p['metadata'].get('deletionTimestamp') and
             any(c['type'] == 'Ready' and c['status'] == 'True' for c in p.get('status', {}).get('conditions', []))]
    if not nodes or len(ready) != len(nodes) or {p['spec']['nodeName'] for p in ready} != {n['metadata']['name'] for n in nodes}:
        raise ValueError('Tester Agent must be Ready exactly once on every node')
    if any(p['spec']['containers'][0]['image'] != IMAGE for p in ready):
        raise ValueError('Tester Agent image differs from assignment')
    identity = 'system:serviceaccount:tester-agent:tester-agent'
    for verb, resource, namespace in [('list','nodes',manage.NS), ('list','pods',manage.NS),
            ('delete','pods',manage.NS), ('list','services',manage.NS),
            *[(v,'leases.coordination.k8s.io','tester-agent') for v in ['get','list','create','update']],
            *[(v,'configmaps','tester-agent') for v in ['get','create','update','delete']]]:
        if not allowed(kube, identity, verb, resource, namespace):
            raise ValueError(f'Tester Agent RBAC blocked: {verb} {resource}')
    # Discovery on each node only. Never run a fault scenario implicitly.
    for pod in ready:
        kube('exec', '-n', 'tester-agent', pod['metadata']['name'], '--', '/app', 'list')
    print('Tester Agent ready on every node; supplied RBAC and practice discovery verified.')


def check_isolation(kube=manage.kubectl):
    policy = json.loads(kube('get', 'validatingadmissionpolicy', 'bnpb-private-storage',
                             '-o', 'json', capture=True).stdout)
    warnings = policy.get('status', {}).get('typeChecking', {}).get('expressionWarnings', [])
    if warnings:
        raise ValueError('Admission policy has CEL type-check warnings: ' + json.dumps(warnings))
    deployments = json.loads(kube('-n', manage.NS, 'get', 'deployments', '-o', 'json', capture=True).stdout)['items']
    by_name = {d['metadata']['name']: d for d in deployments}
    for name, deployment in by_name.items():
        identity = f'system:serviceaccount:{manage.NS}:{name}'
        for verb, resource in [('get','secrets'), ('create','pods'), ('create','pods/exec'), ('get','persistentvolumes')]:
            grant = allowed(kube, identity, verb, resource, manage.NS)
            if grant:
                raise ValueError(f'Unexpected application RBAC: {name} {verb} {resource}')
        pod = {'apiVersion':'v1','kind':'Pod','metadata':{'name':'isolation-admission-probe','namespace':manage.NS,
               'labels':deployment['spec']['template']['metadata']['labels']},
               'spec':deployment['spec']['template']['spec']}
        kube('create','--dry-run=server','-f','-',data=json.dumps(pod),capture=True)
    original = {'apiVersion':'v1','kind':'Pod','metadata':{'name':'isolation-admission-probe','namespace':manage.NS,
                'labels':by_name['client-api']['spec']['template']['metadata']['labels']},
                'spec':by_name['client-api']['spec']['template']['spec']}
    def init_secret(pod):
        container = copy.deepcopy(pod['spec']['containers'][0])
        container['name'] = 'init-probe'
        container.pop('ports', None)
        for key in ('livenessProbe', 'readinessProbe', 'startupProbe'):
            container.pop(key, None)
        container.setdefault('env', []).append({'name':'STOLEN','valueFrom':{
            'secretKeyRef':{'name':'bnpb-user-auth','key':'internal-password'}}})
        pod['spec']['initContainers'] = [container]

    attacks = [
        ('init container Secret', init_secret),
        ('Secret volume', lambda p:p['spec'].setdefault('volumes',[]).append({'name':'stolen','secret':{'secretName':'bnpb-user-auth'}})),
        ('bulk Secret import', lambda p:p['spec']['containers'][0].setdefault('envFrom',[]).append({'secretRef':{'name':'bnpb-user-auth'}})),
        ('claim borrowing', lambda p:p['spec'].setdefault('volumes',[]).append({'name':'stolen','persistentVolumeClaim':{'claimName':'bnpb-m1-data'}})),
        ('Secret borrowing', lambda p:p['spec']['containers'][0].setdefault('env',[]).append({'name':'STOLEN','valueFrom':{'secretKeyRef':{'name':'bnpb-user-auth','key':'internal-password'}}})),
        ('projected Secret', lambda p:p['spec'].setdefault('volumes',[]).append({'name':'stolen','projected':{'sources':[{'secret':{'name':'bnpb-user-auth'}}]}})),
        ('host disk',lambda p:p['spec'].setdefault('volumes',[]).append({'name':'host','hostPath':{'path':'/var/lib/bnpb'}})),
        ('privileged container',lambda p:p['spec']['containers'][0]['securityContext'].update(privileged=True)),
        ('identity spoofing',lambda p:p['metadata']['labels'].update({'app.kubernetes.io/name':'bnpb'})),
    ]
    for label, mutate in attacks:
        pod=copy.deepcopy(original)
        mutate(pod)
        try:
            kube('create','--dry-run=server','-f','-',data=json.dumps(pod),capture=True)
        except subprocess.CalledProcessError as exc:
            if 'bnpb-private-storage' not in (exc.stderr or ''):
                raise ValueError(f'{label}: rejected for a reason other than the storage policy') from exc
        else:
            raise ValueError(f'Admission permitted {label}')
    # A positive control and denied path detect CNIs which silently ignore NetworkPolicy.
    command = ['wget','-q','-T','5','-O','/dev/null','http://auth:8000/health']
    kube('-n',manage.NS,'exec','deployment/client-api','--',*command,capture=True)
    try:
        kube('-n',manage.NS,'exec','deployment/pvmbg','--',*command,capture=True)
    except subprocess.CalledProcessError as exc:
        if 'timed out' not in (exc.stderr or '').lower():
            raise ValueError('Denied network probe failed without proving a policy timeout') from exc
    else:
        raise ValueError('NetworkPolicy not enforced: PVMBG reached private Auth endpoint')
    print('Owner manifests admitted; cross-service volume/Secret/host access denied; RBAC and network controls passed.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['install','agent-check','isolation-check'])
    parser.add_argument('--dev',action='store_true')
    args=parser.parse_args()
    kube=manage.kubectl
    if args.dev:
        from dev import kube
    try:
        {'install':install,'agent-check':check_agent,'isolation-check':check_isolation}[args.action](kube)
    except (ValueError,OSError,subprocess.CalledProcessError) as exc:
        print(f'Security check failed: {exc}',file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
