import contextlib
import io
import subprocess
import hashlib
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

import yaml

HERE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(HERE / 'k3s'))
import security
import manage


class SecurityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.docs=list(yaml.safe_load_all(manage.render()))

    def test_supplied_agent_is_byte_exact_and_outside_application_overlay(self):
        self.assertEqual(hashlib.sha256(security.MANIFEST.read_bytes()).hexdigest(),security.MANIFEST_SHA256)
        docs=list(yaml.safe_load_all(security.MANIFEST.read_text()))
        ds=next(d for d in docs if d['kind']=='DaemonSet')
        pod=ds['spec']['template']['spec']
        self.assertEqual(pod['containers'][0]['image'],security.IMAGE)
        self.assertTrue(pod['containers'][0]['securityContext']['privileged'])
        self.assertTrue(pod['hostNetwork'])
        self.assertTrue(pod['hostPID'])
        self.assertFalse(any(d['metadata'].get('namespace')=='tester-agent' for d in self.docs))
        with patch.object(security.MANIFEST.__class__,'read_bytes',return_value=b'altered'), self.assertRaises(ValueError):
            security.install(lambda *a,**kw:self.fail('Changed agent must not be applied'))

    def test_every_current_workload_has_registered_owner_claims_and_secrets(self):
        policy=next(d for d in self.docs if d['kind']=='ValidatingAdmissionPolicy')['spec']
        variables={v['name']:json.loads(v['expression']) for v in policy['variables'] if v['name'] in ['claims','secrets','roles']}
        accounts={d['metadata']['name'] for d in self.docs if d['kind']=='ServiceAccount'}
        workloads=[d for d in self.docs if d['kind']=='Deployment']+[yaml.safe_load((HERE/'kubernetes/base/topic-init.yaml').read_text())]
        for workload in workloads:
            pod=workload['spec']['template']
            owner=pod['spec']['serviceAccountName']
            self.assertIn(owner,accounts)
            self.assertEqual(pod['metadata']['labels']['app.kubernetes.io/name'],owner)
            self.assertEqual(pod['metadata']['labels']['role'],variables['roles'][owner])
            for volume in pod['spec'].get('volumes',[]):
                if 'persistentVolumeClaim' in volume:
                    self.assertIn(volume['persistentVolumeClaim']['claimName'],variables['claims'][owner])
            for c in pod['spec']['containers']:
                for e in c.get('env',[]):
                    ref=e.get('valueFrom',{}).get('secretKeyRef')
                    if ref:self.assertIn(ref['name'],variables['secrets'][owner])
        self.assertEqual(policy['failurePolicy'],'Fail')
        self.assertEqual(policy['matchConditions'][0]['expression'],f'request.namespace == "{manage.NS}"')

    def test_no_application_api_grants_or_tester_restrictions(self):
        self.assertFalse(any(d['kind'] in ['Role','ClusterRole','RoleBinding','ClusterRoleBinding'] for d in self.docs))
        policies={d['metadata']['name']:d['spec'] for d in self.docs if d['kind']=='NetworkPolicy'}
        self.assertEqual(policies['default-deny-ingress']['ingress'],[])
        self.assertEqual(policies['allow-tester-agent']['podSelector'],{})
        self.assertEqual(policies['allow-tester-agent']['ingress'],[{'from':[{'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':'tester-agent'}}}]}])
        self.assertTrue(all(p['policyTypes']==['Ingress'] for p in policies.values()))
        store=policies['canonical-storage']['ingress']
        self.assertEqual(store[0]['ports'],[{'protocol':'TCP','port':5432}])
        self.assertEqual(store[0]['from'][0]['podSelector']['matchLabels']['app.kubernetes.io/name'],'bnpb')

    def test_agent_check_requires_all_nodes_and_exact_image(self):
        def kube(*args,**kw):
            if args[:2]==('get','nodes'):
                return types.SimpleNamespace(stdout=json.dumps({'items':[{'metadata':{'name':'one'}},{'metadata':{'name':'two'}}]}))
            if args[:3] == ('-n','tester-agent','get'):
                return types.SimpleNamespace(stdout=json.dumps({'items':[{'metadata':{},'spec':{'nodeName':'one','containers':[{'image':security.IMAGE}]},'status':{'conditions':[{'type':'Ready','status':'True'}]}}]}))
            return types.SimpleNamespace(stdout='yes')
        with self.assertRaisesRegex(ValueError,'every node'):
            security.check_agent(kube)

    def test_discovery_runs_on_each_node_and_rejects_wrong_image(self):
        pods=[{'metadata':{'name':'agent-'+n},
               'spec':{'nodeName':n,'containers':[{'image':security.IMAGE}]},
               'status':{'conditions':[{'type':'Ready','status':'True'}]}} for n in ['one','two']]
        calls=[]
        def kube(*args,**kw):
            calls.append(args)
            if args[:2]==('get','nodes'):
                return types.SimpleNamespace(stdout=json.dumps({'items':[{'metadata':{'name':n}} for n in ['one','two']]}))
            if args[:3] == ('-n','tester-agent','get'):
                return types.SimpleNamespace(stdout=json.dumps({'items':pods}))
            return types.SimpleNamespace(stdout='yes')
        with contextlib.redirect_stdout(io.StringIO()):
            security.check_agent(kube)
        discovery=[a for a in calls if a[0]=='exec']
        self.assertEqual({a[3] for a in discovery},{'agent-one','agent-two'})
        self.assertTrue(all(a[-2:]==('/app','list') for a in discovery))
        pods[1]['spec']['containers'][0]['image']='wrong-image'
        with self.assertRaisesRegex(ValueError,'image differs'):
            security.check_agent(kube)

    def test_rbac_denial_does_not_hide_api_failures(self):
        identity='system:serviceaccount:example:component'
        def denied(*args,**kwargs):
            self.assertIn('--as-group=system:serviceaccounts:example',args)
            self.assertIn('--as-group=system:serviceaccounts',args)
            raise subprocess.CalledProcessError(1,args,output='no\n')
        self.assertFalse(security.allowed(denied,identity,'get','secrets','example'))
        for error in ['connection refused','Forbidden','request timed out']:
            def failed(*args,**kwargs):
                raise subprocess.CalledProcessError(1,args,stderr=error)
            with self.subTest(error=error), self.assertRaises(subprocess.CalledProcessError):
                security.allowed(failed,identity,'get','secrets','example')

    def test_kind_firewall_does_not_replace_cni_or_proxy(self):
        docs=list(yaml.safe_load_all((HERE/'kubernetes/dev/network-policy.yaml').read_text()))
        ds=next(d for d in docs if d['kind']=='DaemonSet')
        spec=ds['spec']['template']['spec']
        self.assertEqual(ds['metadata']['namespace'],'kube-system')
        self.assertNotIn('initContainers',spec)
        self.assertTrue(spec['hostNetwork'])
        self.assertEqual(spec['tolerations'],[{'operator':'Exists'}])
        container=spec['containers'][0]
        self.assertEqual(container['image'],'docker.io/cloudnativelabs/kube-router:v2.6.2')
        self.assertEqual(set(container['args']),{'--run-firewall=true','--run-router=false','--run-service-proxy=false'})
        self.assertFalse(any(v['hostPath']['path'] in ['/etc/cni/net.d','/opt'] for v in spec['volumes']))
        rules=next(d for d in docs if d['kind']=='ClusterRole')['rules']
        self.assertTrue(all(set(r['verbs'])=={'get','list','watch'} for r in rules))
        self.assertTrue(any(r['apiGroups']==['discovery.k8s.io'] and r['resources']==['endpointslices'] for r in rules))

    def test_install_passes_supplied_file_directly(self):
        calls=[]
        security.install(lambda *a,**kw:calls.append((a,kw)))
        self.assertEqual(calls,[(('apply','-f',security.MANIFEST),{})])


if __name__=='__main__':
    unittest.main()
