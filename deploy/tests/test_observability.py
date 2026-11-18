import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'deploy/k3s'))
import manage
import observability
import telemetry_smoke


class ObservabilityTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = list(yaml.safe_load_all(manage.render()))
        cls.docs = list(yaml.safe_load_all(observability.render()))

    def test_all_backends_have_config_probes_memory_and_private_data(self):
        deployments = [d for d in self.docs if d['kind']=='Deployment']
        self.assertEqual({d['metadata']['name'] for d in deployments},set(observability.IMAGES))
        maps = {d['metadata']['name'] for d in self.docs if d['kind']=='ConfigMap'}
        for d in deployments:
            name=d['metadata']['name']
            pod=d['spec']['template']
            self.assertEqual(d['spec']['replicas'],1)
            self.assertEqual(pod['metadata']['labels']['role'],'other')
            spec=pod['spec']
            self.assertEqual(spec['nodeSelector'],{'app-eligible':'true'})
            self.assertFalse(spec['automountServiceAccountToken'])
            self.assertEqual(spec['serviceAccountName'],name)
            c=spec['containers'][0]
            self.assertEqual(c['image'],observability.IMAGES[name])
            self.assertEqual(c['resources'],{'requests':{'memory':'128Mi'},'limits':{'memory':'256Mi'}})
            self.assertTrue(c['securityContext']['readOnlyRootFilesystem'])
            self.assertTrue(all(k in c for k in ['startupProbe','readinessProbe','livenessProbe']))
            for v in spec['volumes']:
                self.assertFalse('hostPath' in v or 'persistentVolumeClaim' in v)
                if 'configMap' in v:self.assertIn(v['configMap']['name'],maps)
                if 'projected' in v:self.assertEqual(name,'alloy')
        self.assertTrue(all(d['spec'].get('type','ClusterIP')=='ClusterIP' for d in self.docs if d['kind']=='Service'))

    def test_application_exporters_are_connected_without_public_metrics(self):
        maps={d['metadata']['name']:d['data'] for d in self.base if d['kind']=='ConfigMap'}
        for d in self.base:
            if d['kind']!='Deployment' or d['metadata']['name'] not in ['auth','client-api','bnpb']:continue
            c=d['spec']['template']['spec']['containers'][0]
            self.assertIn({'name':'metrics','containerPort':9090},c['ports'])
            env={e['name']:e for e in c['env']}
            for key in ['OTEL_TRACES_EXPORTER','OTEL_EXPORTER_OTLP_ENDPOINT','METRICS_ADDR']:
                ref=env[key]['valueFrom']['configMapKeyRef']
                self.assertIn(key,maps[ref['name']])
        service=next(d for d in self.base if d['kind']=='Service' and d['metadata']['name']=='client-api')
        self.assertEqual(service['spec']['type'],'NodePort')
        self.assertEqual(len(service['spec']['ports']),1)

    def test_role_and_secret_allowlists_include_backends_and_scoped_log_reader(self):
        policy=next(d for d in self.base if d['kind']=='ValidatingAdmissionPolicy')['spec']
        variables={v['name']:json.loads(v['expression']) for v in policy['variables'] if v['name'] in ['roles','secrets']}
        for name in observability.IMAGES:self.assertEqual(variables['roles'][name],'other')
        self.assertEqual(variables['secrets']['grafana'],['bnpb-grafana-auth'])
        roles=[d for d in self.docs if d['kind']=='Role']
        self.assertEqual(len(roles),1)
        self.assertEqual(roles[0]['rules'],[{'apiGroups':[''],'resources':['pods'],'verbs':['get','list','watch']},{'apiGroups':[''],'resources':['pods/log'],'verbs':['get']}])
        self.assertFalse(any(d['kind']=='ClusterRole' for d in self.docs))
        self.assertIn('variables.owner == "alloy"',policy['validations'][1]['expression'])

    def test_new_grafana_credential_does_not_rotate_existing_credentials(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(manage,'HERE',Path(tmp)):
            Path(tmp,'inventory.json').write_text('{}')
            env=Path(tmp,'.env');env.write_text('JWT_SECRET=keep-this\nINTERNAL_API_KEY=also-keep\n');env.chmod(0o600)
            with contextlib.redirect_stdout(io.StringIO()):
                manage.init()
            first=env.read_text()
            self.assertTrue(first.startswith('JWT_SECRET=keep-this\nINTERNAL_API_KEY=also-keep\n'))
            self.assertEqual(first.count('GRAFANA_ADMIN_PASSWORD='),1)
            with contextlib.redirect_stdout(io.StringIO()):
                manage.init()
            self.assertEqual(env.read_text(),first)

    def test_dashboard_has_red_metrics_availability_and_correlation_query(self):
        config=observability.ROOT/'config'
        dashboard=json.loads((config/'services.json').read_text())
        expressions=[p['targets'][0]['expr'] for p in dashboard['panels']]
        self.assertTrue(any('histogram_quantile' in e for e in expressions))
        self.assertTrue(any('status_code=~"5.."' in e for e in expressions))
        self.assertTrue(any('up{' in e for e in expressions))
        self.assertTrue(any('correlation_id=' in e for e in expressions))
        datasources=yaml.safe_load((config/'datasources.yaml').read_text())
        self.assertEqual({s['uid'] for s in datasources['datasources']},{'prometheus','tempo','loki'})
        self.assertTrue(all(not s['editable'] for s in datasources['datasources']))

    def test_trace_evidence_parser_requires_actual_service_resources(self):
        self.assertEqual(telemetry_smoke.trace_services({'batches':[{'resource':{'attributes':[{'key':'service.name','value':{'stringValue':'bnpb'}}]}}]}),{'bnpb'})
        self.assertEqual(telemetry_smoke.trace_services({'status':'ready'}),set())


if __name__=='__main__':
    unittest.main()
