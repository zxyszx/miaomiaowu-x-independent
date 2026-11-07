"""Persisted opt-in polling schedules; no live network or production data."""
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest import mock

from core import Registry, Error
import test_agent_probe
from test_domain_pool import available
from domain_pool import DomainPool


class ProbeScheduleTests(unittest.TestCase):
    setUp=test_agent_probe.AgentProbeTests.setUp
    tearDown=test_agent_probe.AgentProbeTests.tearDown

    def schedule(self, **changes):
        return self.registry.reality_probe_schedule(self.item['id'],dict(server_id=self.sid,interval_seconds=300,**changes))

    def claim(self,token=None):
        return self.registry.claim_reality_probe(token or self.creds['token'])['job']

    def finish(self,job,result=None):
        return self.registry.complete_reality_probe(self.creds['token'],job['job_id'],job['lease_token'],result or available())

    def test_default_off_persisted_due_boundary_and_no_offline_backlog(self):
        self.assertIsNone(self.claim())
        self.assertIsNone(self.registry.reality_probe_schedule(self.item['id'])['schedule'])
        with mock.patch('core.time.time',return_value=1000):
            self.schedule(); first=self.claim(); self.finish(first)
            self.assertIsNone(self.claim())
        self.registry=Registry(self.registry.path)
        with mock.patch('core.time.time',return_value=1299.99): self.assertIsNone(self.claim())
        with mock.patch('core.time.time',return_value=100000):
            second=self.claim(); self.finish(second); self.assertIsNone(self.claim())
        self.assertNotEqual(first['job_id'],second['job_id'])
        self.assertEqual(self.registry.reality_probe_schedule(self.item['id'])['schedule']['next_run_at'],100300)
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reality_probe_jobs').fetchone()[0],2)

    def test_repeated_save_preserves_deadline_and_retries_are_not_replaced(self):
        with mock.patch('core.time.time',return_value=1000):
            self.schedule(); first=self.claim(); self.finish(first,{'ok':False,'error':'timeout'})
        with mock.patch('core.time.time',return_value=1001):
            self.assertEqual(self.schedule()['schedule']['next_run_at'],1300)
            self.assertIsNone(self.claim())
        with mock.patch('core.time.time',return_value=1400):
            retry=self.claim()
            self.assertEqual(first['job_id'],retry['job_id'])
            self.assertNotEqual(first['lease_token'],retry['lease_token'])
            self.finish(retry)
            self.assertNotEqual(self.claim()['job_id'],first['job_id'])

    def test_concurrent_pollers_enqueue_and_claim_only_one(self):
        self.schedule()
        with ThreadPoolExecutor(max_workers=5) as workers:
            values=list(workers.map(lambda _:self.claim(),range(5)))
        self.assertEqual(sum(value is not None for value in values),1)
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reality_probe_jobs').fetchone()[0],1)

    def test_disable_and_delete_schedule_and_domain(self):
        self.schedule()
        self.pool.save(dict(domain='probe.example',port=443,enabled=False),self.item['id'])
        self.assertIsNone(self.claim())
        self.pool.save(dict(domain='probe.example',port=443,enabled=True),self.item['id'])
        job=self.claim()
        self.registry.reality_probe_schedule(self.item['id'],delete=True)
        self.finish(job)  # Removing a schedule stops future runs, not active work.
        with mock.patch('core.time.time',return_value=10**10): self.assertIsNone(self.claim())
        self.schedule(); self.pool.delete(self.item['id'])
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reality_probe_schedules').fetchone()[0],0)

    def test_manual_probe_postpones_periodic_run_without_duplicate(self):
        with mock.patch('core.time.time',return_value=1000): self.schedule()
        with mock.patch('core.time.time',return_value=1100):
            manual=self.registry.create_reality_probe_job(self.sid,self.item['id'])
            job=self.claim(); self.assertEqual(job['job_id'],manual['job_id']); self.finish(job)
            self.assertIsNone(self.claim())
        with mock.patch('core.time.time',return_value=1399): self.assertIsNone(self.claim())
        with mock.patch('core.time.time',return_value=1400): self.assertIsNotNone(self.claim())

    def test_designated_server_only_and_revocation_does_not_enqueue(self):
        other=self.registry.save('servers',{'name':'other'})['id']
        token=self.registry.provision_agent(other)['token']
        self.schedule()
        self.assertIsNone(self.claim(token))
        self.registry.revoke_agent(self.sid)
        with self.assertRaises(Error): self.claim()
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM reality_probe_jobs').fetchone()[0],0)
        self.creds=self.registry.provision_agent(self.sid)
        self.assertIsNotNone(self.claim())

    def test_invalid_inputs_do_not_replace_existing_schedule(self):
        saved=self.schedule()
        invalid=[{},[],{'server_id':True,'interval_seconds':300},
                 {'server_id':self.sid,'interval_seconds':False},
                 {'server_id':self.sid,'interval_seconds':299},
                 {'server_id':self.sid,'interval_seconds':86401},
                 {'server_id':9999,'interval_seconds':300}]
        for value in invalid:
            with self.subTest(value=value):
                with self.assertRaises(Error): self.registry.reality_probe_schedule(self.item['id'],value)
                self.assertEqual(self.registry.reality_probe_schedule(self.item['id']),saved)

    def test_candidate_edit_retires_old_job_without_publishing_old_result(self):
        self.schedule(); old=self.claim()
        self.pool.save(dict(domain='changed.example',port=443,enabled=True),self.item['id'])
        with self.assertRaises(Error): self.finish(old)
        with mock.patch('core.time.time',return_value=10**10):
            job=self.claim(); self.assertEqual(job['domain'],'changed.example')
            self.assertNotEqual(job['job_id'],old['job_id'])
        self.assertEqual(self.pool.list(self.sid)[0]['status'],'checking')


class ProbeScheduleHttpTests(unittest.TestCase):
    import test_core as _fixtures
    setUp=_fixtures.ApiTests.setUp
    tearDown=_fixtures.ApiTests.tearDown
    request=_fixtures.ApiTests.request

    def test_authenticated_schedule_http_crud_and_agent_cycle(self):
        import json
        import urllib.error
        import agent
        registry=self.server.registry
        sid=registry.save('servers',{'name':'periodic HTTP'})['id']
        token=registry.provision_agent(sid)['token']
        item=DomainPool(registry).save(dict(domain='probe.example',port=443,enabled=True))
        path=f'/api/reality-domains/{item["id"]}/probe-schedule'
        data={'server_id':sid,'interval_seconds':300}
        for method,body in (('GET',None),('PUT',data),('DELETE',None)):
            with self.assertRaises(urllib.error.HTTPError) as error: self.request(path,method,body,auth=False)
            self.assertEqual(error.exception.code,401)
        with self.request(path) as response: self.assertIsNone(json.load(response)['schedule'])
        with self.request(path,'PUT',data) as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            self.assertEqual(json.load(response)['schedule']['server_id'],sid)
        with mock.patch('agent.probe_domain',return_value=available()):
            self.assertEqual(agent.probe_pending(self.base,token)['status'],'available')
            self.assertIsNone(agent.probe_pending(self.base,token))
        with self.assertRaises(urllib.error.HTTPError) as error: self.request(path,'PUT',[])
        self.assertEqual(error.exception.code,400)
        with self.request(path,'DELETE') as response: self.assertIsNone(json.load(response)['schedule'])
        with mock.patch('core.time.time',return_value=10**10):
            self.assertIsNone(agent.probe_pending(self.base,token))
