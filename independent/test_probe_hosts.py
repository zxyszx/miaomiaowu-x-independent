"""Server-scoped probe evidence, queue isolation, and safe node binding."""
from concurrent.futures import ThreadPoolExecutor
import json
import time
import unittest
from unittest import mock
import urllib.error

from core import Registry, Error
from domain_pool import DomainPool, MAX_RESULT_AGE
import test_agent_probe
import test_core
import test_reality
from test_domain_pool import available


class ProbeHostTests(unittest.TestCase):
    setUp=test_agent_probe.AgentProbeTests.setUp
    tearDown=test_agent_probe.AgentProbeTests.tearDown

    def other(self):
        sid=self.registry.save('servers',{'name':'other'})['id']
        return sid,self.registry.provision_agent(sid)['token']

    def finish(self,sid,token,result):
        self.registry.create_reality_probe_job(sid,self.item['id'])
        claim=self.registry.claim_reality_probe(token)['job']
        return self.registry.complete_reality_probe(token,claim['job_id'],claim['lease_token'],result)

    def test_two_hosts_complete_opposite_results_without_overwriting_or_master_fallback(self):
        other,token=self.other()
        with ThreadPoolExecutor(max_workers=2) as workers:
            results=list(workers.map(lambda args:self.finish(*args),[
                (self.sid,self.creds['token'],available(8)),
                (other,token,{'ok':False,'error':'certificate_error'})]))
        self.assertEqual({r['status'] for r in results},{'available','unavailable'})
        self.assertEqual(self.pool.select(self.sid)['result']['latency_ms'],8)
        with self.assertRaises(Error): self.pool.select(other)
        self.assertEqual(self.pool.list()[0]['status'],'not_checked')
        with mock.patch('domain_pool.probe_tls',return_value=available(1)):
            self.pool.probe(self.item['id'])
        self.assertEqual(self.pool.select()['result']['latency_ms'],1)
        self.assertEqual(self.pool.select(self.sid)['result']['latency_ms'],8)
        with self.assertRaises(Error): self.pool.select(other)
        persisted=DomainPool(Registry(self.registry.path))
        self.assertEqual(persisted.list(other)[0]['result']['error'],'certificate_error')

    def test_own_new_generation_does_not_cancel_other_claim(self):
        other,token=self.other()
        self.registry.create_reality_probe_job(self.sid,self.item['id'])
        old=self.registry.claim_reality_probe(self.creds['token'])['job']
        self.registry.create_reality_probe_job(other,self.item['id'])
        untouched=self.registry.claim_reality_probe(token)['job']
        self.registry.create_reality_probe_job(self.sid,self.item['id'])
        with self.assertRaises(Error):
            self.registry.complete_reality_probe(self.creds['token'],old['job_id'],old['lease_token'],available())
        self.registry.complete_reality_probe(token,untouched['job_id'],untouched['lease_token'],available())
        self.assertEqual(self.pool.select(other)['server_id'],other)
        self.assertEqual(self.pool.list(self.sid)[0]['status'],'checking')

    def test_binding_requires_own_recent_probe_and_cannot_use_master_or_other_host(self):
        other,token=self.other()
        nid=self.registry.save('nodes',{'name':'one','server_id':other})['id']
        self.registry.configure_node(nid,test_reality.RealityConfigTests().config())
        before=self.registry.configuration(token)
        data={'domain_id':self.item['id'],'domain_revision':self.item['revision']}
        self.finish(self.sid,self.creds['token'],available())
        with mock.patch('domain_pool.probe_tls',return_value=available()): self.pool.probe(self.item['id'])
        with self.assertRaisesRegex(Error,'所属服务器'): self.pool.bind(nid,data)
        self.assertEqual(self.registry.configuration(token),before)
        self.finish(other,token,available(12))
        result=self.pool.bind(nid,data)
        self.assertEqual(result['domain'],'probe.example')
        snapshot=self.registry.configuration(token)
        checked=self.pool.list(other)[0]['checked_at']
        with mock.patch('domain_pool.time.time',return_value=checked+MAX_RESULT_AGE+1):
            with self.assertRaises(Error): self.pool.bind(nid,data)
        self.assertEqual(self.registry.configuration(token),snapshot)

    def test_edits_disable_and_delete_invalidate_every_host(self):
        other,token=self.other()
        for sid,cred in ((self.sid,self.creds['token']),(other,token)): self.finish(sid,cred,available())
        self.pool.save({'domain':'new.example','port':443,'enabled':True},self.item['id'])
        for sid in (self.sid,other):
            self.assertEqual(self.pool.list(sid)[0]['status'],'not_checked')
            with self.assertRaises(Error): self.pool.select(sid)
        self.finish(self.sid,self.creds['token'],available())
        self.pool.save({'domain':'new.example','port':443,'enabled':False},self.item['id'])
        with self.assertRaises(Error): self.pool.select(self.sid)
        self.pool.delete(self.item['id'])
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reality_server_probes').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM reality_probe_jobs').fetchone()[0],0)

    def test_other_host_manual_probe_does_not_postpone_scheduled_host(self):
        other,token=self.other()
        with mock.patch('core.time.time',return_value=1000):
            self.registry.reality_probe_schedule(self.item['id'],{'server_id':self.sid,'interval_seconds':300})
        with mock.patch('core.time.time',return_value=1001):
            self.registry.create_reality_probe_job(other,self.item['id'])
            self.assertIsNotNone(self.registry.claim_reality_probe(self.creds['token'])['job'])
            self.assertIsNotNone(self.registry.claim_reality_probe(token)['job'])
        self.assertEqual(self.registry.reality_probe_schedule(self.item['id'])['schedule']['next_run_at'],1301)

    def test_legacy_active_claim_migrates_only_to_its_server_and_is_idempotent(self):
        other,token=self.other()
        self.registry.create_reality_probe_job(self.sid,self.item['id'])
        claim=self.registry.claim_reality_probe(self.creds['token'])['job']
        # Recreate the legacy single-result schema/state, keeping the live lease.
        with self.registry.connect() as db:
            db.execute('DROP TABLE reality_server_probes')
            db.execute("UPDATE reality_domains SET probe_generation=1,status='checking'")
            db.execute("DELETE FROM schema_migrations WHERE name='per-server-reality-probes-v1'")
        with ThreadPoolExecutor(max_workers=3) as workers:
            list(workers.map(lambda _:Registry(self.registry.path),range(3)))
        self.assertEqual(self.pool.list(self.sid)[0]['status'],'checking')
        self.assertEqual(self.pool.list(other)[0]['status'],'not_checked')
        self.registry.complete_reality_probe(self.creds['token'],claim['job_id'],claim['lease_token'],available())
        self.assertEqual(DomainPool(Registry(self.registry.path)).select(self.sid)['server_id'],self.sid)
        with self.assertRaises(Error): self.pool.select(other)

    def test_legacy_completed_result_is_not_copied_to_unrelated_server(self):
        other,token=self.other()
        self.finish(self.sid,self.creds['token'],available())
        with self.registry.connect() as db:
            row=db.execute('SELECT * FROM reality_server_probes').fetchone()
            db.execute('UPDATE reality_domains SET probe_generation=?,status=?,checked_at=?,result=?',
                       (row['probe_generation'],row['status'],row['checked_at'],row['result']))
            db.execute('DROP TABLE reality_server_probes')
            db.execute("DELETE FROM schema_migrations WHERE name='per-server-reality-probes-v1'")
        Registry(self.registry.path)
        self.assertEqual(self.pool.select(self.sid)['server_id'],self.sid)
        with self.assertRaises(Error): self.pool.select(other)
        self.assertEqual(self.pool.list()[0]['status'],'not_checked')


class ProbeHostHttpTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_host_results_and_selection_require_admin_and_never_fallback(self):
        registry=self.server.registry
        sid=registry.save('servers',{'name':'one'})['id']
        token=registry.provision_agent(sid)['token']
        other=registry.save('servers',{'name':'other'})['id']
        pool=DomainPool(registry)
        item=pool.save({'domain':'probe.example','port':443,'enabled':True})
        path=f'/api/servers/{sid}/reality-domains'
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,auth=False)
        self.assertEqual(caught.exception.code,401)
        with self.request(path) as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            self.assertEqual(json.load(response)[0]['status'],'not_checked')
        registry.create_reality_probe_job(sid,item['id'])
        claim=registry.claim_reality_probe(token)['job']
        registry.complete_reality_probe(token,claim['job_id'],claim['lease_token'],available())
        with self.request(path+'/selection') as response:
            self.assertEqual(json.load(response)['server_id'],sid)
        for suffix,status in ((f'/api/servers/{other}/reality-domains/selection',409),
                              ('/api/servers/999/reality-domains',404)):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(suffix)
            self.assertEqual(caught.exception.code,status)
