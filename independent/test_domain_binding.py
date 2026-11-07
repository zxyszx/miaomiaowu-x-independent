"""Candidate selection to node snapshot, including rejection and rollback."""
import concurrent.futures
import copy
import json
import time
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.error
import urllib.request
import agent

from core import Error, Registry
from domain_pool import DomainPool, MAX_RESULT_AGE
import test_core
from test_domain_pool import available
import test_reality
from test_xray import VLESS


class DomainBindingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.registry = Registry(Path(self.temp.name)/'registry.db')
        self.pool = DomainPool(self.registry)
        self.sid = self.registry.save('servers', {'name': 'one'})['id']
        self.nid = self.registry.save('nodes', {'name': 'reality', 'server_id': self.sid})['id']
        self.token = self.registry.provision_agent(self.sid)['token']
        self.config = test_reality.RealityConfigTests().config()
        self.registry.configure_node(self.nid, self.config)
        self.uid = self.registry.save('users', {'name': 'one'})['id']
        self.registry.authorize_node_user(self.nid, self.uid)
        self.item = self.pool.save(dict(domain='new.example', port=8443, enabled=True))
        self.registry.create_reality_probe_job(self.sid,self.item['id'])
        claim=self.registry.claim_reality_probe(self.token)['job']
        self.registry.complete_reality_probe(self.token,claim['job_id'],claim['lease_token'],available())
        self.item=self.pool.list(self.sid)[0]
        self.data = dict(domain_id=self.item['id'], domain_revision=self.item['revision'])

    def tearDown(self):
        self.temp.cleanup()

    def snapshot(self):
        return self.registry.configuration(self.token)

    def test_bind_persist_idempotent_and_preserve_keys_users_and_other_servers(self):
        sid = self.registry.save('servers', {'name': 'unrelated'})['id']
        token = self.registry.provision_agent(sid)['token']
        unrelated = self.registry.configuration(token)
        before = self.snapshot()
        result = self.pool.bind(self.nid, self.data)
        after = self.snapshot()
        expected = copy.deepcopy(before)
        expected['nodes'][0]['config']['reality'].update(
            target_host='new.example', server_name='new.example', target_port=8443)
        self.assertEqual(after['nodes'], expected['nodes'])
        self.assertEqual(after['revision'], before['revision']+1)
        self.assertEqual(result['server_revision'], after['revision'])
        self.assertNotIn('private_key', json.dumps(result))
        self.assertNotIn(self.config['reality']['private_key'], json.dumps(result))
        self.assertEqual(self.registry.configuration(token), unrelated)
        self.assertEqual(self.pool.bind(self.nid, self.data), result)
        self.assertEqual(Registry(self.registry.path).configuration(self.token), after)

    def test_expired_future_checking_failed_disabled_and_stale_candidates_do_not_mutate(self):
        before = self.snapshot()
        checked = self.item['checked_at']
        for instant in (checked+MAX_RESULT_AGE+1, checked-1):
            with mock.patch('domain_pool.time.time', return_value=instant), self.assertRaises(Error) as caught:
                self.pool.bind(self.nid, self.data)
            self.assertEqual(caught.exception.status, 409)
            self.assertEqual(self.snapshot(), before)
        for status in ('checking', 'unavailable', 'not_checked'):
            with self.registry.connect() as db:
                db.execute('UPDATE reality_server_probes SET status=?', (status,))
            with self.assertRaises(Error): self.pool.bind(self.nid, self.data)
        self.pool.save(dict(domain='changed.example', port=443, enabled=True), self.item['id'])
        with mock.patch('domain_pool.probe_tls', return_value=available()): self.pool.probe(self.item['id'])
        with self.assertRaises(Error): self.pool.bind(self.nid, self.data)
        self.data['domain_revision'] += 1
        self.pool.save(dict(domain='changed.example', port=443, enabled=False), self.item['id'])
        self.data['domain_revision'] += 1
        with self.assertRaises(Error): self.pool.bind(self.nid, self.data)
        self.assertEqual(self.snapshot(), before)

    def test_pool_edit_and_delete_never_implicitly_change_bound_node(self):
        self.pool.bind(self.nid, self.data)
        before = self.snapshot()
        self.pool.save(dict(domain='changed.example', port=443, enabled=True), self.item['id'])
        self.pool.delete(self.item['id'])
        with self.assertRaises(Error) as caught: self.pool.bind(self.nid, self.data)
        self.assertEqual(caught.exception.status, 404)
        self.assertEqual(self.snapshot(), before)

    def test_wrong_nodes_and_bad_inputs_rejected_without_config_creation(self):
        before = self.snapshot()
        for data in (None, [], {}, {'domain_id': True, 'domain_revision': 1},
                     {**self.data, 'domain_revision': 0}, {**self.data, 'extra': 1}):
            with self.subTest(data=data), self.assertRaises(Error): self.pool.bind(self.nid, data)
        self.assertEqual(self.snapshot(), before)
        nid = self.registry.save('nodes', {'name': 'unconfigured', 'server_id': self.sid})['id']
        for config in (None, {**VLESS, 'listen_port': 12003}, dict(listen_host='127.0.0.1', listen_port=12002,
                                       target_host='example.com', target_port=443, enabled=True)):
            if config: self.registry.configure_node(nid, config)
            before = self.snapshot()
            with self.assertRaises(Error) as caught: self.pool.bind(nid, self.data)
            self.assertEqual(caught.exception.status, 409)
            self.assertEqual(self.snapshot(), before)
        with self.assertRaises(Error) as caught: self.pool.bind(999, self.data)
        self.assertEqual(caught.exception.status, 404)

    def test_save_failure_rolls_back_payload_and_revision(self):
        before = self.snapshot()
        with mock.patch.object(self.registry, '_bump', side_effect=RuntimeError('disk fixture')):
            with self.assertRaises(RuntimeError): self.pool.bind(self.nid, self.data)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.pool.select(self.sid)['id'], self.item['id'])

    def test_concurrent_same_binding_only_increments_revision_once(self):
        before = self.snapshot()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _: self.pool.bind(self.nid, self.data), range(16)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(self.snapshot()['revision'], before['revision']+1)

    def test_inconsistent_success_metadata_is_not_eligible(self):
        before = self.snapshot()
        for result in (available() | {'ok': False}, available() | {'alpn': 'http/1.1'},
                       available() | {'tls_version': 'TLSv1.2'}, available() | {'latency_ms': -1}):
            with self.registry.connect() as db:
                db.execute('UPDATE reality_server_probes SET result=?', (json.dumps(result),))
            with self.assertRaises(Error): self.pool.bind(self.nid, self.data)
            with self.assertRaises(Error): self.pool.select(self.sid)
        self.assertEqual(self.snapshot(), before)


class DomainBindingHttpTests(unittest.TestCase):
    setUp = test_core.ApiTests.setUp
    tearDown = test_core.ApiTests.tearDown
    request = test_core.ApiTests.request

    def test_execution_agent_applies_and_completes_probe_over_http(self):
        import asyncio
        import contextlib
        import execution_agent
        from forward_runtime import ForwardRuntime
        registry=self.server.registry
        sid=registry.save('servers',{'name':'execution probe HTTP'})['id']
        token=registry.provision_agent(sid)['token']
        pool=DomainPool(registry)
        item=pool.save({'domain':'probe.example','port':443,'enabled':True})
        registry.create_reality_probe_job(sid,item['id'])
        async def exercise(state):
            completed=asyncio.Event(); loop=asyncio.get_running_loop()
            original=agent.probe_pending
            def probe(*args):
                result=original(*args)
                loop.call_soon_threadsafe(completed.set)
                return result
            runtime=ForwardRuntime()
            with mock.patch('execution_agent.probe_pending',side_effect=probe), \
                 mock.patch('agent.probe_domain',return_value=available()):
                task=asyncio.create_task(execution_agent.run({'master':self.base,'token':token},state,runtime))
                try:
                    await asyncio.wait_for(completed.wait(),3)
                    self.assertTrue(registry.agent_status()[0]['listeners_current'])
                    self.assertEqual(pool.list(sid)[0]['status'],'available')
                    self.assertIsNone(registry.claim_reality_probe(token)['job'])
                finally:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError): await task
        with tempfile.TemporaryDirectory() as state: asyncio.run(exercise(state))

    def test_agent_http_probe_retries_only_when_due_then_recovers(self):
        registry=self.server.registry
        sid=registry.save('servers',{'name':'retry HTTP'})['id']
        token=registry.provision_agent(sid)['token']
        pool=DomainPool(registry)
        item=pool.save({'domain':'probe.example','port':443,'enabled':True})
        registry.create_reality_probe_job(sid,item['id'])
        with mock.patch('agent.probe_domain',return_value={'ok':False,'error':'timeout'}) as probe:
            result=agent.probe_pending(self.base,token)
            self.assertTrue(result['retry_pending'])
            self.assertIsNone(agent.probe_pending(self.base,token))
            probe.assert_called_once()
        registry=Registry(registry.path)
        with mock.patch('core.time.time',return_value=result['next_attempt_at']), \
             mock.patch('agent.probe_domain',return_value=available()):
            self.assertEqual(agent.probe_pending(self.base,token)['status'],'available')
        self.assertIsNone(agent.probe_pending(self.base,token))
        self.assertEqual(DomainPool(registry).list(sid)[0]['status'],'available')

    def test_agent_once_stages_config_even_when_probe_receipt_becomes_stale(self):
        registry=self.server.registry
        sid=registry.save('servers',{'name':'optional probe'})['id']
        token=registry.provision_agent(sid)['token']
        pool=DomainPool(registry)
        item=pool.save({'domain':'probe.example','port':443,'enabled':True})
        registry.create_reality_probe_job(sid,item['id'])
        def changed_during_probe(*args):
            pool.save({'domain':'changed.example','port':443,'enabled':True},item['id'])
            return available()
        with tempfile.TemporaryDirectory() as directory:
            config=Path(directory)/'agent.json'
            state=Path(directory)/'state'
            config.write_text(json.dumps({'master':self.base,'token':token}))
            with mock.patch('sys.argv',['agent.py','--config',str(config),'--state',str(state),'--once']), \
                 mock.patch('agent.probe_domain',side_effect=changed_during_probe),mock.patch('builtins.print') as output:
                self.assertEqual(agent.main(),0)
            snapshot=json.loads((state/'staged-config.json').read_text())
            status=registry.agent_status()[0]
            self.assertEqual(status['staged_revision'],snapshot['revision'])
            self.assertTrue(status['online'])
            self.assertFalse(status['listeners_current'])
            self.assertEqual(pool.list(sid)[0]['status'],'not_checked')
            self.assertTrue(any('probe failed' in str(call) for call in output.call_args_list))

    def test_incomplete_success_http_receipt_returns_actionable_error_and_can_correct(self):
        registry=self.server.registry
        sid=registry.save('servers',{'name':'receipt test'})['id']
        token=registry.provision_agent(sid)['token']
        pool=DomainPool(registry)
        item=pool.save({'domain':'probe.example','port':443,'enabled':True})
        task=registry.create_reality_probe_job(sid,item['id'])
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
        with self.opener.open(urllib.request.Request(self.base+'/agent/probe-job',headers=headers)) as response:
            claim=json.load(response)['job']
        def complete(result):
            body=json.dumps({'lease_token':claim['lease_token'],'result':result}).encode()
            return self.opener.open(urllib.request.Request(self.base+'/agent/probe-job/'+task['job_id'],method='POST',data=body,headers=headers))
        with self.assertRaises(urllib.error.HTTPError) as caught:
            complete({'ok':True})
        self.assertEqual(caught.exception.code,400)
        self.assertIn('TLS 1.3',caught.exception.read().decode())
        self.assertEqual(pool.list(sid)[0]['status'],'checking')
        with complete(available()) as response:
            self.assertEqual(json.load(response)['status'],'available')
        self.assertEqual(pool.select(sid)['id'],item['id'])

    def test_expired_http_receipt_cannot_publish_success_and_job_can_retry(self):
        from core import PROBE_JOB_LEASE
        registry=self.server.registry
        sid=registry.save('servers',{'name':'lease test'})['id']
        token=registry.provision_agent(sid)['token']
        pool=DomainPool(registry)
        item=pool.save({'domain':'probe.example','port':443,'enabled':True})
        task=registry.create_reality_probe_job(sid,item['id'])
        headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}
        def claim():
            with self.opener.open(urllib.request.Request(self.base+'/agent/probe-job',headers=headers)) as response:
                return json.load(response)['job']
        first=claim()
        with registry.connect() as db:
            db.execute('UPDATE reality_probe_jobs SET claimed_at=? WHERE id=?',(time.time()-PROBE_JOB_LEASE-1,task['job_id']))
        def complete(lease):
            body=json.dumps({'lease_token':lease,'result':available()}).encode()
            return self.opener.open(urllib.request.Request(self.base+'/agent/probe-job/'+task['job_id'],method='POST',data=body,headers=headers))
        with self.assertRaises(urllib.error.HTTPError) as caught:
            complete(first['lease_token'])
        self.assertEqual(caught.exception.code,409)
        self.assertEqual(pool.list(sid)[0]['status'],'checking')
        second=claim()
        self.assertNotEqual(first['lease_token'],second['lease_token'])
        with complete(second['lease_token']) as response:
            self.assertEqual(json.load(response)['status'],'available')

    def test_authenticated_apply_and_error_feedback(self):
        registry = self.server.registry
        sid = registry.save('servers', {'name': 'one'})['id']
        nid = registry.save('nodes', {'name': 'one', 'server_id': sid})['id']
        registry.configure_node(nid, test_reality.RealityConfigTests().config())
        pool = DomainPool(registry)
        item = pool.save(dict(domain='new.example', port=443, enabled=True))
        data = dict(domain_id=item['id'], domain_revision=item['revision'])
        path = f'/api/nodes/{nid}/reality-domain'
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path, 'POST', data, auth=False)
        self.assertEqual(caught.exception.code, 401)
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path, 'POST', data)
        self.assertEqual(caught.exception.code, 409)
        token=registry.provision_agent(sid)['token']
        registry.create_reality_probe_job(sid,item['id'])
        claim=registry.claim_reality_probe(token)['job']
        registry.complete_reality_probe(token,claim['job_id'],claim['lease_token'],available())
        with self.request(path, 'POST', data) as response:
            self.assertEqual(response.headers['Cache-Control'], 'no-store')
            result = json.load(response)
            self.assertEqual(result['domain'], 'new.example')
            self.assertNotIn('private_key', json.dumps(result))
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path, 'POST', {'domain_id': True})
        self.assertEqual(caught.exception.code, 400)

    def test_agent_probe_job_http_flow_requires_current_lease(self):
        registry = self.server.registry
        sid = registry.save('servers', {'name':'probe server'})['id']
        agent_token = registry.provision_agent(sid)['token']
        item = DomainPool(registry).save({'domain':'probe.example','port':443,'enabled':True})

        with self.request(f'/api/servers/{sid}/reality-domains/{item["id"]}/probe-agent', 'POST', {}) as response:
            self.assertEqual(response.status, 202)
            task = json.load(response)
        request = urllib.request.Request(self.base + '/agent/probe-job', headers={'Authorization':'Bearer '+agent_token})
        with self.opener.open(request) as response:
            claimed = json.load(response)['job']
        self.assertEqual(claimed['job_id'], task['job_id'])
        result = available(7)
        body = json.dumps({'lease_token':claimed['lease_token'],'result':result}).encode()
        request = urllib.request.Request(self.base + '/agent/probe-job/' + task['job_id'], method='POST', data=body,
                                         headers={'Authorization':'Bearer '+agent_token,'Content-Type':'application/json'})
        with self.opener.open(request) as response:
            self.assertEqual(json.load(response)['status'], 'available')
        request = urllib.request.Request(self.base + '/agent/probe-job/' + task['job_id'], method='POST', data=body,
                                         headers={'Authorization':'Bearer '+agent_token,'Content-Type':'application/json'})
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(request)
        self.assertEqual(caught.exception.code, 409)

        second = DomainPool(registry).save({'domain':'second.example','port':443,'enabled':True})
        with self.request(f'/api/servers/{sid}/reality-domains/{second["id"]}/probe-agent', 'POST', {}) as response:
            second_task = json.load(response)
        request = urllib.request.Request(self.base + '/agent/probe-job', headers={'Authorization':'Bearer '+agent_token})
        with self.opener.open(request) as response: second_claim = json.load(response)['job']
        DomainPool(registry).save({'domain':'edited.example','port':443,'enabled':True}, second['id'])
        body = json.dumps({'lease_token':second_claim['lease_token'],'result':result}).encode()
        request = urllib.request.Request(self.base + '/agent/probe-job/' + second_task['job_id'], method='POST', data=body,
                                         headers={'Authorization':'Bearer '+agent_token,'Content-Type':'application/json'})
        with self.assertRaises(urllib.error.HTTPError) as caught: self.opener.open(request)
        self.assertEqual(caught.exception.code, 409)
