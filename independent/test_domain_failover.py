"""Opt-in switching, distinct-round debounce, execution confirmation and rollback."""
from concurrent.futures import ThreadPoolExecutor
import copy
import json
import unittest
from unittest import mock
import urllib.error

from core import Registry, Error
from domain_pool import DomainPool
from domain_failover import Failover, CONFIRM_TIMEOUT
import test_agent_probe
import test_core
import test_reality
from test_domain_pool import available


class FailoverTests(unittest.TestCase):
    def setUp(self):
        test_agent_probe.AgentProbeTests.setUp(self)
        self.clock=1000
        self.patcher=mock.patch('domain_failover.time.time',side_effect=lambda:self.clock)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.token=self.creds['token']
        self.backup=self.pool.save({'domain':'backup.example','port':443,'enabled':True})
        self.nid=self.registry.save('nodes',{'name':'one','server_id':self.sid})['id']
        self.config=test_reality.RealityConfigTests().config()
        self.config['reality'].update(target_host='probe.example',server_name='probe.example')
        self.registry.configure_node(self.nid,self.config)
        self.uid=self.registry.save('users',{'name':'one'})['id']
        self.registry.authorize_node_user(self.nid,self.uid)
        self.failover=Failover(self.registry)
        self.settings={'domain_ids':[self.item['id'],self.backup['id']], 'failure_threshold':2,'cooldown_seconds':300}
        self.initial=self.poll()
        self.ack(self.initial)

    tearDown=test_agent_probe.AgentProbeTests.tearDown

    def poll(self): return self.registry.configuration(self.token)
    def state(self): return self.failover.policy(self.nid)['state']
    def enable(self): return self.failover.policy(self.nid,self.settings)
    def ack(self,snapshot):
        receipt={key:snapshot[key] for key in ('revision','digest')}
        self.registry.acknowledge_configuration(self.token,receipt)
        self.registry.acknowledge_runtime(self.token,receipt)

    def probe(self,domain,result,server=None,token=None):
        self.registry.create_reality_probe_job(server or self.sid,domain['id'])
        claim=self.registry.claim_reality_probe(token or self.token)['job']
        return self.registry.complete_reality_probe(token or self.token,claim['job_id'],claim['lease_token'],result)

    def failures(self,count=2):
        for _ in range(count):
            self.probe(self.item,{'ok':False,'error':'certificate_error'})
            snapshot=self.poll()
        return snapshot

    def switch(self):
        self.enable(); self.probe(self.backup,available())
        return self.failures()

    def test_default_off_and_bad_policy_cannot_mutate_config_or_expose_secrets(self):
        self.assertIsNone(self.failover.policy(self.nid)['policy'])
        self.probe(self.backup,available()); self.failures(3)
        self.assertEqual(self.poll(),self.initial)
        for data in (None,{}, {'domain_ids':[True,2],'failure_threshold':2,'cooldown_seconds':300},
                     {**self.settings,'domain_ids':[self.item['id']]*2},
                     {**self.settings,'failure_threshold':True},{**self.settings,'cooldown_seconds':0}):
            with self.subTest(data=data),self.assertRaises(Error): Failover.validate(data)
        public=self.enable()
        self.assertNotIn('private_key',json.dumps(public))
        self.assertNotIn(self.config['reality']['private_key'],json.dumps(public))
        self.assertEqual(self.poll(),self.initial)

    def test_distinct_rounds_trigger_once_preserve_keys_users_and_confirm(self):
        self.enable(); self.probe(self.backup,available())
        self.failures(1)
        for _ in range(4): self.assertEqual(self.poll(),self.initial)
        self.assertEqual(self.state()['failures'],1)
        self.failover.policy(self.nid,self.settings)
        self.assertEqual(self.state()['failures'],1)
        snapshot=self.failures(1)
        self.assertEqual(snapshot['revision'],self.initial['revision']+1)
        expected=copy.deepcopy(self.initial['nodes'])
        expected[0]['config']['reality'].update(target_host='backup.example',server_name='backup.example')
        self.assertEqual(snapshot['nodes'],expected)
        self.assertEqual(self.state()['phase'],'pending')
        self.assertEqual(self.poll(),snapshot)
        self.ack(snapshot); self.poll()
        self.assertEqual(self.state()['phase'],'monitoring')
        self.assertEqual(self.state()['message'],'applied')
        self.assertEqual(self.state()['cooldown_until'],1300)
        self.assertNotIn('private_key',json.dumps(self.failover.policy(self.nid)))

    def test_retry_attempts_do_not_count_and_success_resets_debounce(self):
        self.enable(); self.probe(self.backup,available())
        retry=self.probe(self.item,{'ok':False,'error':'timeout'})
        self.poll(); self.assertEqual(self.state()['failures'],0)
        self.clock=retry['next_attempt_at']
        claim=self.registry.claim_reality_probe(self.token)['job']
        self.registry.complete_reality_probe(self.token,claim['job_id'],claim['lease_token'],available())
        self.poll(); self.assertEqual(self.state()['failures'],0)
        self.failures(1); self.assertEqual(self.state()['failures'],1)
        self.probe(self.item,available()); self.poll()
        self.assertEqual(self.state()['failures'],0)
        self.failures(1); self.assertEqual(self.state()['phase'],'monitoring')

    def test_other_host_master_and_expired_backup_do_not_trigger_switch(self):
        self.enable()
        other=self.registry.save('servers',{'name':'other'})['id']
        token=self.registry.provision_agent(other)['token']
        self.probe(self.backup,available(),other,token)
        with mock.patch('domain_pool.probe_tls',return_value=available()): self.pool.probe(self.backup['id'])
        self.failures(); self.assertEqual(self.state()['message'],'no_eligible_backup')
        self.probe(self.backup,available())
        self.clock+=301
        self.ack(self.poll())
        self.failures(); self.assertEqual(self.state()['message'],'no_eligible_backup')
        self.assertEqual(self.poll()['nodes'],self.initial['nodes'])

    def test_pending_retry_cannot_use_an_earlier_failure_threshold(self):
        self.enable()
        self.failures()
        self.assertEqual(self.state()['message'],'no_eligible_backup')
        self.probe(self.backup,available())
        retry=self.probe(self.item,{'ok':False,'error':'timeout'})
        self.assertIsNotNone(retry['next_attempt_at'])
        self.assertEqual(self.poll()['nodes'],self.initial['nodes'])
        self.assertEqual(self.state()['phase'],'monitoring')

    def test_switch_requires_current_runtime_receipt_not_staged_only(self):
        self.enable(); self.probe(self.backup,available())
        self.clock+=46
        self.failures(); self.assertEqual(self.state()['message'],'waiting_current_receipt')
        snapshot=self.poll()
        self.registry.acknowledge_configuration(self.token,{key:snapshot[key] for key in ('revision','digest')})
        self.poll(); self.assertEqual(self.state()['message'],'waiting_current_receipt')
        self.ack(snapshot)
        self.poll(); self.assertEqual(self.state()['phase'],'pending')

    def test_timeout_restores_configuration_but_waits_for_rollback_receipt(self):
        changed=self.switch()
        self.clock+=CONFIRM_TIMEOUT
        restored=self.poll()
        self.assertEqual(restored['nodes'],self.initial['nodes'])
        self.assertGreater(restored['revision'],changed['revision'])
        self.assertEqual(self.state()['phase'],'rollback_pending')
        with self.assertRaises(Error): self.ack(changed)
        with self.assertRaises(Error): self.failover.policy(self.nid,delete=True)
        self.ack(restored); self.poll()
        self.assertEqual(self.state()['phase'],'paused')
        self.assertEqual(self.state()['message'],'rolled_back')

    def test_unconfirmed_rollback_is_paused_and_never_reported_as_success(self):
        self.switch(); self.clock+=CONFIRM_TIMEOUT; self.poll()
        self.clock+=CONFIRM_TIMEOUT; self.poll()
        self.assertEqual(self.state()['phase'],'paused')
        self.assertEqual(self.state()['message'],'rollback_unconfirmed')
        self.assertEqual(self.poll()['nodes'],self.initial['nodes'])

    def test_manual_edit_during_pending_is_never_overwritten_by_rollback(self):
        self.switch()
        manual=copy.deepcopy(self.config)
        manual['reality'].update(target_host='manual.example',server_name='manual.example')
        self.registry.configure_node(self.nid,manual)
        self.clock+=CONFIRM_TIMEOUT
        snapshot=self.poll()
        self.assertEqual(snapshot['nodes'][0]['config'],manual)
        self.assertEqual(self.state()['message'],'node_changed')
        self.assertEqual(self.state()['phase'],'paused')

    def test_cooldown_blocks_bounce_and_fresh_failure_can_switch_after_interval(self):
        changed=self.switch(); self.ack(changed); self.poll()
        self.probe(self.item,available())
        for _ in range(2):
            self.probe(self.backup,{'ok':False,'error':'certificate_error'}); self.poll()
        self.assertEqual(self.state()['message'],'cooldown')
        self.clock=1300
        self.probe(self.item,available()); self.ack(self.poll())
        self.poll()
        self.assertEqual(self.state()['phase'],'pending')
        self.assertEqual(self.state()['current_domain_id'],self.item['id'])

    def test_restart_and_concurrent_polls_do_not_double_switch(self):
        self.enable(); self.probe(self.backup,available()); self.failures(1)
        self.probe(self.item,{'ok':False,'error':'certificate_error'})
        self.registry=Registry(self.registry.path)
        with ThreadPoolExecutor(max_workers=4) as workers:
            snapshots=list(workers.map(lambda _:self.poll(),range(4)))
        self.assertTrue(all(snapshot==snapshots[0] for snapshot in snapshots))
        self.assertEqual(snapshots[0]['revision'],self.initial['revision']+1)
        self.clock+=CONFIRM_TIMEOUT
        self.registry=Registry(self.registry.path)
        self.assertEqual(self.poll()['nodes'],self.initial['nodes'])
        self.assertEqual(self.state()['phase'],'rollback_pending')

    def test_node_or_candidate_edit_pauses_and_delete_cascades(self):
        self.enable()
        self.pool.save({'domain':'edited.example','port':443,'enabled':True},self.item['id'])
        self.poll(); self.assertEqual(self.state()['message'],'candidate_changed')
        self.failover.policy(self.nid,delete=True)
        self.assertIsNone(self.state())
        with self.assertRaises(Error): self.enable()
        self.pool.save({'domain':'probe.example','port':443,'enabled':True},self.item['id'])
        self.enable()
        self.registry.delete('nodes',self.nid)
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM reality_failover').fetchone()[0],0)

    def test_transaction_failure_does_not_leave_changed_payload_or_state(self):
        self.enable(); self.probe(self.backup,available()); self.failures(1)
        self.probe(self.item,{'ok':False,'error':'certificate_error'})
        with mock.patch.object(self.registry,'_bump',side_effect=OSError('fixture')):
            with self.assertRaises(OSError): self.poll()
        self.assertEqual(self.state()['failures'],1)
        with self.registry.connect() as db:
            self.assertEqual(json.loads(db.execute('SELECT payload FROM node_configs WHERE node_id=?',(self.nid,)).fetchone()[0]),self.config)
        self.assertEqual(self.poll()['revision'],self.initial['revision']+1)


class FailoverHttpTests(unittest.TestCase):
    setUp=test_core.ApiTests.setUp
    tearDown=test_core.ApiTests.tearDown
    request=test_core.ApiTests.request

    def test_admin_crud_validation_and_no_private_key(self):
        registry=self.server.registry; pool=DomainPool(registry)
        sid=registry.save('servers',{'name':'one'})['id']
        registry.provision_agent(sid)
        nid=registry.save('nodes',{'name':'one','server_id':sid})['id']
        config=test_reality.RealityConfigTests().config()
        registry.configure_node(nid,config)
        ids=[pool.save({'domain':domain,'port':443,'enabled':True})['id'] for domain in ('fixture.example','backup.example')]
        path=f'/api/nodes/{nid}/reality-failover'
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,auth=False)
        self.assertEqual(caught.exception.code,401)
        with self.request(path) as response: self.assertIsNone(json.load(response)['policy'])
        for body in (None,{}, {'domain_ids':ids,'failure_threshold':True,'cooldown_seconds':300}):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path,'PUT',body)
            self.assertIn(caught.exception.code,(400,413))
        settings={'domain_ids':ids,'failure_threshold':2,'cooldown_seconds':300}
        with self.request(path,'PUT',settings) as response:
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            result=json.load(response)
            self.assertEqual(result['policy'],settings)
            self.assertNotIn(config['reality']['private_key'],json.dumps(result))
        with self.request(path,'DELETE') as response: self.assertTrue(json.load(response)['deleted'])
