import json
import unittest
from unittest import mock
import agent
from core import Registry, Error
from domain_pool import DomainPool
from test_domain_pool import available
from pathlib import Path
import tempfile
import time
import urllib.error
from core import PROBE_JOB_LEASE

class AgentProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.registry=Registry(Path(self.temp.name)/'db')
        self.sid=self.registry.save('servers',{'name':'remote'})['id']; self.creds=self.registry.provision_agent(self.sid)
        self.pool=DomainPool(self.registry); self.item=self.pool.save({'domain':'probe.example','port':443,'enabled':True})
    def tearDown(self): self.temp.cleanup()

    def test_probe_delegates_to_bounded_domain_probe(self):
        result={'ok':True,'tls_version':'TLSv1.3','alpn':'h2','latency_ms':3}
        with mock.patch('agent.probe_tls',return_value=result) as probe:
            self.assertEqual(agent.probe_domain('fixture.example',443),result)
        probe.assert_called_once_with('fixture.example',443)

    def test_probe_rejects_bad_shape_before_network(self):
        for domain,port in (('',443),('fixture.example',True),('fixture.example','443')):
            with self.subTest(domain=domain,port=port),mock.patch('agent.probe_tls') as probe:
                with self.assertRaises(ValueError): agent.probe_domain(domain,port)
                probe.assert_not_called()

    def test_cli_probe_outputs_result_without_credentials(self):
        with mock.patch('agent.probe_domain',return_value={'ok':False,'error':'dns_error'}):
            with mock.patch('sys.argv',['agent.py','--probe-domain','fixture.example','--probe-port','8443']):
                with mock.patch('builtins.print') as output:
                    self.assertEqual(agent.main(),0)
        self.assertEqual(json.loads(output.call_args.args[0])['error'],'dns_error')

    def test_agent_claims_and_completes_job_and_stale_result_is_rejected(self):
        job=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        claimed=self.registry.claim_reality_probe(self.creds['token'])['job']
        self.assertEqual(claimed['job_id'],job['job_id'])
        result=available(4)
        self.assertEqual(self.registry.complete_reality_probe(self.creds['token'],job['job_id'],claimed['lease_token'],result)['status'],'available')
        self.assertEqual(self.pool.select(self.sid)['id'],self.item['id'])
        with self.assertRaises(Error): self.registry.complete_reality_probe(self.creds['token'],job['job_id'],claimed['lease_token'],result)

    def test_job_result_is_discarded_when_candidate_changes(self):
        job=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        claimed=self.registry.claim_reality_probe(self.creds['token'])['job']
        self.pool.save({'domain':'changed.example','port':443,'enabled':True},self.item['id'])
        with self.assertRaises(Error): self.registry.complete_reality_probe(self.creds['token'],job['job_id'],claimed['lease_token'],available())
        self.assertEqual(self.pool.list(self.sid)[0]['status'],'not_checked')

    def test_expired_claim_can_be_reclaimed_and_old_lease_is_rejected(self):
        job=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        first=self.registry.claim_reality_probe(self.creds['token'])['job']
        with self.registry.connect() as db:
            claimed_at=db.execute('SELECT claimed_at FROM reality_probe_jobs WHERE id=?',(job['job_id'],)).fetchone()['claimed_at']
        with mock.patch('core.time.time', return_value=claimed_at + PROBE_JOB_LEASE + 1):
            second=self.registry.claim_reality_probe(self.creds['token'])['job']
        self.assertEqual(second['job_id'],job['job_id'])
        self.assertNotEqual(second['lease_token'],first['lease_token'])
        with self.assertRaises(Error):
            self.registry.complete_reality_probe(self.creds['token'],job['job_id'],first['lease_token'],available())
        self.assertEqual(self.registry.complete_reality_probe(self.creds['token'],job['job_id'],second['lease_token'],available())['status'],'available')

    def test_new_probe_supersedes_older_pending_and_claimed_tasks(self):
        first=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        claimed=self.registry.claim_reality_probe(self.creds['token'])['job']
        second=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        with self.registry.connect() as db:
            rows=db.execute('SELECT id,status FROM reality_probe_jobs ORDER BY created_at').fetchall()
        self.assertEqual(rows[0]['id'],first['job_id'])
        self.assertEqual(rows[0]['status'],'discarded')
        self.assertEqual(rows[1]['id'],second['job_id'])
        self.assertEqual(rows[1]['status'],'pending')
        with self.assertRaises(Error):
            self.registry.complete_reality_probe(self.creds['token'],first['job_id'],claimed['lease_token'],available())
        self.assertEqual(self.registry.claim_reality_probe(self.creds['token'])['job']['job_id'],second['job_id'])

    def test_expired_result_rejected_before_another_agent_reclaims(self):
        job=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        claim=self.registry.claim_reality_probe(self.creds['token'])['job']
        with self.registry.connect() as db:
            start=db.execute('SELECT claimed_at FROM reality_probe_jobs WHERE id=?',(job['job_id'],)).fetchone()['claimed_at']
        with mock.patch('core.time.time',return_value=start+PROBE_JOB_LEASE):
            with self.assertRaises(Error) as ctx:
                self.registry.complete_reality_probe(self.creds['token'],job['job_id'],claim['lease_token'],available())
            self.assertEqual(ctx.exception.status,409)
            self.assertEqual(self.pool.list(self.sid)[0]['status'],'checking')
            retry=self.registry.claim_reality_probe(self.creds['token'])['job']
        self.assertEqual(retry['job_id'],job['job_id'])
        self.assertNotEqual(retry['lease_token'],claim['lease_token'])

    def test_changed_or_disabled_candidate_is_not_dispatched(self):
        for enabled in (True,False):
            with self.subTest(enabled=enabled):
                self.pool.save({'domain':'probe.example','port':443,'enabled':True},self.item['id'])
                job=self.registry.create_reality_probe_job(self.sid,self.item['id'])
                self.pool.save({'domain':'changed.example','port':443,'enabled':enabled},self.item['id'])
                self.assertIsNone(self.registry.claim_reality_probe(self.creds['token'])['job'])
                with self.registry.connect() as db:
                    row=db.execute('SELECT status,completed_at FROM reality_probe_jobs WHERE id=?',(job['job_id'],)).fetchone()
                self.assertEqual(row['status'],'discarded')
                self.assertIsNotNone(row['completed_at'])

    def test_new_server_probe_does_not_supersede_other_server(self):
        old=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        other=self.registry.save('servers',{'name':'second'})['id']
        credentials=self.registry.provision_agent(other)
        latest=self.registry.create_reality_probe_job(other,self.item['id'])
        self.assertEqual(self.registry.claim_reality_probe(self.creds['token'])['job']['job_id'],old['job_id'])
        self.assertEqual(self.registry.claim_reality_probe(credentials['token'])['job']['job_id'],latest['job_id'])

    def test_stale_queue_head_is_retired_and_current_job_survives_restart(self):
        stale=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        self.pool.save({'domain':'changed.example','port':443,'enabled':True},self.item['id'])
        second=self.pool.save({'domain':'second.example','port':443,'enabled':True})
        latest=self.registry.create_reality_probe_job(self.sid,second['id'])
        restarted=Registry(self.registry.path)
        claimed=restarted.claim_reality_probe(self.creds['token'])['job']
        self.assertEqual(claimed['job_id'],latest['job_id'])
        with Registry(self.registry.path).connect() as db:
            self.assertEqual(db.execute('SELECT status FROM reality_probe_jobs WHERE id=?',(stale['job_id'],)).fetchone()['status'],'discarded')
        self.assertEqual(restarted.complete_reality_probe(self.creds['token'],latest['job_id'],claimed['lease_token'],available())['status'],'available')

    def test_invalid_success_receipt_cannot_publish_available_or_consume_lease(self):
        invalid = [dict(ok=True), dict(available(), tls_version='TLSv1.2'),
                   dict(available(), alpn='http/1.1'), dict(available(), latency_ms=True),
                   dict(available(), latency_ms=-1), dict(available(), latency_ms=float('inf')),
                   dict(available(), error='timeout'), dict(ok='true'), dict(error='timeout')]
        for result in invalid:
            with self.subTest(result=result):
                task=self.registry.create_reality_probe_job(self.sid,self.item['id'])
                claim=self.registry.claim_reality_probe(self.creds['token'])['job']
                with self.assertRaises(Error) as caught:
                    self.registry.complete_reality_probe(self.creds['token'],task['job_id'],claim['lease_token'],result)
                self.assertEqual(caught.exception.status,400)
                self.assertEqual(self.pool.list(self.sid)[0]['status'],'checking')
                self.assertEqual(self.registry.complete_reality_probe(self.creds['token'],task['job_id'],claim['lease_token'],available())['status'],'available')

    def test_failed_probe_receipt_is_persisted_and_excluded_from_selection(self):
        task=self.registry.create_reality_probe_job(self.sid,self.item['id'])
        claim=self.registry.claim_reality_probe(self.creds['token'])['job']
        result={'ok':False,'error':'timeout'}
        self.assertEqual(self.registry.complete_reality_probe(self.creds['token'],task['job_id'],claim['lease_token'],result)['status'],'unavailable')
        self.assertEqual(DomainPool(Registry(self.registry.path)).list(self.sid)[0]['result'],result)
        with self.assertRaises(Error): self.pool.select(self.sid)

    def test_optional_probe_failure_does_not_skip_configuration_sync(self):
        failures=[OSError('offline'),Error('busy',503),ValueError('invalid receipt'),
                  KeyError('lease_token'),urllib.error.HTTPError('local',409,'stale',{},None)]
        config=Path(self.temp.name)/'agent.json'
        config.write_text(json.dumps({'master':'http://127.0.0.1','token':'fixture'}))
        for failure in failures:
            with self.subTest(failure=type(failure).__name__), \
                 mock.patch('sys.argv',['agent.py','--config',str(config),'--once']), \
                 mock.patch('agent.begin_session'),mock.patch('agent.send'), \
                 mock.patch('agent.sync') as sync, \
                 mock.patch('agent.probe_pending',side_effect=failure),mock.patch('builtins.print') as output:
                self.assertEqual(agent.main(),0)
                sync.assert_called_once()
                self.assertTrue(any('probe failed' in str(call) for call in output.call_args_list))

    def test_probe_authentication_failure_is_not_hidden_as_optional(self):
        config=Path(self.temp.name)/'agent.json'
        config.write_text(json.dumps({'master':'http://127.0.0.1','token':'fixture'}))
        for status in (401,403):
            with self.subTest(status=status), \
                 mock.patch('sys.argv',['agent.py','--config',str(config),'--once']), \
                 mock.patch('agent.begin_session'),mock.patch('agent.send'),mock.patch('agent.sync'), \
                 mock.patch('agent.probe_pending',side_effect=urllib.error.HTTPError('local',status,'denied',{},None)), \
                 mock.patch('builtins.print'):
                self.assertEqual(agent.main(),1)

    def test_failed_sync_never_starts_optional_probe(self):
        config=Path(self.temp.name)/'agent.json'
        config.write_text(json.dumps({'master':'http://127.0.0.1','token':'fixture'}))
        with mock.patch('sys.argv',['agent.py','--config',str(config),'--once']), \
             mock.patch('agent.begin_session'),mock.patch('agent.send'), \
             mock.patch('agent.sync',side_effect=ValueError('rejected')), \
             mock.patch('agent.probe_pending') as probe,mock.patch('builtins.print'):
            self.assertEqual(agent.main(),1)
            probe.assert_not_called()
