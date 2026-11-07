"""Bounded persisted probe retries, independent of real network timing."""
from concurrent.futures import ThreadPoolExecutor
import time
import unittest
from unittest import mock

from core import Registry, Error, PROBE_JOB_LEASE, PROBE_MAX_ATTEMPTS, PROBE_RETRY_DELAY
from domain_pool import DomainPool
import test_agent_probe
from test_domain_pool import available


class ProbeRetryTests(unittest.TestCase):
    setUp=test_agent_probe.AgentProbeTests.setUp
    tearDown=test_agent_probe.AgentProbeTests.tearDown

    def create(self):
        return self.registry.create_reality_probe_job(self.sid,self.item['id'])

    def claim(self):
        return self.registry.claim_reality_probe(self.creds['token'])['job']

    def finish(self,claim,result):
        return self.registry.complete_reality_probe(self.creds['token'],claim['job_id'],claim['lease_token'],result)

    def test_transient_failures_back_off_persist_and_stop_at_three_attempts(self):
        task=self.create()
        now=time.time()
        for index in range(PROBE_MAX_ATTEMPTS):
            with mock.patch('core.time.time',return_value=now):
                claim=self.claim()
                self.assertEqual(claim['job_id'],task['job_id'])
                result=self.finish(claim,{'ok':False,'error':'timeout'})
                self.assertIsNone(self.claim())
                with self.assertRaises(Error): self.finish(claim,available())
            self.registry=Registry(self.registry.path)
            if index<PROBE_MAX_ATTEMPTS-1:
                self.assertTrue(result['retry_pending'])
                due=now+PROBE_RETRY_DELAY * 2**index
                self.assertEqual(result['next_attempt_at'],due)
                with mock.patch('core.time.time',return_value=due-.001): self.assertIsNone(self.claim())
                now=due
            else:
                self.assertNotIn('retry_pending',result)
        with mock.patch('core.time.time',return_value=now+10000): self.assertIsNone(self.claim())
        with self.registry.connect() as db:
            row=db.execute('SELECT * FROM reality_probe_jobs WHERE id=?',(task['job_id'],)).fetchone()
        self.assertEqual((row['status'],row['attempts']),('completed',3))
        self.assertEqual(self.pool.list(self.sid)[0]['status'],'unavailable')

    def test_retry_success_finishes_and_new_manual_request_resets_budget(self):
        self.create(); first=self.claim()
        failed=self.finish(first,{'ok':False,'error':'connection_error'})
        with mock.patch('core.time.time',return_value=failed['next_attempt_at']):
            second=self.claim()
            self.assertNotEqual(second['lease_token'],first['lease_token'])
            self.assertEqual(self.finish(second,available())['status'],'available')
        self.assertIsNone(self.claim())
        fresh=self.create(); claim=self.claim()
        self.assertEqual(claim['job_id'],fresh['job_id'])
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT attempts FROM reality_probe_jobs WHERE id=?',(fresh['job_id'],)).fetchone()[0],1)

    def test_permanent_errors_do_not_retry(self):
        for error in ('certificate_error','unsupported_tls_or_alpn','non_public_address','unsupported_local_tls','unknown'):
            with self.subTest(error=error):
                self.create(); claim=self.claim()
                self.assertNotIn('retry_pending',self.finish(claim,{'ok':False,'error':error}))
                self.assertIsNone(self.claim())

    def test_waiting_retry_does_not_block_other_candidate_and_disable_cancels(self):
        old=self.create(); first=self.claim()
        result=self.finish(first,{'ok':False,'error':'dns_error'})
        item=self.pool.save({'domain':'second.example','port':443,'enabled':True})
        second=self.registry.create_reality_probe_job(self.sid,item['id'])
        self.assertEqual(self.claim()['job_id'],second['job_id'])
        self.pool.save({'domain':'probe.example','port':443,'enabled':False},self.item['id'])
        with mock.patch('core.time.time',return_value=result['next_attempt_at']): self.assertIsNone(self.claim())
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT status FROM reality_probe_jobs WHERE id=?',(old['job_id'],)).fetchone()[0],'discarded')

    def test_unreturned_claims_are_bounded_and_publish_explicit_failure(self):
        self.create(); now=time.time(); leases=[]
        for _ in range(PROBE_MAX_ATTEMPTS):
            with mock.patch('core.time.time',return_value=now):
                claim=self.claim(); self.assertIsNotNone(claim); leases.append(claim)
            now+=PROBE_JOB_LEASE
        with mock.patch('core.time.time',return_value=now): self.assertIsNone(self.claim())
        item=DomainPool(Registry(self.registry.path)).list(self.sid)[0]
        self.assertEqual(item['status'],'unavailable')
        self.assertEqual(item['result'],{'ok':False,'error':'lease_expired'})
        for claim in leases:
            with self.assertRaises(Error): self.finish(claim,available())

    def test_concurrent_pollers_only_claim_one_retry(self):
        self.create(); due=self.finish(self.claim(),{'ok':False,'error':'timeout'})['next_attempt_at']
        with mock.patch('core.time.time',return_value=due),ThreadPoolExecutor(max_workers=4) as workers:
            claims=list(workers.map(lambda _:self.claim(),range(4)))
        self.assertEqual(sum(claim is not None for claim in claims),1)

    def test_old_queue_schema_migrates_once_without_losing_active_claim(self):
        task=self.create(); claim=self.claim()
        with self.registry.connect() as db:
            db.execute('CREATE TABLE legacy_probe_jobs AS SELECT id,server_id,domain_id,domain_revision,probe_generation,status,result,created_at,claimed_at,claim_token,completed_at FROM reality_probe_jobs')
            db.execute('DROP TABLE reality_probe_jobs')
            db.execute('ALTER TABLE legacy_probe_jobs RENAME TO reality_probe_jobs')
        with ThreadPoolExecutor(max_workers=3) as workers:
            list(workers.map(lambda _:Registry(self.registry.path),range(3)))
        with self.registry.connect() as db:
            row=db.execute('SELECT * FROM reality_probe_jobs WHERE id=?',(task['job_id'],)).fetchone()
        self.assertEqual(row['attempts'],1)
        self.assertEqual(row['claim_token'],claim['lease_token'])
        self.assertEqual(self.finish(claim,available())['status'],'available')
