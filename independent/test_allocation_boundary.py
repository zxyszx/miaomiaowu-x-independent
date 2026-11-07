"""Cross-protocol HTTP checks before introducing executable allocation grants.

Evidence ACKs must not become legacy usage, authority, or drain completion.
These exercise the existing protocols; they do not activate a new protocol.
"""
from concurrent.futures import ThreadPoolExecutor
import unittest
import urllib.error

from agent import request as send
from allocation_outbox import acknowledge
from core import Registry, Error
from drain_seal import DrainSeal
from total_allocation import TotalAllocations
from total_usage import current
from usage_migration import UsageMigration
import test_allocation_http as http
import test_usage_settlement as settlement
import test_drain_checkpoints as checkpoints


class AllocationBoundaryTests(unittest.TestCase):
    setUp=http.EvidenceHTTPTests.setUp
    tearDown=http.EvidenceHTTPTests.tearDown
    fixtures=http.EvidenceHTTPTests.fixtures
    fixture=http.EvidenceHTTPTests.fixture
    setup_total=http.EvidenceHTTPTests.setup_total
    request=http.EvidenceHTTPTests.request
    reserved=http.EvidenceHTTPTests.reserved
    prepare=http.EvidenceHTTPTests.prepare
    count=http.EvidenceHTTPTests.count
    start=http.EvidenceHTTPTests.start
    applied=settlement.SettlementTests.applied
    encode=staticmethod(checkpoints.DrainCheckpointTests.encode)

    def legacy(self):
        return dict(session_id=self.session,sequence=1,items=[dict(self.item)])

    def assert_usage(self,expected):
        with self.r.connect() as db:
            db.execute('BEGIN')
            self.assertEqual(current(db,self.period),expected)
        self.assertEqual(self.reserved(self.r),7)

    def reject(self,path,data,status):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            send(self.url,self.token,path,data)
        self.assertEqual(caught.exception.code,status)

    def test_ack_cannot_relabel_a_hold_as_legacy_usage_or_execution(self):
        self.start()
        _,ticket=self.quota.reserve_write(0,7)
        self.quota.complete_write(ticket)  # Controlled local completion only.
        frozen=self.quota.freeze_evidence()
        for phase in ('before','after'):
            with self.subTest(phase=phase):
                self.reject('/agent/usage',self.legacy(),409)
                self.reject('/agent/usage/settle',self.legacy(),403)
                self.assert_usage(0)
            receipt=send(self.url,self.token,http.PATH,frozen)
            self.assertFalse(receipt['ready_for_execution'])
            acknowledge(self.quota,receipt)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_execution_grants').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],0)

    def test_legacy_confirmation_and_evidence_remain_distinct_after_restart(self):
        self.start();self.cycles.set_total_budget(self.period,20)
        legacy=self.legacy()
        self.assertFalse(send(self.url,self.token,'/agent/usage',legacy)['duplicate'])
        _,ticket=self.quota.reserve_write(1,7);self.quota.complete_write(ticket)
        frozen=self.quota.freeze_evidence()
        reply=send(self.url,self.token,http.PATH,frozen)
        self.r=Registry(self.r.path)  # Reopen accounting, not a new Agent identity.
        self.assertTrue(send(self.url,self.token,'/agent/usage',legacy)['duplicate'])
        self.assertTrue(send(self.url,self.token,http.PATH,frozen)['duplicate'])
        self.assertTrue(acknowledge(self.quota,reply));self.assert_usage(7)
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_deltas').fetchone()[0],1)
        for path in ('/agent/usage','/agent/usage/settle'):
            self.reject(path,dict(legacy,allocation_id=self.scope['allocation_id']),400)
            tagged=dict(legacy,items=[dict(self.item,allocation_id=self.scope['allocation_id'])])
            self.reject(path,tagged,400)
        self.assert_usage(7)

    def test_complete_evidence_does_not_close_drain_or_restore_clients(self):
        self.start();self.cycles.set_total_budget(self.period,20)
        self.sid=self.applied(self.r)['server_id']
        send(self.url,self.token,'/agent/usage',self.legacy())
        migration=UsageMigration(self.r);migration.begin_drain(self.period)
        envelope=self.encode(dict(format=1,server_id=self.sid,session_id=self.session,
                                  items=[dict(self.item)],uncertain_writes=[]))
        migration.accept_closed(self.period,self.token,envelope)
        _,ticket=self.quota.reserve_write(0,7);self.quota.complete_write(ticket)
        receipt=send(self.url,self.token,http.PATH,self.quota.freeze_evidence())
        acknowledge(self.quota,receipt)
        reopened=Registry(self.r.path);gate=DrainSeal(reopened)
        self.assertIn('unresolved_allocations',gate.status(self.period)['blockers'])
        with self.assertRaises(Error):gate.seal(self.period)
        with self.assertRaises(Error):TotalAllocations(reopened).reserve(self.token,self.request(1))
        self.assertTrue(all(not node['clients'] for node in reopened.configuration(self.token)['nodes']))
        self.assert_usage(7)

    def test_parallel_http_evidence_and_legacy_report_do_not_share_balance(self):
        self.start();frozen=self.quota.freeze_evidence()
        def legacy():
            try:send(self.url,self.token,'/agent/usage',self.legacy())
            except urllib.error.HTTPError as error:return error.code
            return 200
        with ThreadPoolExecutor(2) as pool:
            evidence=pool.submit(send,self.url,self.token,http.PATH,frozen)
            old=pool.submit(legacy)
            self.assertEqual(old.result(),409)
            receipt=evidence.result()
        self.assertTrue(receipt['evidence_only']);self.assertEqual(self.count(),1)
        self.assertTrue(acknowledge(self.quota,receipt));self.assert_usage(0)
