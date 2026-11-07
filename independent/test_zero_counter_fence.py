"""Zero-byte identities must obey the same drain boundary as positive usage."""
import unittest
import uuid
from core import Error, Registry
from drain_seal import DrainSeal
from usage_ledger import UsageLedger
import test_drain_seal as base


class ZeroCounterFenceTests(unittest.TestCase):
    setUp=base.DrainSealTests.setUp
    tearDown=base.DrainSealTests.tearDown
    fixtures=base.DrainSealTests.fixtures
    fixture=base.DrainSealTests.fixture
    setup_total=base.DrainSealTests.setup_total
    setup_drain=base.DrainSealTests.setup_drain
    applied=base.DrainSealTests.applied
    report=base.DrainSealTests.report
    data=base.DrainSealTests.data
    encode=staticmethod(base.DrainSealTests.encode)
    envelope=base.DrainSealTests.envelope
    accept=base.DrainSealTests.accept

    def fresh_session(self,r):
        self.session=str(uuid.uuid4())
        r.begin_session(self.token,dict(session_id=self.session))
        self.ledger=UsageLedger(Registry(r.path))

    def zero(self):
        return dict(self.item,upload_bytes=0,download_bytes=0)

    def counts(self,r):
        with r.connect() as db:
            return tuple(db.execute('SELECT count(*) FROM '+table).fetchone()[0]
                         for table in ('usage_counters','usage_reports','usage_deltas'))

    def test_open_cycle_accepts_zero_identity_and_exact_retry(self):
        r=self.setup_total(); self.item=self.zero()
        self.assertFalse(self.report()['duplicate'])
        self.assertTrue(self.report()['duplicate'])
        self.assertEqual(self.counts(r),(1,1,0))

    def test_captured_execution_can_submit_initial_zero_during_drain(self):
        r=self.setup_drain(report=False); self.item=self.zero()
        self.assertFalse(self.report()['duplicate'])
        self.assertEqual(self.counts(r),(1,1,0))

    def test_uncaptured_zero_session_cannot_enter_draining_cycle(self):
        r=self.setup_drain(); self.fresh_session(r); before=self.counts(r)
        with self.assertRaises(Error) as caught:self.report(items=[self.zero()])
        self.assertEqual(caught.exception.status,403)
        self.assertEqual(self.counts(r),before)

    def test_uncaptured_zero_session_cannot_enter_sealed_cycle_after_reopen(self):
        r=self.setup_drain(); self.accept(); sealed=DrainSeal(r).seal(self.period)
        self.assertTrue(self.report()['duplicate'])
        self.fresh_session(r); before=self.counts(r)
        with self.assertRaises(Error) as caught:self.report(items=[self.zero()])
        self.assertEqual(caught.exception.status,409)
        self.assertEqual(self.counts(r),before)
        self.assertEqual(DrainSeal(Registry(r.path)).seal(self.period)['digest'],sealed['digest'])

    def test_rejected_zero_identity_rolls_back_prior_valid_batch_item(self):
        r=self.setup_drain(); self.accept(); DrainSeal(r).seal(self.period)
        uid=r.save('users',dict(name='unrelated'))['id']
        credential=r.authorize_node_user(self.nid,uid)['uuid']
        self.fresh_session(r); before=self.counts(r)
        valid=dict(self.item,user_id=uid,credential=credential,upload_bytes=1,download_bytes=2)
        with self.assertRaises(Error):self.report(items=[valid,self.zero()])
        self.assertEqual(self.counts(r),before)
        self.assertEqual(self.ledger.user(uid)['upload_bytes'],0)
