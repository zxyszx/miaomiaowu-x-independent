"""Master execution receipts remain explicit audit records, never readiness."""
import copy
import json
import time
import unittest
import urllib.error
import uuid

from agent import request
from grant_execution import GrantExecution
from grant_lifecycle import GrantLifecycle
from grant_capabilities import STAGING, INSTALLATION
import test_grant_lifecycle as fixture


class GrantExecutionTests(unittest.TestCase):
    def execution_fact(self, **changes):
        value=dict(self.fact,
                   execution_id=str(uuid.uuid4()))
        value.update(changes)
        return value

    def execute(self, fact=None):
        return request(self.url, self.token, '/agent/grants/execution',
                       {'fact': self.execution_fact() if fact is None else fact})

    def row(self):
        with self.r.connect() as db:
            return db.execute('SELECT * FROM usage_first_total_execution').fetchone()

    def rejected(self, status, fn):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            fn()
        try:
            self.assertEqual(caught.exception.code, status)
        finally:
            caught.exception.close()

    def setup_installed(self):
        self.setup_lifecycle()
        self.lifecycle('begin')
        self.lifecycle('installed')

    def test_records_only_after_installed_and_retries_exactly(self):
        self.setup_installed()
        first=self.execute()
        self.assertTrue(first['accepted'])
        self.assertFalse(first['ready_for_execution'])
        self.assertEqual(first['state'],'execution_recorded')
        self.assertEqual(first['execution']['grant_id'],self.g['grant_id'])
        self.assertEqual(self.execute(first['execution']),first)
        self.assertEqual(tuple(self.row()),
                         (self.g['grant_id'],
                          json.dumps(first['execution'], sort_keys=True,
                                     separators=(',', ':')),
                          first['recorded_at']))

    def test_requires_lifecycle_and_installation_ack(self):
        self.start_http(offer=[STAGING, INSTALLATION]);self.fetch()
        from grant_ack import fact_for
        from grant_configuration import GrantConfiguration
        self.fact=fact_for(GrantConfiguration(self.r).bind(
            self.token, self.g['grant_id']))
        self.rejected(409,self.execute)
        # Keep the original negotiated session and staged configuration.  The
        # installation ACK is required before lifecycle.begin, while begin
        # itself must still be insufficient for an execution receipt.
        self.ack();self.lifecycle('begin')
        self.rejected(409,self.execute)
        self.assertIsNone(self.row())

    def test_closed_lifecycle_cannot_record(self):
        self.setup_installed();self.lifecycle('close')
        self.rejected(409,self.execute)
        self.assertIsNone(self.row())

    def test_fact_and_identity_mismatches_do_not_write(self):
        self.setup_installed()
        for key, value in (('digest','0'*64),('revision',self.fact['revision']+1),
                           ('protocol','other'),('execution_id','bad')):
            bad=self.execution_fact(**{key:value})
            with self.assertRaises(urllib.error.HTTPError) as caught:
                request(self.url,self.token,'/agent/grants/execution',{'fact':bad})
            try:self.assertIn(caught.exception.code,(400,409))
            finally:caught.exception.close()
            self.assertIsNone(self.row())
        bad=self.execution_fact(session_id=str(uuid.uuid4()))
        self.rejected(403,lambda:request(self.url,self.token,'/agent/grants/execution',{'fact':bad}))
        self.assertIsNone(self.row())

    def test_first_record_expiry_rolls_back(self):
        self.setup_installed()
        with self.r.connect() as db:
            db.execute("UPDATE usage_first_total_grants SET valid_until=0")
        self.rejected(409,self.execute);self.assertIsNone(self.row())

    def test_runtime_digest_and_valid_other_protocol_cannot_replace_installation(self):
        self.setup_installed()
        for changes in ({'runtime_digest':'0'*64},
                        {'protocol':'reality' if self.fact['protocol']=='vless' else 'vless'}):
            with self.subTest(changes=changes):
                self.rejected(409, lambda: self.execute(self.execution_fact(**changes)))
                self.assertIsNone(self.row())

    def test_missing_installation_fact_cannot_be_rebuilt_from_applied(self):
        self.setup_installed()
        with self.r.connect() as db:
            db.execute('DELETE FROM usage_first_total_installations')
        self.rejected(409,self.execute)
        self.assertIsNone(self.row())

    def test_exact_retry_rechecks_original_installation(self):
        self.setup_installed()
        result=self.execute()
        with self.r.connect() as db:
            db.execute('DELETE FROM usage_first_total_installations')
        self.rejected(409,lambda:self.execute(result['execution']))
        self.assertEqual(json.loads(self.row()['fact']),result['execution'])

    def test_storage_failure_rolls_back(self):
        self.setup_installed()
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER reject_execution BEFORE INSERT ON usage_first_total_execution BEGIN SELECT RAISE(ABORT,'private'); END")
        self.rejected(500,self.execute);self.assertIsNone(self.row())


for name in ('setUp','tearDown','fixtures','fixture','setup_total','setup_drain','applied',
             'report','data','envelope','accept','sealed','request','prepare','start',
             'start_http','fetch','post','ack','setup_lifecycle','lifecycle'):
    setattr(GrantExecutionTests,name,getattr(fixture.GrantLifecycleTests,name))
GrantExecutionTests.encode=staticmethod(fixture.GrantLifecycleTests.encode)


if __name__ == '__main__':
    unittest.main()
