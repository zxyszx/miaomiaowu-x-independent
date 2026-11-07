"""Authenticated first-grant usage requires a complete execution receipt."""
import unittest

from agent import request
from core import Error
import test_grant_execution as fixture


class GrantUsageHTTPTests(unittest.TestCase):
    def usage(self,sequence=1,upload=2,download=1):
        return request(self.url,self.token,'/agent/grants/usage',dict(
            grant_id=self.g['grant_id'],policy_id=self.g['policy_id'],
            session_id=self.session,sequence=sequence,
            upload_bytes=upload,download_bytes=download))

    def test_applied_and_installed_without_execution_cannot_confirm_usage(self):
        self.setup_installed()
        self.rejected(409,self.usage)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0],0)
            self.assertEqual(db.execute('''SELECT count(*) FROM
                usage_cycle_accounting_events WHERE event_kind='first_grant' ''').fetchone()[0],0)

    def test_complete_execution_http_usage_and_retry_commit_once(self):
        self.setup_installed();execution=self.execute()
        first=self.usage()
        self.assertTrue(first['accepted']);self.assertFalse(first['duplicate'])
        self.assertEqual((first['confirmed_bytes'],first['held_bytes']),(3,3))
        retry=self.usage()
        self.assertTrue(retry['duplicate'])
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0],1)
            event=db.execute('''SELECT upload_delta,download_delta FROM
                usage_cycle_accounting_events WHERE event_kind='first_grant' ''').fetchone()
            self.assertEqual(tuple(event),('2','1'))
            source=db.execute('''SELECT upload_bytes,download_bytes,execution_id
                FROM usage_attributed_sources WHERE ledger_kind='first_grant' ''').fetchone()
            self.assertEqual(tuple(source),( '2','1',execution['execution']['execution_id']))

    def test_overcommitted_policy_still_allows_existing_hold_conversion(self):
        self.setup_installed();self.execute()
        self.cycles.set_total_budget(self.period,0)
        result=self.usage()
        self.assertEqual((result['confirmed_bytes'],result['held_bytes']),(3,3))
        with self.assertRaises(Error) as caught:
            from first_grant import FirstGrant
            FirstGrant(self.r).prepare(self.token,self.request(1))
        self.assertEqual(caught.exception.status,409)

    def test_event_storage_failure_http_500_rolls_back_without_detail(self):
        self.setup_installed();self.execute()
        with self.r.connect() as db:
            db.execute("""CREATE TRIGGER fail_first_event BEFORE INSERT ON
                usage_cycle_accounting_events WHEN NEW.event_kind='first_grant'
                BEGIN SELECT RAISE(ABORT,'private accounting detail'); END""")
        self.rejected(500,self.usage)
        with self.r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_first_total_meter').fetchone()[0],0)
            self.assertEqual(tuple(db.execute('''SELECT upload_bytes,download_bytes FROM
                usage_attributed_sources WHERE ledger_kind='first_grant' ''').fetchone()),('0','0'))


for name in ('setUp','tearDown','fixtures','fixture','setup_total','setup_drain','applied',
             'report','data','envelope','accept','sealed','request','prepare','start',
             'start_http','fetch','post','ack','setup_lifecycle','lifecycle',
             'setup_installed','execute','rejected','execution_fact'):
    setattr(GrantUsageHTTPTests,name,getattr(fixture.GrantExecutionTests,name))
GrantUsageHTTPTests.encode=staticmethod(fixture.GrantExecutionTests.encode)


if __name__=='__main__':unittest.main()
