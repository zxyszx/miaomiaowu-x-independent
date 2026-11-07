import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest import mock

import test_usage_outbox
from usage_outbox import UsageOutbox
from usage_recovery_cli import ExistingOutbox,inspect,retry


class RecoveryCliTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot

    def rejected(self):
        self.box.capture(self.session,self.snapshot())
        report=self.box.next_report(self.session)
        self.box.isolate(report,100)
        return report

    def cli(self,*args):
        return subprocess.run([sys.executable,str(Path(__file__).with_name('usage_recovery_cli.py')),
            '--database',str(self.path),'--server-id','1',*args],capture_output=True,text=True)

    def test_inspect_readonly_sanitized_and_subprocess_retry_audited(self):
        self.rejected(); before=Path(self.path).read_bytes()
        result=self.cli('inspect'); self.assertEqual(result.returncode,0,result.stdout)
        rows=json.loads(result.stdout); self.assertEqual(len(rows),1)
        self.assertNotIn(self.item['credential'],result.stdout)
        self.assertEqual(Path(self.path).read_bytes(),before)
        row=rows[0]
        result=self.cli('retry','--session',self.session,'--sequence','1',
                        '--report-digest',row['report_digest'],'--reason','execution proof repaired')
        self.assertEqual(result.returncode,0,result.stdout)
        self.assertEqual(json.loads(result.stdout)['status'],'queued_for_retry')
        with self.box.connect() as db:
            audit=dict(db.execute('SELECT * FROM recovery_actions').fetchone())
            totals=dict(db.execute('SELECT * FROM totals').fetchone())
        self.assertEqual((audit['operator'],audit['reason'],audit['report_digest']),
                         (f'uid:{os.geteuid()}','execution proof repaired',row['report_digest']))
        self.assertEqual((totals['acked_upload'],totals['acked_download']),(0,0))
        self.assertEqual(len(self.box.isolation_status()['failures']),1)
        self.assertEqual(self.box.next_report(self.session)['sequence'],2)

    def test_stale_digest_sequence_repeat_and_later_rejection_are_rejected(self):
        self.rejected(); box=ExistingOutbox(self.path,1)
        digest=inspect(box)[0][1]['report_digest']
        for seq,hash_value in ((1,'0'*64),(2,digest)):
            with self.assertRaises(ValueError): retry(box,self.session,seq,hash_value,'repair')
        retry(box,self.session,1,digest,'repair')
        with self.assertRaises(ValueError): retry(box,self.session,1,digest,'repair')
        newer=self.box.next_report(self.session); self.box.isolate(newer,110)
        with self.assertRaises(ValueError): retry(box,self.session,1,digest,'repair')
        self.assertEqual(self.box.isolation_status()['quarantined'][0]['rejected_sequence'],2)

    def test_invalid_metadata_and_atomic_failure_do_not_release_quarantine(self):
        report=self.rejected(); before=self.box.isolation_status()
        for actor,reason in ((None,'why'),('uid:1',None),('uid:1',' '),('uid:1','x\ny'),('uid:1','x'*501)):
            with self.assertRaises(ValueError):
                self.box.retry_quarantined(report,101,operator=actor,reason=reason)
        with self.box.connect() as db:
            db.execute("CREATE TRIGGER fail_action BEFORE INSERT ON recovery_actions BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.box.retry_quarantined(report,101,operator='uid:1',reason='repair')
        self.assertEqual(self.box.isolation_status(),before)
        self.assertEqual(self.box.recovery_status(),[])
        with self.box.connect() as db:
            db.execute('DROP TRIGGER fail_action')
            db.execute("CREATE TRIGGER fail_delete BEFORE DELETE ON quarantine BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.box.retry_quarantined(report,102,operator='uid:1',reason='repair')
        with self.box.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM recovery_actions').fetchone()[0],0)
        self.assertEqual(self.box.isolation_status(),before)

    def test_no_migration_no_creation_and_owner_private_file_checks(self):
        self.rejected()
        missing=Path(self.path).with_name('missing.db')
        with self.assertRaises(FileNotFoundError): ExistingOutbox(missing,1)
        self.assertFalse(missing.exists())
        link=Path(self.path).with_name('alias.db'); link.symlink_to(self.path)
        with self.assertRaises(ValueError): ExistingOutbox(link,1)
        os.chmod(self.path,0o640)
        with self.assertRaises(ValueError): ExistingOutbox(self.path,1)
        os.chmod(self.path,0o600)
        with mock.patch('usage_recovery_cli.os.geteuid',return_value=os.geteuid()+1):
            with self.assertRaises(ValueError): ExistingOutbox(self.path,1)
        with self.assertRaises(ValueError): inspect(ExistingOutbox(self.path,2))
        with self.box.connect() as db: db.execute('DROP TABLE recovery_actions')
        box=ExistingOutbox(self.path,1); digest=inspect(box)[0][1]['report_digest']
        with self.assertRaises(sqlite3.OperationalError): retry(box,self.session,1,digest,'repair')
        self.assertEqual(len(self.box.isolation_status()['quarantined']),1)
        self.assertEqual(self.box.recovery_status(),[])
        UsageOutbox(self.path,1)  # Only normal runtime initialization migrates.
        retry(box,self.session,1,digest,'repair')

    def test_intervening_retry_and_rejection_is_rechecked_inside_transaction(self):
        self.rejected(); box=ExistingOutbox(self.path,1)
        digest=inspect(box)[0][1]['report_digest']; original=box.retry_quarantined
        def changed(report,now,**kwargs):
            self.box.retry_quarantined(report,101)
            self.box.isolate(self.box.next_report(self.session),102)
            return original(report,now,**kwargs)
        with mock.patch.object(box,'retry_quarantined',side_effect=changed):
            with self.assertRaises(ValueError): retry(box,self.session,1,digest,'repair')
        self.assertEqual(self.box.isolation_status()['quarantined'][0]['rejected_sequence'],2)

    def test_retry_never_resolves_unknown_writes_or_changes_accounting(self):
        self.rejected()
        self.box.prepare_write(self.session,dict(self.item,upload_bytes=10))
        before=self.box.uncertain_writes()
        box=ExistingOutbox(self.path,1); row=inspect(box)[0][1]
        retry(box,self.session,1,row['report_digest'],'permission repaired; payload remains unknown')
        self.assertEqual(self.box.uncertain_writes(),before)
        self.assertEqual(self.box.write_resolution_history(),[])
        report=self.box.next_report(self.session)
        self.assertEqual(report['items'],[self.item])


if __name__=='__main__': unittest.main()
