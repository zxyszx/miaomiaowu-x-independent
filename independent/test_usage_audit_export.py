"""Local evidence export must be private, consistent and non-destructive."""
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
import uuid
from unittest import mock

import test_usage_outbox
import usage_audit_export as exporter
from usage_outbox import UsageOutbox


class AuditExportTests(unittest.TestCase):
    setUp=test_usage_outbox.OutboxTests.setUp
    snapshot=test_usage_outbox.OutboxTests.snapshot

    def fixture(self):
        self.box.capture(self.session,self.snapshot())
        self.box.prepare_write(self.session,dict(self.item,upload_bytes=9))
        self.box=UsageOutbox(self.path,1)
        self.destination=Path(self.temp.name)/'evidence.json'

    def test_private_export_includes_exact_bounds_and_redacts_credentials(self):
        self.fixture()
        other=str(uuid.uuid4()); credential=str(uuid.uuid4())
        item=dict(self.item,credential=credential)
        self.box.capture(other,self.snapshot(item))
        self.box.prepare_write(other,dict(item,download_bytes=11))
        self.box=UsageOutbox(self.path,1)
        self.box.capture(other,self.snapshot(dict(item,download_bytes=11)))
        before=self.path.read_bytes()
        result=exporter.export_evidence(self.path,1,self.destination)
        content=self.destination.read_text(); data=json.loads(content)
        self.assertEqual((result['unresolved'],result['resolutions']),(1,1))
        self.assertEqual(data['server_id'],1)
        self.assertEqual((data['unresolved'][0]['committed_upload'],data['unresolved'][0]['possible_upload'],
                          data['unresolved'][0]['observed_lower_upload']),(5,9,5))
        self.assertEqual(data['resolutions'][0]['checkpoint_download'],11)
        self.assertEqual(data['resolutions'][0]['source'],'successful_checkpoint')
        self.assertNotIn(self.item['credential'],content)
        self.assertNotIn(credential,content)
        self.assertEqual(data['unresolved'][0]['credential_fingerprint'],hashlib.sha256(self.item['credential'].encode()).hexdigest())
        self.assertEqual(self.destination.stat().st_mode&0o777,0o600)
        self.assertEqual(result['sha256'],hashlib.sha256(self.destination.read_bytes()).hexdigest())
        self.assertEqual(self.path.read_bytes(),before)

    def test_unobserved_intent_export_is_read_only_and_does_not_create_observation(self):
        self.fixture()
        self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=9)))
        other=str(uuid.uuid4())
        self.box.prepare_write(other,self.item)
        result=exporter.export_evidence(self.path,1,self.destination)
        self.assertEqual(result['unresolved'],1)
        row=json.loads(self.destination.read_text())['unresolved'][0]
        self.assertIsNone(row['observed_lower_upload'])
        self.assertEqual((row['committed_upload'],row['possible_upload']),(0,5))
        with self.box.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM recovered_writes WHERE session=?',(other,)).fetchone()[0],0)

    def test_existing_file_symlink_and_source_are_never_overwritten(self):
        self.fixture()
        self.destination.write_text('keep me')
        for target in (self.destination,self.path):
            before=target.read_bytes()
            with self.assertRaises(FileExistsError): exporter.export_evidence(self.path,1,target)
            self.assertEqual(target.read_bytes(),before)
        link=Path(self.temp.name)/'link.json'; link.symlink_to(self.destination)
        with self.assertRaises(FileExistsError): exporter.export_evidence(self.path,1,link)
        self.assertEqual(self.destination.read_text(),'keep me')
        self.assertEqual(list(Path(self.temp.name).glob('.usage-evidence-*')),[])

    def test_missing_input_bad_identity_and_legacy_schema_do_not_publish(self):
        self.fixture()
        missing=Path(self.temp.name)/'missing.db'
        with self.assertRaises(FileNotFoundError): exporter.export_evidence(missing,1,self.destination)
        self.assertFalse(missing.exists())
        with self.assertRaises(ValueError): exporter.export_evidence(self.path,2,self.destination)
        for bad in (True,0,-1,'1'):
            with self.assertRaises(ValueError): exporter.export_evidence(self.path,bad,self.destination)
        with self.box.connect() as db: db.execute('DROP TABLE write_resolutions')
        with self.assertRaises(sqlite3.Error): exporter.export_evidence(self.path,1,self.destination)
        self.assertFalse(self.destination.exists())
        self.assertEqual(list(Path(self.temp.name).glob('.usage-evidence-*')),[])
        with self.box.connect() as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='write_resolutions'").fetchone())

    def test_fsync_or_publication_failure_leaves_no_partial_archive(self):
        self.fixture()
        for operation in ('fsync','link'):
            with self.subTest(operation=operation),mock.patch.object(exporter.os,operation,side_effect=OSError('fixture')):
                with self.assertRaises(OSError): exporter.export_evidence(self.path,1,self.destination)
            self.assertFalse(self.destination.exists())
            self.assertEqual(list(Path(self.temp.name).glob('.usage-evidence-*')),[])
            self.assertEqual(len(self.box.uncertain_writes()),1)

    def test_wal_concurrent_resolution_does_not_mix_read_snapshots(self):
        self.fixture()
        with self.box.connect() as db: self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0],'wal')
        original=exporter.fingerprint; calls=[]
        def resolve(credential):
            calls.append(credential)
            if len(calls)==1:
                # The SELECT cursor is active. WAL permits the writer to
                # resolve while the exporter retains its original read view.
                self.box.capture(self.session,self.snapshot(dict(self.item,upload_bytes=9)))
            return original(credential)
        with mock.patch.object(exporter,'fingerprint',side_effect=resolve):
            result=exporter.export_evidence(self.path,1,self.destination)
        self.assertEqual((result['unresolved'],result['resolutions']),(1,0))
        self.assertEqual(self.box.uncertain_writes(),[])
        self.assertEqual(len(self.box.write_resolution_history()),1)

    def test_cli_escaped_database_path_and_safe_failure(self):
        self.fixture()
        special=Path(self.temp.name)/'证据?#.sqlite'; self.path.rename(special)
        command=[sys.executable,str(Path(exporter.__file__)),
                 '--database',str(special),'--server-id','1','--output',str(self.destination)]
        result=subprocess.run(command,capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout)['unresolved'],1)
        result=subprocess.run(command,capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,1)
        self.assertNotIn('Traceback',result.stderr)
        self.assertNotIn(self.item['credential'],result.stdout+result.stderr)
