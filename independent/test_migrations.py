"""Isolated failure, retry and concurrent startup checks for DB migrations."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from core import Registry
from test_xray import VLESS


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.path=Path(self.temp.name)/'registry.db'
        self.registry=Registry(self.path)
        self.servers=[]
        user=self.registry.save('users',{'name':'migration user'})['id']
        for index in range(2):
            sid=self.registry.save('servers',{'name':f'server-{index}'})['id']
            nid=self.registry.save('nodes',{'name':'node','server_id':sid})['id']
            self.registry.configure_node(nid,{**VLESS,'listen_port':24000+index})
            self.registry.authorize_node_user(nid,user)
            self.servers.append(sid)
        self.registry.temporary_bandwidth(user,{'bandwidth':{'upload_bps':32768,'download_bps':None},'expires_at':time.time()+600})
        with self.registry.connect() as db: db.execute('DROP TABLE schema_migrations')
        self.before=self.revisions()

    def tearDown(self): self.temp.cleanup()

    def revisions(self):
        with self.registry.connect() as db:
            return dict(db.execute('SELECT server_id,revision FROM server_configs').fetchall())

    def test_partial_revision_failure_rolls_back_all_and_can_retry(self):
        original=Registry._bump
        calls=[]
        def interrupt(registry,db,sid):
            original(registry,db,sid)
            calls.append(sid)
            if len(calls)==2: raise OSError('injected interrupted migration')
        with mock.patch.object(Registry,'_bump',interrupt):
            with self.assertRaises(OSError): Registry(self.path)
        self.assertEqual(self.revisions(),self.before)
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM schema_migrations').fetchone()[0],0)
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0],'ok')
        Registry(self.path)
        self.assertEqual(self.revisions(),{sid:rev+1 for sid,rev in self.before.items()})

    def test_process_death_mid_migration_recovers_sqlite_and_can_retry(self):
        script="""
import os,sys
from core import Registry
original=Registry._bump
calls=[]
def interrupted(registry,db,sid):
    original(registry,db,sid)
    calls.append(sid)
    if len(calls)==2: os._exit(91)
Registry._bump=interrupted
Registry(sys.argv[1])
"""
        result=subprocess.run([sys.executable,'-c',script,str(self.path)],
                              cwd=Path(__file__).resolve().parent,timeout=15,
                              capture_output=True,text=True)
        self.assertEqual(result.returncode,91,result.stderr)
        self.assertEqual(self.revisions(),self.before)
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM schema_migrations').fetchone()[0],0)
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchone()[0],'ok')
        Registry(self.path)
        self.assertEqual(self.revisions(),{sid:rev+1 for sid,rev in self.before.items()})

    def test_concurrent_startups_commit_migration_once(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda _: Registry(self.path),range(12)))
        self.assertEqual(self.revisions(),{sid:rev+1 for sid,rev in self.before.items()})
        with self.registry.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM schema_migrations WHERE name=?',
                                        ('per-user-temporary-deadline-v1',)).fetchone()[0],1)

    def test_migration_marker_write_failure_rolls_back_revisions(self):
        with self.registry.connect() as db:
            db.execute('CREATE TABLE schema_migrations(name TEXT PRIMARY KEY)')
            db.execute("CREATE TRIGGER fail_marker BEFORE INSERT ON schema_migrations BEGIN SELECT RAISE(ABORT,'injected marker failure'); END")
        with self.assertRaises(sqlite3.IntegrityError): Registry(self.path)
        self.assertEqual(self.revisions(),self.before)
        with self.registry.connect() as db: db.execute('DROP TRIGGER fail_marker')
        Registry(self.path)
        self.assertEqual(self.revisions(),{sid:rev+1 for sid,rev in self.before.items()})
