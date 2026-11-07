"""Authenticated drain lookup is bounded and cannot manufacture final evidence."""
import threading
import unittest
import uuid
from unittest.mock import patch
from http.server import ThreadingHTTPServer
from agent import request
from core import Error,Registry
from server import Handler
from usage_migration import UsageMigration
import test_drain_checkpoints as base

class DrainDiscoveryTests(unittest.TestCase):
    setUp=base.DrainCheckpointTests.setUp
    tearDown=base.DrainCheckpointTests.tearDown
    fixtures=base.DrainCheckpointTests.fixtures
    fixture=base.DrainCheckpointTests.fixture
    setup_total=base.DrainCheckpointTests.setup_total
    applied=base.DrainCheckpointTests.applied
    data=base.DrainCheckpointTests.data
    report=base.DrainCheckpointTests.report
    setup_drain=base.DrainCheckpointTests.setup_drain
    envelope=base.DrainCheckpointTests.envelope
    encode=staticmethod(base.DrainCheckpointTests.encode)
    accept=base.DrainCheckpointTests.accept

    def query(self):
        return self.migration.discover(self.token,dict(sessions=[self.session],after=None))

    def test_only_captured_original_agent_and_no_evidence_side_effects(self):
        r=self.setup_drain()
        reply=self.query()
        self.assertEqual(reply['drains'],[dict(cycle_id=self.period,session_id=self.session)])
        self.assertIsNone(reply['next_after']); self.assertFalse(reply['ready_for_execution'])
        other=r.save('servers',dict(name='other'))['id']; token=r.provision_agent(other)['token']
        self.assertEqual(self.migration.discover(token,dict(sessions=[self.session],after=None))['drains'],[])
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_drain_checkpoints').fetchone()[0],0)
            db.execute('DELETE FROM usage_drain_inventory_captures')
        self.assertEqual(self.query()['drains'],[])

    def test_expiry_only_allows_already_accepted_recovery_and_rotation_revokes(self):
        r=self.setup_drain()
        with patch('usage_migration.time.time',return_value=self.now+10000):
            self.assertEqual(self.query()['drains'],[])
        self.accept()
        with patch('usage_migration.time.time',return_value=self.now+10000):
            self.assertEqual(len(self.query()['drains']),1)
        self.token=r.provision_agent(self.sid)['token']
        self.assertEqual(self.query()['drains'],[])
        with self.assertRaises(Error): self.migration.discover('bad',dict(sessions=[self.session],after=None))

    def test_every_original_identity_must_remain_authorized_and_in_window(self):
        r=self.setup_drain()
        with r.connect() as db:
            db.execute('''INSERT INTO usage_drain_inventory SELECT cycle_id,server_id,session_id,
                node_id+100,user_id,credential,'revoked',issued_at,settle_until,upload_bytes,download_bytes
                FROM usage_drain_inventory''')
        self.assertEqual(self.query()['drains'],[])
        with r.connect() as db:
            db.execute('UPDATE usage_drain_inventory SET token_hash=(SELECT token_hash FROM agents WHERE server_id=?)',(self.sid,))
            db.execute('UPDATE usage_drain_inventory SET issued_at=? WHERE node_id>100',(self.now+10000,))
        self.assertEqual(self.query()['drains'],[])

    def test_bounded_keyset_pagination_reopen_without_duplicates(self):
        r=self.setup_drain()
        with r.connect() as db:
            for n in range(70):
                cycle='extra-%03d'%n
                db.execute('INSERT INTO usage_cycles SELECT ?,user_id,starts_at,ends_at,expired FROM usage_cycles WHERE id=?',(cycle,self.period))
                db.execute('INSERT INTO usage_cycle_drains VALUES(?,?)',(cycle,self.now))
                db.execute('INSERT INTO usage_drain_inventory_captures VALUES(?)',(cycle,))
                db.execute('''INSERT INTO usage_drain_inventory SELECT ?,server_id,session_id,node_id,user_id,
                    credential,token_hash,issued_at,settle_until,upload_bytes,download_bytes
                    FROM usage_drain_inventory WHERE cycle_id=?''',(cycle,self.period))
        first=self.query(); self.assertEqual(len(first['drains']),64)
        self.migration=UsageMigration(Registry(r.path))
        second=self.migration.discover(self.token,dict(sessions=[self.session],after=first['next_after']))
        all_rows=first['drains']+second['drains']
        self.assertEqual(len(all_rows),71); self.assertIsNone(second['next_after'])
        self.assertEqual(len({(x['cycle_id'],x['session_id']) for x in all_rows}),71)

    def test_invalid_bounded_inputs_rejected_without_changing_inventory(self):
        self.setup_drain()
        bad=[{},dict(sessions=[],after=None),dict(sessions=[self.session]*2,after=None),
             dict(sessions=[str(uuid.uuid4()) for _ in range(65)],after=None),
             dict(sessions=['bad'],after=None),dict(sessions=[self.session],after=['x','bad']),
             dict(sessions=[{}],after=None),dict(sessions=[self.session],after=['x'*201,self.session])]
        for data in bad:
            with self.subTest(data=data),self.assertRaises(Error): self.migration.discover(self.token,data)
        self.assertEqual(len(self.query()['drains']),1)

    def test_real_http_route_authentication_and_strict_body(self):
        r=self.setup_drain(); server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        server.registry=r; thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            url='http://127.0.0.1:%s'%server.server_port
            data=dict(sessions=[self.session],after=None)
            self.assertEqual(request(url,self.token,'/agent/usage/drains',data),self.query())
            import urllib.error
            for token,body,status in [('bad',data,401),(self.token,dict(data,extra=True),400)]:
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    request(url,token,'/agent/usage/drains',body)
                self.assertEqual(caught.exception.code,status)
        finally:
            server.shutdown(); thread.join(); server.server_close()

if __name__=='__main__': unittest.main()
