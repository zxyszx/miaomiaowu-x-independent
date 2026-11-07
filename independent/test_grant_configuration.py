"""Binding is immutable and policy checked, never execution evidence."""
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from core import Registry,Error
from first_grant import FirstGrant
from grant_configuration import GrantConfiguration
from total_allocation import held
import test_first_grant as base

class GrantConfigurationTests(unittest.TestCase):
    setUp=base.FirstGrantTests.setUp
    tearDown=base.FirstGrantTests.tearDown
    fixtures=base.FirstGrantTests.fixtures
    fixture=base.FirstGrantTests.fixture
    setup_total=base.FirstGrantTests.setup_total
    setup_drain=base.FirstGrantTests.setup_drain
    applied=base.FirstGrantTests.applied
    report=base.FirstGrantTests.report
    data=base.FirstGrantTests.data
    encode=staticmethod(base.FirstGrantTests.encode)
    envelope=base.FirstGrantTests.envelope
    accept=base.FirstGrantTests.accept
    sealed=base.FirstGrantTests.sealed
    request=base.FirstGrantTests.request
    prepare=base.FirstGrantTests.prepare

    def start(self):
        r=self.sealed(); self.g=self.prepare(r); return r

    def bind(self,r): return GrantConfiguration(r).bind(self.token,self.g['grant_id'])

    def test_immutable_reopen_and_normal_ack_never_applies_grant(self):
        r=self.start(); row=self.bind(r)
        self.assertEqual(row,self.bind(Registry(r.path)))
        self.assertFalse(row['ready_for_execution'])
        self.assertEqual(row['client']['uuid'],self.item['credential'])
        config=r.configuration(self.token)
        ack={k:config[k] for k in ('revision','digest')}; ack['session_id']=self.session
        r.acknowledge_configuration(self.token,ack); r.acknowledge_runtime(self.token,ack)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_applied').fetchone()[0],0)
            db.execute('BEGIN'); self.assertEqual(held(db,self.period),6)
        self.assertEqual(config['nodes'][0]['clients'],[])

    def test_policy_shared_with_ordinary_snapshot_helper(self):
        r=self.start()
        r.user_bandwidth(self.uid,dict(upload_bps=123,download_bps=456))
        r.server_bandwidth(self.g['server_id'],dict(upload_bps=789,download_bps=None))
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            client={'user_id':self.uid,'uuid':self.item['credential']}
            r._client_policy(db,client,self.nid,True)
        client['cycle_usage']={'upload_bytes':4,'download_bytes':3}
        result=self.bind(r)
        self.assertEqual(result['client'],client)
        self.assertEqual(client['bandwidth'],dict(upload_bps=123,download_bps=456))
        self.assertEqual(result['server_bandwidth'],dict(upload_bps=789,download_bps=None))

    def test_known_baseline_preserved_without_directional_budget(self):
        r=self.start(); result=self.bind(r)
        self.assertNotIn('cycle_budget',result['client'])
        self.assertEqual(result['client']['cycle_usage'],dict(upload_bytes=4,download_bytes=3))
        self.assertEqual(sum(result['client']['cycle_usage'].values()),result['baseline_bytes'])

    def test_legacy_missing_baseline_binding_is_not_silently_rewritten(self):
        import json
        from node_config import digest
        r=self.start(); result=self.bind(r)
        for key in ('digest','delivery_state','ready_for_execution'): result.pop(key)
        result['client'].pop('cycle_usage')
        encoded=json.dumps(result,sort_keys=True,separators=(',',':'))
        with r.connect() as db:
            db.execute('UPDATE usage_first_total_configurations SET payload=?,digest=?',
                       (encoded,digest(result)))
        with self.assertRaises(Error): self.bind(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT payload FROM usage_first_total_configurations').fetchone()[0],encoded)

    def test_changed_revision_cannot_rewrite_bound_configuration(self):
        r=self.start(); first=self.bind(r)
        with r.connect() as db: r._bump(db,self.g['server_id'])
        with self.assertRaises(Error): self.bind(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT digest FROM usage_first_total_configurations').fetchone()[0],first['digest'])

    def test_lowered_total_policy_rejected_without_refunding_intent(self):
        r=self.start(); self.cycles.set_total_budget(self.period,0)
        with self.assertRaises(Error): self.bind(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_configurations').fetchone()[0],0)
            db.execute('BEGIN'); self.assertEqual(held(db,self.period),6)

    def test_expired_and_clock_reversed_bindings_rejected_even_on_retry(self):
        r=self.start(); self.bind(r)
        for now in (self.g['issued_at']-1,self.g['valid_until']):
            with self.subTest(now=now),patch('grant_configuration.time.time',return_value=now),self.assertRaises(Error): self.bind(r)

    def test_deleted_binding_or_revoked_token_rejected(self):
        r=self.start()
        with r.connect() as db: db.execute('DELETE FROM node_users WHERE node_id=?',(self.nid,))
        with self.assertRaises(Error): self.bind(r)
        with r.connect() as db: db.execute("UPDATE agents SET token_hash='revoked'")
        with self.assertRaises(Error): self.bind(r)

    def test_insert_failure_atomic_and_concurrent_retry_one_binding(self):
        r=self.start()
        with r.connect() as db:
            db.execute("CREATE TRIGGER bad_binding BEFORE INSERT ON usage_first_total_configurations BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(sqlite3.IntegrityError): self.bind(r)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_configurations').fetchone()[0],0)
            db.execute('DROP TRIGGER bad_binding')
        with ThreadPoolExecutor(2) as pool: results=list(pool.map(lambda _:self.bind(Registry(r.path)),range(2)))
        self.assertEqual(results[0],results[1])
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_configurations').fetchone()[0],1)

    def test_changed_effective_policy_without_revision_bump_is_rejected(self):
        r=self.start(); r.user_bandwidth(self.uid,dict(upload_bps=123,download_bps=456)); self.bind(r)
        with r.connect() as db:
            db.execute("UPDATE user_bandwidth SET payload=? WHERE user_id=?",('{"upload_bps":999,"download_bps":456}',self.uid))
        with self.assertRaises(Error): self.bind(r)

    def test_disabled_lifecycle_or_retired_session_rejected(self):
        from plan_lifecycle import Lifecycle
        import uuid
        r=self.start(); Lifecycle(r).manage(self.uid,dict(enabled=False,expires_at=None))
        with self.assertRaises(Error): self.bind(r)
        Lifecycle(r).manage(self.uid,dict(enabled=True,expires_at=None))
        r.begin_session(self.token,dict(session_id=str(uuid.uuid4())))
        with self.assertRaises(Error): self.bind(r)
