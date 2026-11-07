"""Formal grant lifecycle orchestration stays distinct from execution readiness."""
import json
import sqlite3
import unittest
import urllib.error
import uuid
from agent import request
from grant_ack import fact_for
from grant_capabilities import INSTALLATION,STAGING
from grant_configuration import GrantConfiguration
from grant_lifecycle import GrantLifecycle
import test_grant_ack as fixture


class GrantLifecycleTests(unittest.TestCase):
    def setup_lifecycle(self, offer=(STAGING, INSTALLATION)):
        self.start_http(offer=list(offer));self.fetch()
        self.value=GrantConfiguration(self.r).bind(self.token,self.g['grant_id'])
        self.fact=fact_for(self.value)
        self.ack()

    def lifecycle(self, action, session=None):
        return request(self.url,self.token,'/agent/grants/lifecycle',dict(
            action=action,grant_id=self.g['grant_id'],
            session_id=self.session if session is None else session))

    def rejected(self, status, fn):
        with self.assertRaises(urllib.error.HTTPError) as caught:
            fn()
        try:self.assertEqual(caught.exception.code,status)
        finally:caught.exception.close()

    def row(self):
        with self.r.connect() as db:
            return db.execute('SELECT * FROM usage_first_total_lifecycle').fetchone()

    def test_begin_install_close_are_idempotent_and_not_execution(self):
        self.setup_lifecycle()
        begin=self.lifecycle('begin');self.assertEqual(begin['state'],'installing')
        self.assertFalse(begin['ready_for_execution']);self.assertEqual(self.lifecycle('begin'),begin)
        installed=self.lifecycle('installed');self.assertEqual(installed['state'],'installed')
        self.assertFalse(installed['ready_for_execution']);self.assertEqual(self.lifecycle('installed'),installed)
        closed=self.lifecycle('close');self.assertEqual(closed['state'],'closed')
        self.assertFalse(closed['ready_for_execution']);self.assertEqual(self.lifecycle('close'),closed)
        self.assertEqual(self.lifecycle('status'),closed)
        self.assertEqual(self.row()['state'],'closed')
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_applied').fetchone()[0],1)

    def test_install_requires_ack_and_begin_requires_installation_capability(self):
        self.start_http(offer=[STAGING]);self.fetch()
        self.rejected(409,lambda:self.lifecycle('begin'))
        self.assertIsNone(self.row())

    def test_installed_requires_begin_and_verified_ack(self):
        self.setup_lifecycle()
        self.rejected(409,lambda:self.lifecycle('installed'))
        self.assertIsNone(self.row())
        self.lifecycle('begin')
        with self.r.connect() as db:
            db.execute('DELETE FROM usage_first_total_installations')
            db.execute('DELETE FROM usage_first_total_applied')
        self.rejected(409,lambda:self.lifecycle('installed'))
        self.assertEqual(self.row()['state'],'installing')

    def test_rotated_session_cannot_progress_or_close(self):
        self.setup_lifecycle();self.lifecycle('begin')
        rotated=str(uuid.uuid4())
        self.r.begin_session(self.token,dict(session_id=rotated))
        self.rejected(403,lambda:self.lifecycle('status',session=rotated))
        self.assertEqual(self.row()['state'],'installing')

    def test_expired_grant_cannot_begin_and_storage_is_unchanged(self):
        self.setup_lifecycle()
        with self.r.connect() as db:db.execute("UPDATE usage_first_total_grants SET valid_until=0")
        self.rejected(409,lambda:self.lifecycle('begin'))
        self.assertIsNone(self.row())

    def test_close_before_installation_does_not_refund_or_close(self):
        self.setup_lifecycle();self.lifecycle('begin')
        self.rejected(409,lambda:self.lifecycle('close'))
        self.assertEqual(self.row()['state'],'installing')
        with self.r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM usage_first_total_applied').fetchone()[0],1)

    def test_storage_failure_rolls_back_transition(self):
        self.setup_lifecycle();self.lifecycle('begin')
        with self.r.connect() as db:
            db.execute("CREATE TRIGGER reject_lifecycle BEFORE UPDATE ON usage_first_total_lifecycle BEGIN SELECT RAISE(ABORT,'private storage detail'); END")
        self.rejected(500,lambda:self.lifecycle('installed'))
        self.assertEqual(self.row()['state'],'installing')


for name in ('setUp','tearDown','fixtures','fixture','setup_total','setup_drain','applied',
             'report','data','envelope','accept','sealed','request','prepare','start',
             'start_http','fetch','post','rejected','ack'):
    setattr(GrantLifecycleTests,name,getattr(fixture.GrantAckTests,name))
GrantLifecycleTests.encode=staticmethod(fixture.GrantAckTests.encode)

