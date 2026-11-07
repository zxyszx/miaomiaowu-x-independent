"""I1 historical evidence: real persistent fixtures, no runtime permission."""
import copy
import sqlite3
import unittest
from contextlib import ExitStack
from unittest.mock import patch
from core import Error
from multi_execution_evidence import _read_multi_execution
from multi_agent_meter import execution_chain
from usage_accounting import MULTI,require_source
import test_multi_agent_meter as meter
import test_multi_agent_recovery as recovery


def snapshot(db):
    tables=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
    return [(t,sorted([tuple(r) for r in db.execute('SELECT * FROM "'+t.replace('"','""')+'"')],key=repr)) for t in tables]


class EvidenceTests(unittest.TestCase):
    setUp=meter.MultiMeterTests.setUp
    tearDown=meter.MultiMeterTests.tearDown
    prepare=meter.MultiMeterTests.prepare
    data=meter.MultiMeterTests.data
    send=meter.MultiMeterTests.send

    @property
    def ident(self):return self.a[1]['ledger_id']

    def check(self,db,*,ident=None,kind=MULTI,error=False,expected=None):
        before=snapshot(db);changes=db.total_changes
        def no_write(action,*args):
            if action in (sqlite3.SQLITE_INSERT,sqlite3.SQLITE_UPDATE,sqlite3.SQLITE_DELETE,sqlite3.SQLITE_CREATE_TABLE,sqlite3.SQLITE_DROP_TABLE,sqlite3.SQLITE_ALTER_TABLE):return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK
        db.set_authorizer(no_write)
        try:
            with ExitStack() as stack:
                for target in ('multi_agent_configuration.MultiAgentConfiguration._bind','multi_agent_test_runtime.MultiAgentTestHarness.install','core.Registry.begin_session','multi_agent_lifecycle.gated','multi_agent_meter.live_identity'):
                    stack.enter_context(patch(target,side_effect=AssertionError('reader side effect')))
                if error:
                    with self.assertRaises(Error):_read_multi_execution(db,kind,ident or self.ident,expected_allocation=expected)
                    result=None
                else:result=_read_multi_execution(db,kind,ident or self.ident,expected_allocation=expected)
        finally:db.set_authorizer(None)
        self.assertEqual(db.total_changes,changes);self.assertEqual(snapshot(db),before)
        return result

    def test_v1_complete_private_historical_chain(self):
        with self.r.connect() as db:
            db.execute('BEGIN');value,fact,state=self.check(db)
            self.assertEqual((value,fact,state),({k:v for k,v in self.a[3].items() if k not in ('delivery_state','ready_for_execution')},self.a[2],'execution_recorded'))
            self.assertNotIn('tail_authority',value);self.assertNotIn('current',value)
            self.assertFalse(value.get('ready_for_execution',False))

    def test_requires_existing_transaction(self):
        with self.r.connect() as db:
            before=snapshot(db)
            with self.assertRaises(RuntimeError):_read_multi_execution(db,MULTI,self.ident)
            self.assertEqual(before,snapshot(db))

    def test_wrong_kind_unknown_and_first_identity(self):
        with self.r.connect() as db:
            db.execute('BEGIN')
            for kind in ('first_grant','legacy',True,None):self.check(db,kind=kind,error=True)
            self.check(db,ident='unknown-evidence',error=True)
            self.check(db,ident=self.f.first['grant_id'],error=True)

    def test_allocation_argument_not_authority(self):
        with self.r.connect() as db:
            db.execute('BEGIN');a=dict(db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(self.ident,)).fetchone())
            for key in a:
                changed=copy.deepcopy(a);changed[key]=None
                with self.subTest(key=key):self.check(db,expected=changed,error=True)
            self.assertEqual(execution_chain(db,a),self.check(db,expected=a))

    def test_missing_chain_rows(self):
        with self.r.connect() as db:
            db.execute('PRAGMA foreign_keys=OFF');db.execute('BEGIN')
            for t in ('usage_multi_agent_configurations','usage_multi_configuration_refs','usage_multi_execution','usage_multi_lifecycle','usage_multi_installations','usage_multi_applied'):
                with self.subTest(table=t):
                    db.execute('SAVEPOINT damage');db.execute('DELETE FROM '+t+' WHERE allocation_id=?',(self.ident,));self.check(db,error=True)
                    db.execute('ROLLBACK TO damage');db.execute('RELEASE damage')

    def test_binding_installation_execution_corruption(self):
        mutations=("UPDATE usage_multi_agent_configurations SET digest='bad'","UPDATE usage_multi_agent_configurations SET payload='{}'","UPDATE usage_multi_configuration_refs SET token_hash='bad'","UPDATE usage_multi_configuration_refs SET digest='bad'","UPDATE usage_multi_applied SET revision=revision+1","UPDATE usage_multi_applied SET execute_until=execute_until+1","UPDATE usage_multi_installations SET fact='{}'","UPDATE usage_multi_execution SET fact='{}'","UPDATE usage_multi_lifecycle SET state='installed'","UPDATE usage_multi_lifecycle SET state='closed'")
        with self.r.connect() as db:
            db.execute('BEGIN')
            for sql in mutations:
                with self.subTest(sql=sql):
                    db.execute('SAVEPOINT damage');db.execute(sql);self.check(db,error=True)
                    db.execute('ROLLBACK TO damage');db.execute('RELEASE damage')

    def test_allocation_corruption(self):
        fields={'session_id':'wrong','token_hash':'wrong','credential':'wrong','reserved_bytes':9,'baseline_bytes':999,'valid_until':1,'issued_at':1,'seal_digest':'wrong','first_grant_id':'wrong'}
        with self.r.connect() as db:
            db.execute('PRAGMA foreign_keys=OFF');db.execute('BEGIN')
            for key,value in fields.items():
                with self.subTest(key=key):
                    db.execute('SAVEPOINT damage');db.execute('UPDATE usage_multi_agent_allocations SET '+key+'=? WHERE allocation_id=?',(value,self.ident));self.check(db,error=True)
                    db.execute('ROLLBACK TO damage');db.execute('RELEASE damage')

    def test_dual_ledger_collision(self):
        with self.r.connect() as db:
            db.execute('PRAGMA foreign_keys=OFF');db.execute('BEGIN')
            db.execute('UPDATE usage_first_total_grants SET grant_id=?',(self.ident,));self.check(db,error=True)

    def test_meter_does_not_rebind_baseline(self):
        self.send()
        with self.r.connect() as db:
            db.execute('BEGIN');self.assertEqual(self.check(db)[0]['baseline_bytes'],self.a[3]['baseline_bytes'])
            a=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(self.ident,)).fetchone();require_source(db,MULTI,a)

    def test_source_integrity_remains_with_caller(self):
        self.send()
        with self.r.connect() as db:
            db.execute('BEGIN');db.execute("UPDATE usage_cycle_confirmed_totals SET total_bytes='999'");self.check(db)
            a=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(self.ident,)).fetchone();before=snapshot(db)
            with self.assertRaises(Error):require_source(db,MULTI,a)
            self.assertEqual(before,snapshot(db))
        with self.assertRaises(Error):self.send()

    def test_closing_blocked_historical_states(self):
        with self.r.connect() as db:
            db.execute('BEGIN')
            for state in ('closing','blocked'):
                db.execute('UPDATE usage_multi_lifecycle SET state=? WHERE allocation_id=?',(state,self.ident));self.assertEqual(self.check(db)[2],state)

    def test_second_connection_consistent_read(self):
        with self.r.connect() as db:
            db.execute('PRAGMA journal_mode=WAL');db.execute('BEGIN');expected=self.check(db)
            with self.r.connect() as second:
                second.execute('BEGIN');self.assertEqual(self.check(second),expected)
                second.execute("UPDATE usage_multi_lifecycle SET state='blocked' WHERE allocation_id=?",(self.ident,))
            self.assertEqual(self.check(db),expected)
        with self.r.connect() as fresh:
            fresh.execute('BEGIN');self.assertEqual(self.check(fresh)[2],'blocked')


@unittest.skipUnless(recovery.base.base.BINARY.is_file(),'real Xray required')
class TailEvidenceTests(unittest.TestCase):
    setUp=recovery.RecoveryTests.setUp
    tearDown=recovery.RecoveryTests.tearDown
    success=recovery.RecoveryTests.success
    frozen=recovery.RecoveryTests.frozen
    setup_recovery=recovery.RecoveryTests.setup_recovery
    proposal=recovery.RecoveryTests.proposal

    def read(self):
        with self.r.connect() as db:
            db.execute('BEGIN');before=snapshot(db);changes=db.total_changes
            result=_read_multi_execution(db,MULTI,self.a['allocation_id'])
            a=db.execute('SELECT * FROM usage_multi_agent_allocations WHERE allocation_id=?',(self.a['allocation_id'],)).fetchone();require_source(db,MULTI,a)
            self.assertEqual(before,snapshot(db));self.assertEqual(changes,db.total_changes);return result

    def test_tail_authority_and_ordinary_history(self):
        self.read();self.success();self.local.send_ordinary(self.token)
        self.assertEqual(self.read()[0]['tail_authority']['body_digest'],self.auth['body_digest'])

    def test_final_and_closed_chain(self):
        self.success();self.frozen();self.local.deliver(self.token);self.read()
        self.local.finish(self.token);self.assertEqual(self.read()[2],'closed')

    def test_manual_history_no_recursion(self):
        self.setup_recovery();self.recovery.commit(self.actor['token'],self.proposal());self.read()

    def test_missing_tail_authority_no_v1_downgrade(self):
        with self.r.connect() as db:
            db.execute('PRAGMA foreign_keys=OFF');db.execute('BEGIN');db.execute('DELETE FROM usage_multi_tail_authorities');before=snapshot(db)
            with self.assertRaises(Error):_read_multi_execution(db,MULTI,self.a['allocation_id'])
            self.assertEqual(before,snapshot(db))
