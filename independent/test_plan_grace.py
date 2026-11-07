import concurrent.futures
import sqlite3
import unittest
import time
from types import SimpleNamespace
from unittest.mock import patch
from core import Registry,Error
from plan_grace import GracePolicies
from test_accounts import plan
import test_core


class GracePolicyTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown

    def fixtures(self):
        r=self.registry
        return r,GracePolicies(r),plan(r,'original'),plan(r,'downgrade')

    def test_persist_replace_clear_and_old_database_upgrade(self):
        r,m,a,b=self.fixtures()
        self.assertIsNone(m.get(a))
        self.assertEqual(m.save(a,dict(target_plan_id=b,days=1)),dict(target_plan_id=b,days=1))
        self.assertEqual(GracePolicies(Registry(r.path)).get(a),m.get(a))
        self.assertEqual(m.save(a,dict(target_plan_id=b,days=365))['days'],365)
        m.delete(a); m.delete(a); self.assertIsNone(m.get(a))
        with r.connect() as db: db.execute('DROP TABLE plan_grace_policies')
        self.assertIsNone(GracePolicies(Registry(r.path)).get(a))

    def test_validation_does_not_replace_valid_policy(self):
        r,m,a,b=self.fixtures(); good=m.save(a,dict(target_plan_id=b,days=7))
        values=[None,[],{},dict(target_plan_id=b,days=7,enabled=True)]
        values += [dict(target_plan_id=b,days=x) for x in (True,0,366,1.5,'1',None)]
        values += [dict(target_plan_id=x,days=7) for x in (True,0,-1,1.5,'1',None,999)]
        for value in values:
            with self.subTest(value=value):
                with self.assertRaises(Error): m.save(a,value)
                self.assertEqual(m.get(a),good)
        for ident in (True,0,-1,'1',999):
            with self.assertRaises(Error): m.get(ident)
            with self.assertRaises(Error): m.delete(ident)

    def test_self_and_indirect_cycle_rejected(self):
        r,m,a,b=self.fixtures(); c=plan(r,'third')
        with self.assertRaises(Error): m.save(a,dict(target_plan_id=a,days=7))
        m.save(a,dict(target_plan_id=b,days=7)); m.save(b,dict(target_plan_id=c,days=7))
        with self.assertRaises(Error) as caught: m.save(c,dict(target_plan_id=a,days=7))
        self.assertEqual(caught.exception.status,409); self.assertIsNone(m.get(c))

    def test_target_delete_blocked_source_delete_cascades(self):
        r,m,a,b=self.fixtures(); m.save(a,dict(target_plan_id=b,days=7))
        with self.assertRaises(Error) as caught: r.delete_plan(b)
        self.assertEqual(caught.exception.status,409)
        with r.connect() as db:
            with self.assertRaises(sqlite3.IntegrityError): db.execute('DELETE FROM plans WHERE id=?',(b,))
        self.assertEqual(m.get(a)['target_plan_id'],b)
        r.delete_plan(a); r.delete_plan(b)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM plan_grace_policies').fetchone()[0],0)
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_concurrent_opposite_edges_cannot_create_cycle(self):
        r,m,a,b=self.fixtures()
        def save(pair):
            try: m.save(pair[0],dict(target_plan_id=pair[1],days=7)); return 'saved'
            except Error as error: return error.status
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            self.assertCountEqual(list(pool.map(save,[(a,b),(b,a)])),['saved',409])

    def test_storage_does_not_change_live_access_or_validity(self):
        # This stage intentionally does not activate incomplete downgrade rules.
        from test_accounts import AccountTests
        from plan_lifecycle import Lifecycle
        import time
        r,uid,nid,token=AccountTests.fixtures(self)
        a,b=plan(r,'original'),plan(r,'downgrade'); r.assign_plan(uid,dict(plan_id=a))
        r.authorize_node_user(nid,uid)
        life=Lifecycle(r); life.manage(uid,dict(enabled=True,expires_at=time.time()+1000))
        before=r.configuration(token); state=life.manage(uid)
        m=GracePolicies(r); m.save(a,dict(target_plan_id=b,days=7))
        self.assertEqual(r.configuration(token),before); self.assertEqual(life.manage(uid),state)
        life.manage(uid,dict(enabled=True,expires_at=time.time()-1))
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])
        self.assertEqual(life.manage(uid)['status'],'expired')
        m.delete(a); self.assertEqual(r.configuration(token)['nodes'][0]['clients'],[])


class GracePreviewTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown

    def fixtures(self):
        from test_accounts import AccountTests
        from plan_lifecycle import Lifecycle
        r,uid,nid,token=AccountTests.fixtures(self)
        a,b=plan(r,'original'),plan(r,'downgrade')
        r.assign_plan(uid,dict(plan_id=a)); auth=r.authorize_node_user(nid,uid)
        end=time.time()+1000; Lifecycle(r).manage(uid,dict(enabled=True,expires_at=end))
        m=GracePolicies(r); m.save(a,dict(target_plan_id=b,days=1))
        return r,m,uid,nid,token,a,b,end,auth

    def at(self,m,uid,now):
        with patch('plan_grace.time',SimpleNamespace(time=lambda:now)): return m.preview(uid)

    def test_exact_boundaries_no_automatic_chained_grace_or_credentials(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        c=plan(r,'third'); m.save(b,dict(target_plan_id=c,days=365))
        for now,state,target in ((end-1,'active',a),(end,'grace',b),
                                  (end+86399,'grace',b),(end+86400,'expired',None)):
            result=self.at(m,uid,now)
            self.assertEqual((result['state'],result['candidate_plan_id']),(state,target))
            self.assertFalse(result['execution_ready'])
            self.assertNotIn(auth['uuid'],str(result))
        self.assertEqual(self.at(m,uid,end)['valid_until'],end+86400)

    def test_target_scope_intersects_existing_authorizations_without_deletion(self):
        from plan_nodes import PlanNodes
        import test_xray
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        with r.connect() as db: sid=db.execute('SELECT server_id FROM nodes WHERE id=?',(nid,)).fetchone()[0]
        other=r.save('nodes',dict(name='target-only',server_id=sid))['id']
        r.configure_node(other,{**test_xray.VLESS,'listen_port':29998})
        PlanNodes(r).save(b,dict(node_ids=[other]))
        before=r.configuration(token)
        self.assertEqual(self.at(m,uid,end)['node_ids'],[])
        self.assertEqual(r.configuration(token),before)
        PlanNodes(r).save(b,dict(node_ids=[nid,other]))
        self.assertEqual(self.at(m,uid,end)['node_ids'],[nid])
        PlanNodes(r).save(b,dict(node_ids=[]))
        self.assertEqual(self.at(m,uid,end)['node_ids'],[])
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT credential FROM node_users WHERE user_id=?',(uid,)).fetchone()[0],auth['uuid'])

    def test_disable_renew_and_expiry_clock_rollback(self):
        from plan_lifecycle import Lifecycle
        r,m,uid,nid,token,a,b,end,auth=self.fixtures(); life=Lifecycle(r)
        life.manage(uid,dict(enabled=False,expires_at=end))
        self.assertEqual(self.at(m,uid,end)['state'],'disabled')
        self.assertEqual(self.at(m,uid,end)['node_ids'],[])
        life.manage(uid,dict(enabled=True,expires_at=end))
        with r.connect() as db: db.execute('UPDATE plan_lifecycle SET active=0 WHERE user_id=?',(uid,))
        self.assertEqual(self.at(m,uid,end-1)['state'],'expired')
        self.assertEqual(self.at(m,uid,end)['state'],'grace')
        life.manage(uid,dict(enabled=True,expires_at=end+200000))
        self.assertEqual(self.at(m,uid,end)['candidate_plan_id'],a)
        self.assertEqual(r.configuration(token)['nodes'][0]['clients'][0]['uuid'],auth['uuid'])

    def test_no_rule_long_term_unbound_and_invalid_id(self):
        from plan_lifecycle import Lifecycle
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        m.delete(a); self.assertEqual(self.at(m,uid,end)['state'],'expired')
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=None))
        self.assertEqual(self.at(m,uid,end+1000000)['candidate_plan_id'],a)
        for ident in (True,0,-1,'1',999):
            with self.assertRaises(Error): m.preview(ident)
        r.assign_plan(uid,dict(plan_id=None))
        with self.assertRaises(Error): m.preview(uid)


class GraceClockTests(unittest.TestCase):
    setUp=test_core.RegistryTests.setUp
    tearDown=test_core.RegistryTests.tearDown
    fixtures=GracePreviewTests.fixtures
    at=GracePreviewTests.at

    def observe(self,r,token,now):
        # Exercise the real configuration/lifecycle transaction, not just SQL.
        with patch('plan_lifecycle.time',SimpleNamespace(time=lambda:now)):
            return r.configuration(token)

    def test_observed_grace_end_survives_restart_rollback_and_policy_edit(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        self.observe(r,token,end+86400)
        m=GracePolicies(Registry(r.path))
        for now in (end-100,end,end+86399):
            self.assertEqual(self.at(m,uid,now)['state'],'expired')
        m.delete(a); m.save(a,dict(target_plan_id=b,days=1))
        self.assertEqual(self.at(m,uid,end)['node_ids'],[])
        self.assertEqual(self.observe(r,token,end)['nodes'][0]['clients'],[])
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT observed_at FROM plan_grace_observations').fetchone()[0],end+86400)
            self.assertEqual(db.execute('SELECT credential FROM node_users WHERE user_id=?',(uid,)).fetchone()[0],auth['uuid'])

    def test_observed_grace_progress_cannot_rewind_and_preview_is_read_only(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        self.at(m,uid,end+86400)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM plan_grace_observations').fetchone()[0],0)
        self.observe(r,token,end+100)
        self.assertEqual(self.at(m,uid,end-1)['state'],'grace')
        self.observe(r,token,end+20)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT observed_at FROM plan_grace_observations').fetchone()[0],end+100)
        self.assertFalse(self.at(m,uid,end-1)['execution_ready'])

    def test_renewal_is_new_contract_and_preserves_credentials(self):
        from plan_lifecycle import Lifecycle
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        self.observe(r,token,end+86400)
        new_end=end+200000
        Lifecycle(r).manage(uid,dict(enabled=True,expires_at=new_end))
        self.assertEqual(self.at(m,uid,end+86400)['candidate_plan_id'],a)
        self.assertEqual(self.observe(r,token,end+86400)['nodes'][0]['clients'][0]['uuid'],auth['uuid'])
        self.observe(r,token,new_end)
        self.assertEqual(self.at(m,uid,new_end-10)['candidate_plan_id'],b)
        with r.connect() as db: self.assertEqual(db.execute('SELECT count(*) FROM plan_grace_observations').fetchone()[0],2)

    def test_observation_and_lifecycle_changes_rollback_together(self):
        from plan_lifecycle import expire
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            with patch('plan_lifecycle.time',SimpleNamespace(time=lambda:end+86400)):
                expire(r,db)
            db.rollback()
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM plan_grace_observations').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT active FROM plan_lifecycle WHERE user_id=?',(uid,)).fetchone()[0],1)

    def test_old_database_migration_and_foreign_key_cleanup(self):
        r,m,uid,nid,token,a,b,end,auth=self.fixtures()
        with r.connect() as db: db.execute('DROP TABLE plan_grace_observations')
        r=Registry(r.path); self.observe(r,token,end+86400)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM plan_grace_observations').fetchone()[0],1)
        r.delete('users',uid)
        with r.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM plan_grace_observations').fetchone()[0],0)
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(),[])
