"""Subsequent multi-Agent allocation is atomic, durable and non-executable."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import time
import unittest
import uuid
from unittest.mock import patch

from core import Error,Registry
from first_grant import FirstGrant
from multi_agent_allocation import MultiAgentAllocations,outstanding
from total_allocation import held
import test_first_grant as first_base
import test_total_allocation as allocation_base


class MultiAgentAllocationTests(unittest.TestCase):
    setUp=first_base.FirstGrantTests.setUp
    tearDown=first_base.FirstGrantTests.tearDown
    fixtures=first_base.FirstGrantTests.fixtures
    fixture=first_base.FirstGrantTests.fixture
    setup_total=first_base.FirstGrantTests.setup_total
    setup_drain=first_base.FirstGrantTests.setup_drain
    applied=first_base.FirstGrantTests.applied
    report=first_base.FirstGrantTests.report
    data=first_base.FirstGrantTests.data
    encode=staticmethod(first_base.FirstGrantTests.encode)
    envelope=first_base.FirstGrantTests.envelope
    accept=first_base.FirstGrantTests.accept
    sealed=first_base.FirstGrantTests.sealed
    first_request=first_base.FirstGrantTests.request
    second_host=allocation_base.AllocationTests.second_host

    def start(self,total=20,first_bytes=2):
        r=self.sealed()
        self.cycles.set_total_budget(self.period,total)
        self.first=FirstGrant(r).prepare(
            self.token,self.first_request(first_bytes))
        self.assertFalse(self.first['ready_for_execution'])
        return r

    def host(self,r,name=None):
        token,body=self.second_host(r)
        r.heartbeat(token,dict(hostname=name or str(uuid.uuid4()),uptime=1,
                               session_id=body['session_id']))
        item=body['items'][0]
        return token,dict(session_id=body['session_id'],node_id=item['node_id'],
                          credential=item['credential'])

    def request(self,host,count=10,**changes):
        value=dict(request_id=str(uuid.uuid4()),
                   first_grant_id=self.first['grant_id'],
                   cycle_id=self.period,policy_id=self.first['policy_id'],
                   seal_digest=self.first['seal_digest'],
                   session_id=host['session_id'],node_id=host['node_id'],
                   user_id=self.uid,credential=host['credential'],
                   requested_bytes=count)
        return dict(value,**changes)

    def balances(self,r):
        with r.connect() as db:
            db.execute('BEGIN')
            return held(db,self.period),outstanding(db,self.period)

    def test_two_different_agents_concurrently_share_one_atomic_balance(self):
        r=self.start(); token_a,host_a=self.host(r,'a'); token_b,host_b=self.host(r,'b')
        requests=[(token_a,self.request(host_a)),(token_b,self.request(host_b))]
        def allocate(args):
            return MultiAgentAllocations(Registry(r.path)).allocate(*args)
        with ThreadPoolExecutor(2) as pool:
            rows=list(pool.map(allocate,requests))
        self.assertEqual(sorted(row['reserved_bytes'] for row in rows),[1,10])
        self.assertEqual(len({row['server_id'] for row in rows}),2)
        self.assertTrue(all(row['baseline_bytes']==7 for row in rows))
        self.assertTrue(all(row['seal_digest']==self.first['seal_digest'] for row in rows))
        self.assertTrue(all(row['allocation_state']=='held_not_delivered'
                            and row['ready_for_execution'] is False for row in rows))
        self.assertEqual(self.balances(r),(13,11)) # first hold 2 + later holds 11
        with self.assertRaises(Error):
            MultiAgentAllocations(r).allocate(token_a,self.request(host_a,1))

    def test_confirmed_first_and_legacy_holds_all_reduce_new_allocation(self):
        r=self.start(); token,host=self.host(r)
        now=time.time()
        with r.connect() as db:
            db.execute('''INSERT INTO usage_total_allocations
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (str(uuid.uuid4()),str(uuid.uuid4()),self.period,
                 self.first['policy_id'],self.first['server_id'],self.session,
                 'retained-old-token-fingerprint',self.nid,self.uid,
                 self.item['credential'],3,3,now,now+1))
        row=MultiAgentAllocations(r).allocate(token,self.request(host,10))
        # policy 20 - confirmed 7 - first hold 2 - retained legacy hold 3
        self.assertEqual(row['reserved_bytes'],8)
        self.assertEqual(self.balances(r),(13,8))

    def test_exact_retry_is_immutable_but_conflict_and_cross_agent_replay_fail(self):
        r=self.start(); token_a,host_a=self.host(r,'a'); token_b,host_b=self.host(r,'b')
        data=self.request(host_a,4); api=MultiAgentAllocations(r)
        first=api.allocate(token_a,data)
        self.assertEqual(api.allocate(token_a,data),first)
        with self.assertRaises(Error) as conflict:
            api.allocate(token_a,dict(data,requested_bytes=5))
        self.assertEqual(conflict.exception.status,409)
        with self.assertRaises(Error):
            api.allocate(token_b,dict(data,session_id=host_b['session_id'],
                                      node_id=host_b['node_id'],
                                      credential=host_b['credential']))
        self.assertEqual(self.balances(r),(6,4))

    def test_policy_cycle_seal_and_resource_identity_are_exact(self):
        r=self.start(); token,host=self.host(r); api=MultiAgentAllocations(r)
        bad=[
            dict(policy_id=self.first['policy_id']+999),
            dict(seal_digest='0'*64),
            dict(first_grant_id=str(uuid.uuid4())),
            dict(cycle_id=str(uuid.uuid4())),
            dict(node_id=999),dict(user_id=999),
            dict(credential=str(uuid.uuid4())),dict(requested_bytes=True),
        ]
        for change in bad:
            with self.subTest(change=change),self.assertRaises(Error):
                api.allocate(token,self.request(host,**change))
        self.assertEqual(self.balances(r),(2,0))

    def test_offline_retired_rotated_revoked_and_deleted_identity_never_takes_over(self):
        r=self.start(); token,host=self.host(r); data=self.request(host,4)
        row=MultiAgentAllocations(r).allocate(token,data)
        with r.connect() as db:
            last_seen=db.execute('SELECT last_seen FROM agents WHERE server_id=?',
                                 (row['server_id'],)).fetchone()[0]
        with patch('multi_agent_allocation.time.time',return_value=last_seen+45):
            with self.assertRaises(Error): MultiAgentAllocations(r).allocate(token,data)
        r.begin_session(token,dict(session_id=str(uuid.uuid4())))
        with self.assertRaises(Error): MultiAgentAllocations(r).allocate(token,data)
        new_token=r.provision_agent(row['server_id'])['token']
        with self.assertRaises(Error): MultiAgentAllocations(r).allocate(token,data)
        with self.assertRaises(Error): MultiAgentAllocations(r).allocate(new_token,data)
        r.revoke_agent(row['server_id'])
        with self.assertRaises(Error): MultiAgentAllocations(r).allocate(new_token,data)
        r.delete('nodes',host['node_id']); r.delete('servers',row['server_id'])
        self.assertEqual(self.balances(r),(6,4))

    def test_binding_revocation_policy_lowering_and_cycle_end_do_not_refund(self):
        r=self.start(); token,host=self.host(r); data=self.request(host,4)
        MultiAgentAllocations(r).allocate(token,data)
        r.authorize_node_user(host['node_id'],self.uid,revoke=True)
        with self.assertRaises(Error): MultiAgentAllocations(r).allocate(token,data)
        self.cycles.set_total_budget(self.period,10)
        with self.assertRaises(Error): MultiAgentAllocations(r).allocate(token,data)
        with patch('multi_agent_allocation.time.time',return_value=self.now+101):
            with self.assertRaises(Error): MultiAgentAllocations(r).allocate(token,data)
        self.assertEqual(self.balances(r),(6,4))

    def test_restart_preserves_exact_result_and_expiry_never_releases_hold(self):
        r=self.start(); token,host=self.host(r); data=self.request(host,4)
        first=MultiAgentAllocations(r).allocate(token,data)
        reopened=Registry(r.path)
        self.assertEqual(MultiAgentAllocations(reopened).allocate(token,data),first)
        with patch('multi_agent_allocation.time.time',return_value=first['valid_until']):
            with self.assertRaises(Error):
                MultiAgentAllocations(reopened).allocate(token,data)
        self.assertEqual(self.balances(reopened),(6,4))

    def test_database_failure_rolls_back_policy_baseline_and_allocation(self):
        r=self.start(); token,host=self.host(r)
        with r.connect() as db:
            before_policy=db.execute(
                'SELECT count(*) FROM usage_total_policy_versions').fetchone()[0]
            before_total=db.execute(
                'SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0]
            db.execute('''CREATE TRIGGER fail_multi BEFORE INSERT ON usage_multi_agent_allocations
                BEGIN SELECT RAISE(ABORT,'simulated allocation failure'); END''')
        with self.assertRaises(sqlite3.IntegrityError):
            MultiAgentAllocations(r).allocate(token,self.request(host,4))
        with r.connect() as db:
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_multi_agent_allocations').fetchone()[0],0)
            self.assertEqual(db.execute(
                'SELECT count(*) FROM usage_total_policy_versions').fetchone()[0],before_policy)
            self.assertEqual(db.execute(
                'SELECT total_bytes FROM usage_cycle_confirmed_totals').fetchone()[0],before_total)
            db.execute('DROP TRIGGER fail_multi')
        self.assertEqual(MultiAgentAllocations(r).allocate(
            token,self.request(host,4))['reserved_bytes'],4)


if __name__=='__main__':
    unittest.main()
