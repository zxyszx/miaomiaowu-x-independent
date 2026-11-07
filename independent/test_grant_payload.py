"""Local payload enforcement is not a production applied receipt."""
import asyncio
import copy
import socket
import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from allocation_quota import AllocationQuota
from grant_payload import GrantPayloadQuota
from payload_meter import PayloadMeter,MeteredWriter
import test_grant_agent as staging


class GrantPayloadTests(unittest.TestCase):
    setUp=staging.GrantAgentTests.setUp
    sign=staging.GrantAgentTests.sign
    def quota(self,row=None):
        row=row or self.row
        self.store.stage(row,staging.GrantAgentTests.identity(self,row))
        return GrantPayloadQuota(self.path.with_name('payload.db'),self.store,row)

    def limited(self,upload=4,download=3,total=6):
        row=copy.deepcopy(self.row)
        row['reserved_bytes']=total
        row['baseline_bytes']=5
        row['client'].update(cycle_id=row['cycle_id'],
            cycle_budget={'upload_bytes':upload+2,'download_bytes':download+3},
            cycle_usage={'upload_bytes':2,'download_bytes':3})
        return self.sign(row)

    def test_direction_and_bidirectional_intersection_survives_reopen(self):
        row=self.limited(); q=self.quota(row)
        self.assertEqual(q.reserve(0,100),4)
        self.assertEqual(self.quota(row).reserve(1,100),2)
        self.assertEqual(q.reserve(0,1),0)
        self.assertEqual(q.snapshot()['remaining_bytes'],0)

    def test_constraints_cannot_be_removed_or_rebound(self):
        q=self.quota(self.limited())
        with self.assertRaises(ValueError): AllocationQuota(q.path,q._scope)
        other=copy.deepcopy(self.limited()); other['client']['cycle_budget']['upload_bytes']+=1
        self.sign(other)
        with self.assertRaises(ValueError): self.quota(other)
        self.assertEqual(q.snapshot()['remaining_bytes'],6)

    def test_concurrent_opposite_directions_share_one_balance(self):
        row=self.limited(20,20,7); self.quota(row)
        with ThreadPoolExecutor(2) as pool:
            results=list(pool.map(lambda d:self.quota(row).reserve(d,7),(0,1)))
        self.assertEqual(sum(results),7)

    def test_socket_success_and_unknown_intent_stay_separate(self):
        row=self.limited(); q=self.quota(row)
        left,right=socket.socketpair(); self.addCleanup(left.close); self.addCleanup(right.close)
        class Destination:
            def write(self,data): left.sendall(data)
        meter=PayloadMeter(); meter.bind(row['server_id'])
        key=(row['node_id'],row['client']['user_id'],row['client']['uuid'])
        writer=MeteredWriter(Destination(),meter,key,0,q)
        asyncio.run(writer.awrite(b'abcd'))
        self.assertEqual(right.recv(4),b'abcd')
        allowed,ticket=q.reserve_write(1,2); self.assertEqual(allowed,2)
        reopened=self.quota(row)
        with self.assertRaises(ValueError): reopened.complete_write(ticket)
        evidence=reopened.snapshot()
        self.assertEqual(evidence['successful_upload_bytes'],4)
        self.assertEqual(evidence['unknown_download_bytes'],2)
        self.assertFalse(evidence['ready_for_execution'])

    def test_window_and_exhausted_direction_fail_closed(self):
        row=self.limited(0,8,6); q=self.quota(row)
        self.assertEqual(q.reserve(0,1),0)
        for now in (row['issued_at']-1,row['valid_until']):
            with patch('allocation_quota.time.time',return_value=now):
                self.assertEqual(q.reserve(1,1),0)
        self.assertEqual(q.snapshot()['remaining_bytes'],6)

    def test_constraint_insert_failure_rolls_back_all_budget_rows(self):
        q=self.quota(self.limited())
        with sqlite3.connect(q.path) as db:
            db.execute('DELETE FROM allocation_constraints'); db.execute('DELETE FROM allocation_quota')
            db.execute("CREATE TRIGGER fail BEFORE INSERT ON allocation_constraints BEGIN SELECT RAISE(ABORT,'disk'); END")
        with self.assertRaises(OSError): self.quota(self.limited())
        with sqlite3.connect(q.path) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM allocation_quota').fetchone()[0],0)

    def test_missing_nonzero_baseline_is_not_assumed_zero(self):
        row=self.limited(); del row['client']['cycle_usage']; self.sign(row)
        with self.assertRaises(ValueError): self.quota(row)
        self.assertFalse(self.path.with_name('payload.db').exists())

    def test_large_direction_baseline_is_subtracted_exactly(self):
        row=self.limited(); row['baseline_bytes']=2**70
        row['client'].update(cycle_usage={'upload_bytes':2**70,'download_bytes':0},
                             cycle_budget={'upload_bytes':2**70+3,'download_bytes':2**70})
        self.sign(row); q=self.quota(row)
        self.assertEqual(q.reserve(0,100),3)
        self.assertEqual(q.reserve(1,100),3)
