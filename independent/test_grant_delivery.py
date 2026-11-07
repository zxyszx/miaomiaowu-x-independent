"""Frozen successful bytes, retry persistence and separate grant transport."""
import asyncio
import io
import socket
import sqlite3
import unittest
import urllib.error
from grant_delivery import GrantDelivery
from payload_meter import PayloadMeter,MeteredWriter
import test_grant_payload as fixture

class GrantDeliveryTests(unittest.TestCase):
    setUp=fixture.GrantPayloadTests.setUp
    sign=fixture.GrantPayloadTests.sign
    quota=fixture.GrantPayloadTests.quota
    limited=fixture.GrantPayloadTests.limited

    def written(self):
        q=self.quota(self.limited()); left,right=socket.socketpair()
        self.addCleanup(left.close);self.addCleanup(right.close)
        class Destination:
            def write(self,data): left.sendall(data)
        meter=PayloadMeter();meter.bind(q._scope['server_id'])
        key=(q._scope['node_id'],q._scope['user_id'],q._scope['credential'])
        asyncio.run(MeteredWriter(Destination(),meter,key,0,q).awrite(b'abcd'))
        self.assertEqual(right.recv(4),b'abcd')
        q.reserve_write(1,2)
        return q

    @staticmethod
    def receipt(body):
        total=body['upload_bytes']+body['download_bytes']
        return dict(accepted=True,duplicate=False,grant_id=body['grant_id'],policy_id=body['policy_id'],
                    sequence=body['sequence'],confirmed_bytes=total,held_bytes=6-total,ready_for_execution=False)

    def test_only_success_frozen_pending_immutable_and_no_old_outbox(self):
        q=self.written();body=q.freeze_evidence()
        self.assertEqual((body['upload_bytes'],body['download_bytes']),(4,0))
        self.assertEqual(q.freeze_evidence(),body)
        with sqlite3.connect(q.path) as db:
            self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='allocation_evidence_outbox'").fetchone())
        seen=[]
        def transport(base,token,path,data):
            seen.append((path,data));return self.receipt(data)
        self.assertEqual(GrantDelivery(q).step('', '',0,transport=transport),'accepted')
        self.assertEqual(seen,[('/agent/grants/usage',body)])
        self.assertEqual(q.freeze_evidence(),body)
        self.assertEqual(GrantDelivery(q).step('', '',10,transport=transport),'idle')
        self.assertEqual(q.snapshot()['unknown_download_bytes'],2)

    def test_lost_response_reopen_preserves_request_and_backoff(self):
        q=self.written();body=q.freeze_evidence();seen=[]
        def lost(*args):seen.append(args[-1]);raise OSError('lost response')
        self.assertEqual(GrantDelivery(q).step('','',100,transport=lost),'retry')
        reopened=self.quota(self.limited())
        self.assertEqual(GrantDelivery(reopened).step('','',104,transport=lost),'deferred')
        def recovered(*args):seen.append(args[-1]);return self.receipt(args[-1])
        self.assertEqual(GrantDelivery(reopened).step('','',105,transport=recovered),'accepted')
        self.assertEqual(seen,[body,body])
        self.assertEqual(reopened.snapshot()['remaining_bytes'],0)

    def test_bad_response_cannot_ack_or_confirm_unknown(self):
        q=self.written();body=q.freeze_evidence()
        for i,change in enumerate(({'sequence':True},{'confirmed_bytes':6},{'held_bytes':0},{'extra':1},{'ready_for_execution':True})):
            result=dict(self.receipt(body),**change)
            self.assertEqual(GrantDelivery(q).step('','',i*100,transport=lambda *a:result),'retry')
        with sqlite3.connect(q.path) as db:self.assertEqual(db.execute('SELECT acknowledged FROM grant_usage_outbox').fetchone()[0],0)

    def test_revocation_survives_reopen_and_freeze(self):
        q=self.written();q.freeze_evidence()
        def revoked(*args):raise urllib.error.HTTPError('local',403,'revoked',{},io.BytesIO())
        self.assertEqual(GrantDelivery(q).step('','',0,transport=revoked),'blocked')
        q=self.quota(self.limited());q.freeze_evidence()
        self.assertEqual(GrantDelivery(q).step('','',100,transport=lambda *a:self.fail('sent')),'blocked')

    def test_ack_disk_failure_retains_pending(self):
        q=self.written();body=q.freeze_evidence()
        with sqlite3.connect(q.path) as db:
            db.execute("CREATE TRIGGER fail_ack BEFORE UPDATE ON grant_usage_outbox BEGIN SELECT RAISE(ABORT,'disk'); END")
        self.assertEqual(GrantDelivery(q).step('','',0,transport=lambda *a:self.receipt(body)),'retry')
        self.assertEqual(q.freeze_evidence(),body)
