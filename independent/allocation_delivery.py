"""Explicit frozen-evidence delivery, not execution issuance or usage billing.

One step makes at most one authenticated request. Persist the retry deadline
before I/O so a crash cannot discard backoff. No credentials are stored here.
This worker is not automatically enabled by the current Agent configuration.
"""
from contextlib import closing, contextmanager
import json
import math
import sqlite3
import urllib.error

from agent import request
from allocation_outbox import SCHEMA as OUTBOX_SCHEMA, acknowledge
from quota_store import PAYLOAD_LOCK_TIMEOUT

SCHEMA = '''CREATE TABLE IF NOT EXISTS allocation_evidence_delivery(
 allocation_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL,
 attempts INTEGER NOT NULL, next_attempt REAL NOT NULL,
 blocked INTEGER NOT NULL CHECK(blocked IN (0,1)))'''


class AllocationDelivery:
    def __init__(self, quota):
        self.quota = quota
        with self.connect() as db:
            db.execute(OUTBOX_SCHEMA)
            db.execute(SCHEMA)

    @contextmanager
    def connect(self):
        try:
            with closing(sqlite3.connect(self.quota.path, timeout=PAYLOAD_LOCK_TIMEOUT)) as db, db:
                db.row_factory = sqlite3.Row
                db.execute('PRAGMA synchronous=FULL')
                db.execute('BEGIN IMMEDIATE')
                yield db
        except sqlite3.Error as error:
            raise OSError('写入证据投递状态保存失败，保留原请求') from error

    def step(self, base, token, now, *, transport=request):
        if type(now) not in (int, float) or not math.isfinite(now) or now < 0:
            raise ValueError('投递时间无效')
        ident = self.quota._scope['allocation_id']
        with self.connect() as db:
            owner = db.execute('SELECT scope FROM allocation_quota WHERE allocation_id=?', (ident,)).fetchone()
            if owner is None or owner['scope'] != self.quota._canonical:
                raise ValueError('写入证据投递身份不一致')
            frozen = db.execute('SELECT * FROM allocation_evidence_outbox WHERE allocation_id=?', (ident,)).fetchone()
            if frozen is None or frozen['acknowledged']:
                return 'idle'
            state = db.execute('SELECT * FROM allocation_evidence_delivery WHERE allocation_id=?', (ident,)).fetchone()
            # Revocation survives restart and cannot be reset by enqueueing.
            if state is not None and state['blocked']:
                return 'blocked'
            sequence = frozen['sequence']
            if state is not None and state['sequence'] == sequence and state['next_attempt'] > now:
                return 'deferred'
            attempts = state['attempts'] + 1 if state is not None and state['sequence'] == sequence else 1
            delay = min(60, 15 * 2 ** min(attempts - 1, 2))
            db.execute('INSERT OR REPLACE INTO allocation_evidence_delivery VALUES(?,?,?,?,0)',
                       (ident, sequence, attempts, now + delay))
            body = json.loads(frozen['body'])
            digest = frozen['digest']
        try:
            receipt = transport(base, token, '/agent/usage/allocation-evidence', body)
        except urllib.error.HTTPError as error:
            revoked = error.code in (401, 403)
            error.close()
            if revoked:
                with self.connect() as db:
                    db.execute('UPDATE allocation_evidence_delivery SET blocked=1 WHERE allocation_id=? AND sequence=?',
                               (ident, sequence))
                return 'blocked'
            return 'retry'
        except (OSError, ValueError):
            return 'retry'
        # Strict original-scope receipt validation and FULL durable ACK. A
        # failed local commit leaves the same immutable request retryable.
        try:
            # Another caller may ACK this request and freeze its successor
            # during I/O. Never accept a successor's receipt for our request,
            # even if it now matches the latest local outbox row.
            if not isinstance(receipt, dict) or receipt.get('sequence') != sequence or receipt.get('digest') != digest:
                raise ValueError('写入证据回执不属于本次请求')
            acknowledge(self.quota, receipt)
        except (OSError, ValueError):
            return 'retry'
        return 'accepted'
