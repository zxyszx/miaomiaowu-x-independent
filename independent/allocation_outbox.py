"""Local immutable evidence requests, not a network worker or billing ledger."""
from contextlib import closing
import hashlib
import json
import sqlite3

from allocation_quota import MAX_COUNTER
from quota_store import PAYLOAD_LOCK_TIMEOUT

SCHEMA = '''CREATE TABLE IF NOT EXISTS allocation_evidence_outbox(
 allocation_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL,
 body TEXT NOT NULL, digest TEXT NOT NULL,
 acknowledged INTEGER NOT NULL CHECK(acknowledged IN (0,1)))'''


def freeze_snapshot(db, snapshot):
    """Called inside the quota's FULL write transaction, after snapshot reads."""
    if not db.in_transaction: raise RuntimeError('evidence freeze requires transaction')
    db.execute(SCHEMA)
    ident=snapshot['allocation_id']
    old=db.execute('SELECT sequence,body,acknowledged FROM allocation_evidence_outbox WHERE allocation_id=?',
                   (ident,)).fetchone()
    if old and not old[2]: return json.loads(old[1])
    sequence=old[0]+1 if old else 1
    if sequence>MAX_COUNTER: raise ValueError('写入证据序号已耗尽')
    body=json.dumps(dict(snapshot,sequence=sequence),sort_keys=True,separators=(',',':'),allow_nan=False)
    digest=hashlib.sha256(body.encode()).hexdigest()
    db.execute('INSERT OR REPLACE INTO allocation_evidence_outbox VALUES(?,?,?,?,0)',
               (ident,sequence,body,digest))
    return json.loads(body)


def acknowledge(quota, receipt):
    """Only the exact authenticated response may advance local evidence state.

    Caller must authenticate its transport. Matching a digest alone is not
    remote authentication. Preserve quota counters, unknown writes and holds.
    """
    fields={'accepted','duplicate','allocation_id','policy_id','session_id','cycle_id',
            'server_id','sequence','digest','held_bytes','evidence_only','ready_for_execution'}
    if not isinstance(receipt,dict) or set(receipt)!=fields:
        raise ValueError('写入证据回执字段无效')
    if (receipt['accepted'] is not True or type(receipt['duplicate']) is not bool or
            receipt['evidence_only'] is not True or receipt['ready_for_execution'] is not False):
        raise ValueError('写入证据回执状态无效')
    for key in ('policy_id','server_id','sequence','held_bytes'):
        if type(receipt[key]) is not int or not 1<=receipt[key]<=MAX_COUNTER:
            raise ValueError('写入证据回执编号无效')
    # Freeze caller-owned response before locking or comparing it.
    receipt=dict(receipt)
    try:
        with closing(sqlite3.connect(quota.path,timeout=PAYLOAD_LOCK_TIMEOUT)) as db, db:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            db.execute(SCHEMA)
            owner=db.execute('SELECT scope FROM allocation_quota WHERE allocation_id=?',
                             (quota._scope['allocation_id'],)).fetchone()
            if owner is None or owner[0]!=quota._canonical:
                raise ValueError('写入证据本地身份不一致')
            row=db.execute('SELECT sequence,body,digest,acknowledged FROM allocation_evidence_outbox WHERE allocation_id=?',
                           (quota._scope['allocation_id'],)).fetchone()
            if row is None: raise ValueError('没有待核对的写入证据')
            data=json.loads(row[1])
            expected={key:data[key] for key in ('allocation_id','policy_id','session_id','cycle_id','server_id','sequence')}
            expected.update(digest=row[2],held_bytes=data['reserved_bytes'])
            if any(receipt[key]!=value for key,value in expected.items()):
                raise ValueError('写入证据回执与冻结请求不一致')
            if row[3]: return False
            db.execute('UPDATE allocation_evidence_outbox SET acknowledged=1 WHERE allocation_id=?',
                       (data['allocation_id'],))
            return True
    except sqlite3.Error as error:
        raise OSError('写入证据回执持久化失败，保留冻结请求') from error
