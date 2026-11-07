"""Explicit durable delivery of successful grant payload; no automatic worker.

Unknown writes remain local held reservations. A frozen cumulative request is
never replaced before its exact authenticated receipt is durably acknowledged.
"""
from contextlib import closing, contextmanager
import json
import math
import sqlite3
import urllib.error
from agent import request
from allocation_quota import MAX_COUNTER
from quota_store import PAYLOAD_LOCK_TIMEOUT

SCHEMA='''CREATE TABLE IF NOT EXISTS grant_usage_outbox(
 grant_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL, body TEXT NOT NULL,
 acknowledged INTEGER NOT NULL CHECK(acknowledged IN (0,1)));
CREATE TABLE IF NOT EXISTS grant_usage_delivery(
 grant_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL, attempts INTEGER NOT NULL,
 next_attempt REAL NOT NULL, blocked INTEGER NOT NULL CHECK(blocked IN (0,1)));'''

def schema(db):
    # executescript would commit the caller's payload transaction.
    for statement in SCHEMA.split(';'):
        if statement.strip(): db.execute(statement)

def freeze_snapshot(db, snapshot):
    if not db.in_transaction: raise RuntimeError('grant freeze requires transaction')
    schema(db); gid=snapshot['allocation_id']
    old=db.execute('SELECT sequence,body,acknowledged FROM grant_usage_outbox WHERE grant_id=?',(gid,)).fetchone()
    if old and not old[2]: return json.loads(old[1])
    body=dict(grant_id=gid,policy_id=snapshot['policy_id'],session_id=snapshot['session_id'],
              sequence=old[0]+1 if old else 1,
              upload_bytes=snapshot['successful_upload_bytes'],download_bytes=snapshot['successful_download_bytes'])
    if body['sequence']>MAX_COUNTER: raise ValueError('授予计量序号已耗尽')
    if old:
        previous=json.loads(old[1])
        if all(body[k]==previous[k] for k in ('upload_bytes','download_bytes')): return previous
    db.execute('INSERT OR REPLACE INTO grant_usage_outbox VALUES(?,?,?,0)',
               (gid,body['sequence'],json.dumps(body,sort_keys=True,separators=(',',':'))))
    return body

class GrantDelivery:
    def __init__(self,quota):
        # Never send ordinary allocations to the grant accounting endpoint.
        from grant_payload import GrantPayloadQuota
        if not isinstance(quota,GrantPayloadQuota): raise ValueError('需要授予载荷存储')
        self.quota=quota
        with self.connect() as db: schema(db)

    @contextmanager
    def connect(self):
        try:
            with closing(sqlite3.connect(self.quota.path,timeout=PAYLOAD_LOCK_TIMEOUT)) as db,db:
                db.row_factory=sqlite3.Row
                db.execute('PRAGMA synchronous=FULL');db.execute('BEGIN IMMEDIATE')
                yield db
        except sqlite3.Error as error:
            raise OSError('授予计量持久化失败，保留待确认请求') from error

    def step(self,base,token,now,*,transport=request):
        if type(now) not in (int,float) or not math.isfinite(now) or now<0: raise ValueError('投递时间无效')
        gid=self.quota._scope['allocation_id']
        with self.connect() as db:
            owner=db.execute('SELECT scope FROM allocation_quota WHERE allocation_id=?',(gid,)).fetchone()
            if owner is None or owner[0]!=self.quota._canonical: raise ValueError('授予计量本地身份不一致')
            row=db.execute('SELECT * FROM grant_usage_outbox WHERE grant_id=?',(gid,)).fetchone()
            if row is None or row['acknowledged']: return 'idle'
            state=db.execute('SELECT * FROM grant_usage_delivery WHERE grant_id=?',(gid,)).fetchone()
            if state and state['blocked']: return 'blocked'
            if state and state['sequence']==row['sequence'] and state['next_attempt']>now: return 'deferred'
            attempts=state['attempts']+1 if state and state['sequence']==row['sequence'] else 1
            db.execute('INSERT OR REPLACE INTO grant_usage_delivery VALUES(?,?,?,?,0)',
                       (gid,row['sequence'],attempts,now+min(60,5*2**min(attempts-1,4))))
            body=json.loads(row['body'])
        try:
            receipt=transport(base,token,'/agent/grants/usage',body)
            fields={'accepted','duplicate','grant_id','policy_id','sequence','confirmed_bytes','held_bytes','ready_for_execution'}
            if type(receipt) is not dict or set(receipt)!=fields: raise ValueError('授予计量回执字段无效')
            if receipt['accepted'] is not True or receipt['ready_for_execution'] is not False or type(receipt['duplicate']) is not bool: raise ValueError('授予计量回执状态无效')
            expected=dict(grant_id=gid,policy_id=body['policy_id'],sequence=body['sequence'],
                confirmed_bytes=body['upload_bytes']+body['download_bytes'],
                held_bytes=self.quota._scope['reserved_bytes']-body['upload_bytes']-body['download_bytes'])
            if any(type(receipt[k]) is not type(v) or receipt[k]!=v for k,v in expected.items()): raise ValueError('授予计量回执身份或计数不一致')
            with self.connect() as db:
                # An overlapping caller may already have advanced the outbox.
                db.execute('UPDATE grant_usage_outbox SET acknowledged=1 WHERE grant_id=? AND sequence=? AND body=?',
                           (gid,body['sequence'],row['body']))
            return 'accepted'
        except urllib.error.HTTPError as error:
            blocked=error.code in (401,403);error.close()
            if blocked:
                with self.connect() as db:
                    db.execute('UPDATE grant_usage_delivery SET blocked=1 WHERE grant_id=?',(gid,))
                return 'blocked'
            return 'retry'
        except (OSError,ValueError): return 'retry'
