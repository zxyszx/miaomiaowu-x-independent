"""Authenticated, immutable Agent evidence intake, NOT billing or grants.

A success declaration is not proof of a remote socket write. Until exclusive
execution issuance and legacy migration are integrated, full holds remain and
these records never enter confirmed usage or change execution readiness.
"""
import hashlib
import json
import math
import time
from allocation_quota import FIELDS, MAX_COUNTER
from core import Error

COUNTERS = {f'{prefix}_{direction}_bytes' for prefix in
            ('reserved', 'successful', 'unknown', 'unattributed')
            for direction in ('upload', 'download')} | {'remaining_bytes'}

SCHEMA = '''
CREATE TABLE IF NOT EXISTS usage_allocation_evidence(
 allocation_id TEXT NOT NULL REFERENCES usage_total_allocations(allocation_id) ON DELETE CASCADE,
 sequence INTEGER NOT NULL, body TEXT NOT NULL, digest TEXT NOT NULL,
 received_at REAL NOT NULL, PRIMARY KEY(allocation_id,sequence));
'''


def validate(data):
    if not isinstance(data,dict) or set(data) != FIELDS | COUNTERS | {'sequence','ready_for_execution'}:
        raise Error('预留写入证据字段无效')
    for key in COUNTERS | {'sequence','policy_id','server_id','node_id','user_id','reserved_bytes'}:
        minimum = 1 if key not in COUNTERS else 0
        if type(data[key]) is not int or not minimum <= data[key] <= MAX_COUNTER:
            raise Error('预留写入证据计数无效')
    for key in ('allocation_id','session_id','cycle_id','credential'):
        if not isinstance(data[key],str) or not 1 <= len(data[key]) <= 128:
            raise Error('预留写入证据身份无效')
    for key in ('issued_at','valid_until'):
        if type(data[key]) not in (int,float) or not math.isfinite(data[key]) or not 0 <= data[key] <= MAX_COUNTER:
            raise Error('预留写入证据期限无效')
    if data['valid_until'] <= data['issued_at'] or data['ready_for_execution'] is not False:
        raise Error('预留写入证据不能授予执行权限')
    used = 0
    for direction in ('upload','download'):
        reserved, successful, unknown, unattributed = (data[f'{p}_{direction}_bytes'] for p in
                                                       ('reserved','successful','unknown','unattributed'))
        if reserved != successful+unknown or unattributed > unknown:
            raise Error('预留写入证据成功与未知计数不一致')
        used += reserved
    if used+data['remaining_bytes'] != data['reserved_bytes']:
        raise Error('预留写入证据超过原双向额度')


class AllocationEvidence:
    def __init__(self,registry): self.registry=registry

    def receive(self,token,data):
        validate(data)
        # Freeze input before taking the transaction; body is the exact receipt
        # identity, including unknown bytes, not a mutable latest-only report.
        body=json.dumps(data,sort_keys=True,separators=(',',':'),allow_nan=False)
        data=json.loads(body); digest=hashlib.sha256(body.encode()).hexdigest()
        r=self.registry
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token); r._check_session(db,sid,data['session_id'])
            token_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            allocation=db.execute('SELECT * FROM usage_total_allocations WHERE allocation_id=?',
                                  (data['allocation_id'],)).fetchone()
            if allocation is None or allocation['server_id']!=sid or allocation['token_hash']!=token_hash or allocation['session_id']!=data['session_id']:
                raise Error('写入证据不属于此Agent会话',403)
            if any(allocation[key]!=data[key] for key in FIELDS):
                raise Error('写入证据与原预留身份、版本或期限不一致',409)
            exact=db.execute('SELECT body FROM usage_allocation_evidence WHERE allocation_id=? AND sequence=?',
                             (data['allocation_id'],data['sequence'])).fetchone()
            if exact:
                if exact['body']!=body: raise Error('写入证据序号已用于不同内容',409)
            else:
                previous=db.execute('SELECT sequence,body FROM usage_allocation_evidence WHERE allocation_id=? ORDER BY sequence DESC LIMIT 1',
                                    (data['allocation_id'],)).fetchone()
                if previous:
                    prior=json.loads(previous['body'])
                    monotonic={f'{p}_{d}_bytes' for p in ('reserved','successful','unattributed') for d in ('upload','download')}
                    if data['sequence']<=previous['sequence'] or any(data[k]<prior[k] for k in monotonic):
                        raise Error('预留写入证据高水位不可回退',409)
                db.execute('INSERT INTO usage_allocation_evidence VALUES(?,?,?,?,?)',
                           (data['allocation_id'],data['sequence'],body,digest,time.time()))
            return dict(accepted=True,duplicate=exact is not None,allocation_id=data['allocation_id'],
                        policy_id=data['policy_id'],session_id=data['session_id'],cycle_id=data['cycle_id'],
                        server_id=sid,sequence=data['sequence'],digest=digest,
                        held_bytes=allocation['reserved_bytes'],evidence_only=True,ready_for_execution=False)
