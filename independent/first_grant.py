"""Durable first aggregate-grant intent, deliberately NOT delivered execution.

Admission and balance reservation share BEGIN IMMEDIATE. Only the first intent
is supported: later grants need an attributed execution ledger before they can
share a cycle safely. No timeout, deletion or policy change refunds this hold.
No HTTP/config/Agent path consumes this record yet; delivery must recheck all
effective node/plan/speed/connection policies and authenticate the bound Agent.
"""
import time
import uuid
from core import Error
from grant_admission import require_sealed_legacy
from usage_ledger import integer
from usage_settlement import EXECUTION_LEASE

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_first_total_grants(
 request_id TEXT PRIMARY KEY,grant_id TEXT NOT NULL UNIQUE,
 cycle_id TEXT NOT NULL UNIQUE REFERENCES usage_cycles(id) ON DELETE CASCADE,
 policy_id INTEGER NOT NULL REFERENCES usage_total_policy_versions(id),
 seal_digest TEXT NOT NULL,server_id INTEGER NOT NULL,session_id TEXT NOT NULL,
 token_hash TEXT NOT NULL,node_id INTEGER NOT NULL,user_id INTEGER NOT NULL,
 credential TEXT NOT NULL,requested_bytes INTEGER NOT NULL CHECK(requested_bytes>0),
 reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>0 AND reserved_bytes<=requested_bytes),
 baseline_bytes TEXT NOT NULL,issued_at REAL NOT NULL,valid_until REAL NOT NULL);
'''

FIELDS={'request_id','cycle_id','session_id','node_id','user_id','credential','requested_bytes'}

def receipt(row):
    return {**{k:row[k] for k in ('request_id','grant_id','cycle_id','policy_id','seal_digest',
        'server_id','session_id','node_id','user_id','credential','requested_bytes',
        'reserved_bytes','issued_at','valid_until')},
        'baseline_bytes':int(row['baseline_bytes']),
        'delivery_state':'not_delivered','ready_for_execution':False}


class FirstGrant:
    def __init__(self,registry): self.registry=registry

    def prepare(self,token,data):
        if not isinstance(data,dict) or set(data)!=FIELDS: raise Error('首次授予字段无效')
        r=self.registry
        for key in ('request_id','cycle_id','session_id','credential'): r._session_value(data[key])
        if any(not integer(data[k],1) for k in ('node_id','user_id','requested_bytes')):
            raise Error('首次授予需要有效资源编号和正整数字节')
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid=r._agent_id(db,token); r._check_session(db,sid,data['session_id'])
            token_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
            old=db.execute('SELECT * FROM usage_first_total_grants WHERE request_id=?',(data['request_id'],)).fetchone()
            if old is not None:
                if old['server_id']!=sid or old['token_hash']!=token_hash:
                    raise Error('首次授予不属于此Agent',403)
                if any(old[k]!=data[k] for k in FIELDS): raise Error('首次授予请求编号冲突',409)
                from usage_accounting import FIRST,require_source
                require_source(db,FIRST,old)
                return receipt(old)
            if db.execute('SELECT 1 FROM usage_first_total_grants WHERE cycle_id=?',(data['cycle_id'],)).fetchone():
                raise Error('此周期已有首次授予，后续分配尚未启用',409)
            baseline=require_sealed_legacy(db,data['cycle_id'])
            binding=db.execute('''SELECT 1 FROM node_users u JOIN nodes n ON n.id=u.node_id
                WHERE n.server_id=? AND u.node_id=? AND u.user_id=? AND u.credential=?''',
                (sid,data['node_id'],data['user_id'],data['credential'])).fetchone()
            if not binding or baseline['user_id']!=data['user_id']:
                raise Error('首次授予不属于当前服务器绑定身份和周期',403)
            from usage_accounting import liability
            count=data['requested_bytes']; available=baseline['available_bytes']
            if baseline['total_bytes'] is not None:
                available=max(0,baseline['total_bytes']-liability(db,data['cycle_id']))
            if available is not None: count=min(count,available)
            if count==0: raise Error('周期没有可授予余额',409)
            now=time.time()
            end=db.execute('SELECT ends_at FROM usage_cycles WHERE id=?',(data['cycle_id'],)).fetchone()[0]
            until=min(now+EXECUTION_LEASE,end)
            if until<=now: raise Error('首次授予窗口已结束',409)
            ident=str(uuid.uuid4())
            db.execute('''INSERT INTO usage_first_total_grants VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (data['request_id'],ident,data['cycle_id'],baseline['policy_id'],
                 baseline['seal_digest'],sid,data['session_id'],token_hash,data['node_id'],
                 data['user_id'],data['credential'],data['requested_bytes'],count,
                 str(baseline['confirmed_bytes']),now,until))
            row=db.execute('SELECT * FROM usage_first_total_grants WHERE request_id=?',(data['request_id'],)).fetchone()
            from usage_accounting import FIRST,create_source
            create_source(db,FIRST,row)
            return receipt(row)
