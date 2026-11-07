"""Bounded accounting-only tails for independently acknowledged execution grants.

Never authorizes traffic or restores configuration. Current Agent credentials
remain mandatory; credential rotation deliberately revokes old settlements.
"""
import time
from core import Error
from execution_directions import SCHEMA as DIRECTION_SCHEMA

EXECUTION_LEASE=45
DELIVERY_WINDOW=300
SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_execution_grants(
    server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    credential TEXT NOT NULL, cycle_id TEXT REFERENCES usage_cycles(id) ON DELETE SET NULL, token_hash TEXT NOT NULL,
    issued_at REAL NOT NULL, settle_until REAL NOT NULL,
    PRIMARY KEY(server_id,session_id,node_id,user_id,credential));
'''+DIRECTION_SCHEMA


def record_execution(db, snapshot, session):
    if session is None: return # legacy receipts provide no session-bound evidence
    now=time.time(); sid=snapshot['server_id']
    token_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
    for node in snapshot['nodes']:
        if not node['config']['enabled']: continue
        for client in node.get('clients',[]):
            end=min(now+EXECUTION_LEASE,snapshot.get('valid_until',float('inf')),
                    client.get('valid_until',float('inf')))
            if end<=now: continue
            previous=db.execute('SELECT cycle_id FROM usage_execution_grants WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?',
                                (sid,session,node['id'],client['user_id'],client['uuid'])).fetchone()
            if previous is not None and previous['cycle_id']!=client.get('cycle_id'):
                # Cumulative counters have no per-cycle split. Rebinding this
                # proof would silently charge old bytes to the next period.
                raise Error('消费周期已变化，需要新的执行会话',409)
            db.execute('INSERT INTO usage_execution_grants(server_id,session_id,node_id,user_id,credential,cycle_id,token_hash,issued_at,settle_until) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(server_id,session_id,node_id,user_id,credential) DO UPDATE SET token_hash=excluded.token_hash,issued_at=excluded.issued_at,settle_until=excluded.settle_until',
                       (sid,session,node['id'],client['user_id'],client['uuid'],client.get('cycle_id'),token_hash,now,end+DELIVERY_WINDOW))
            from execution_directions import record as record_directions
            record_directions(db,snapshot,session,node,client,token_hash,now,end,end+DELIVERY_WINDOW)
            from total_policy import observe
            observe(db,snapshot,session,node,client)


def authorize(db, sid, data, *, retry=False):
    token_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
    now=time.time()
    cycles={}
    for item in data['items']:
        row=db.execute('SELECT * FROM usage_execution_grants WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?',
                       (sid,data['session_id'],item['node_id'],item['user_id'],item['credential'])).fetchone()
        if row is None or row['token_hash']!=token_hash:
            raise Error('流量尾部没有此服务器会话的执行授权记录',403)
        # An exact accepted retry adds nothing and may recover a lost reply after
        # the deadline. New growth is bounded by the original execution receipt.
        if not retry and not row['issued_at']<=now<=row['settle_until']:
            raise Error('流量尾部结算窗口已结束或时钟异常',403)
        cycles[(item['node_id'],item['user_id'],item['credential'])]=row['cycle_id']
    return cycles
