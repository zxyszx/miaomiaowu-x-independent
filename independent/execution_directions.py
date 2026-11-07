"""Versioned direction fields from actual runtime ACKs, not aggregate grants.

Finite delivered directions may support accounting-only old tails after a
lowering. Missing budgets never inherit later policy. Only the actual runtime
ACK transaction records this history; staging a configuration is insufficient.
"""
import json
from core import Error

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_execution_direction_receipts(
 server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
 session_id TEXT NOT NULL, revision INTEGER NOT NULL,
 node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
 user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 credential TEXT NOT NULL,
 cycle_id TEXT REFERENCES usage_cycles(id) ON DELETE CASCADE,
 token_hash TEXT NOT NULL, digest TEXT NOT NULL,
 budget_json TEXT NOT NULL, baseline_json TEXT NOT NULL,
 issued_at REAL NOT NULL, execution_until REAL NOT NULL, settle_until REAL NOT NULL,
 PRIMARY KEY(server_id,session_id,revision,node_id,user_id,credential));
'''


def encoded(value,*,budget=False):
    if value is None: return 'null'
    if (not isinstance(value,dict) or set(value)!={'upload_bytes','download_bytes'} or
        any(not (type(x) is int and x>=0 or budget and x is None) for x in value.values())):
        raise Error('执行方向额度或基线格式无效',409)
    return json.dumps(value,sort_keys=True,separators=(',',':'))


def record(db,snapshot,session,node,client,token_hash,now,end,settle_until):
    if not db.in_transaction: raise RuntimeError('execution receipt requires a transaction')
    budget=encoded(client.get('cycle_budget'),budget=True)
    baseline=encoded(client.get('cycle_usage'))
    if (budget=='null')!=(baseline=='null'):
        raise Error('执行方向额度与基线必须同时存在',409)
    key=(snapshot['server_id'],session,snapshot['revision'],node['id'],client['user_id'],client['uuid'])
    identity=(client.get('cycle_id'),token_hash,snapshot['digest'],budget,baseline)
    previous=db.execute('''SELECT cycle_id,token_hash,digest,budget_json,baseline_json
        FROM usage_execution_direction_receipts
        WHERE server_id=? AND session_id=? AND revision=? AND node_id=? AND user_id=? AND credential=?''',key).fetchone()
    if previous is not None:
        if tuple(previous)!=identity: raise Error('同一执行版本的方向额度记录冲突',409)
        # Re-ACK of the still-current exact configuration may renew its existing
        # lease. It cannot rewrite its budget/baseline or extend another version.
        db.execute('''UPDATE usage_execution_direction_receipts
            SET execution_until=MAX(execution_until,?),settle_until=MAX(settle_until,?)
            WHERE server_id=? AND session_id=? AND revision=? AND node_id=? AND user_id=? AND credential=?''',
            (end,settle_until,*key))
        return
    db.execute('INSERT INTO usage_execution_direction_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
               (*key,*identity,now,end,settle_until))


def permits_tail(db, sid, session, item, cycle_id, budget, used, delta, now):
    """Accounting-only finite historical ceiling, shared by the whole cycle.

    Caller must first authenticate the settlement grant. No budgets are added
    across hosts or revisions; one intact ACK must support both directions.
    Unknown usage and missing historical fields never create an allowance.
    Aggregate enforcement is intentionally outside this proof.
    """
    if used[2]: return False
    token_hash=db.execute('SELECT token_hash FROM agents WHERE server_id=?',(sid,)).fetchone()[0]
    rows=db.execute('''SELECT budget_json,baseline_json FROM usage_execution_direction_receipts
        WHERE server_id=? AND session_id=? AND node_id=? AND user_id=? AND credential=?
        AND cycle_id=? AND token_hash=? AND issued_at<=? AND settle_until>=?''',
        (sid,session,item['node_id'],item['user_id'],item['credential'],cycle_id,token_hash,now,now))
    fields=('upload_bytes','download_bytes')
    for row in rows:
        old=json.loads(row['budget_json']); baseline=json.loads(row['baseline_json'])
        if old is None or baseline is None: continue
        if any(baseline[k]>used[i] for i,k in enumerate(fields)): continue
        # Current unlimited directions need no exception. For a lowered finite
        # direction only a finite actually-delivered cap is usable evidence.
        if all(budget[k] is None or used[i]+delta[i]<=budget[k] or
               (old[k] is not None and used[i]+delta[i]<=old[k])
               for i,k in enumerate(fields)):
            return True
    return False
