"""Internal aggregate policy history; observations are NOT traffic allocations.

An execution ACK currently carries no aggregate cap. Recording what the master
observed must never be treated as evidence that an Agent enforced that cap.
"""
import time
from core import Error

SCHEMA='''
CREATE TABLE IF NOT EXISTS usage_total_policy_versions(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 cycle_id TEXT NOT NULL REFERENCES usage_cycles(id) ON DELETE CASCADE,
 total_bytes INTEGER CHECK(total_bytes IS NULL OR total_bytes>=0),
 observed_at REAL NOT NULL, origin TEXT NOT NULL CHECK(origin IN ('baseline','change')));
CREATE INDEX IF NOT EXISTS usage_total_policy_cycle ON usage_total_policy_versions(cycle_id,id);
CREATE TABLE IF NOT EXISTS usage_execution_total_observations(
 server_id INTEGER NOT NULL REFERENCES servers(id) ON DELETE CASCADE,
 session_id TEXT NOT NULL, revision INTEGER NOT NULL,
 node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
 user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 credential TEXT NOT NULL,
 policy_id INTEGER NOT NULL REFERENCES usage_total_policy_versions(id) ON DELETE CASCADE,
 observed_at REAL NOT NULL,
 PRIMARY KEY(server_id,session_id,revision,node_id,user_id,credential));
'''


def current(db,cycle_id):
    """Read/seed only a known current baseline inside the caller's write TX.

    Historical versions prior to upgrade cannot be reconstructed. Unexpected
    direct SQL edits are rejected instead of silently rewriting their history.
    """
    if not db.in_transaction: raise RuntimeError('policy history requires a transaction')
    cap=db.execute('SELECT total_bytes FROM usage_cycle_total_budgets WHERE cycle_id=?',(cycle_id,)).fetchone()
    row=db.execute('SELECT * FROM usage_total_policy_versions WHERE cycle_id=? ORDER BY id DESC LIMIT 1',(cycle_id,)).fetchone()
    if cap is None:
        if row is not None: raise Error('合计额度历史与当前策略不一致',409)
        return None
    if row is None:
        db.execute("INSERT INTO usage_total_policy_versions(cycle_id,total_bytes,observed_at,origin) VALUES(?,?,?,'baseline')",
                   (cycle_id,cap['total_bytes'],time.time()))
        row=db.execute('SELECT * FROM usage_total_policy_versions WHERE id=last_insert_rowid()').fetchone()
    if row['total_bytes']!=cap['total_bytes']: raise Error('合计额度历史与当前策略不一致',409)
    return dict(row)


def observe(db,snapshot,session,node,client):
    policy=current(db,client.get('cycle_id'))
    if policy is None: return
    # Revision is part of the identity: later policy observations cannot erase
    # earlier ones, even within the same cumulative-counter session.
    key=(snapshot['server_id'],session,snapshot['revision'],node['id'],client['user_id'],client['uuid'])
    previous=db.execute('''SELECT policy_id FROM usage_execution_total_observations
        WHERE server_id=? AND session_id=? AND revision=? AND node_id=? AND user_id=? AND credential=?''',key).fetchone()
    if previous is not None:
        if previous['policy_id']!=policy['id']: raise Error('同一执行版本的额度观察记录冲突',409)
        return
    db.execute('''INSERT INTO usage_execution_total_observations
        VALUES(?,?,?,?,?,?,?,?)''',(*key,policy['id'],time.time()))
