"""Stored downgrade policy foundation, not yet used by execution snapshots.

No HTTP/UI entry is exposed until node, bandwidth and quota transitions can
be committed together. Saving this policy alone never extends access.
"""
from core import Error
import time

SCHEMA='''
CREATE TABLE IF NOT EXISTS plan_grace_policies(
 plan_id INTEGER PRIMARY KEY REFERENCES plans(id) ON DELETE CASCADE,
 target_plan_id INTEGER NOT NULL REFERENCES plans(id) ON DELETE RESTRICT,
 days INTEGER NOT NULL CHECK(days BETWEEN 1 AND 365),
 CHECK(plan_id != target_plan_id));
CREATE TABLE IF NOT EXISTS plan_grace_observations(
 user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 plan_id INTEGER NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
 expires_at REAL NOT NULL,
 observed_at REAL NOT NULL CHECK(observed_at >= expires_at),
 PRIMARY KEY(user_id,plan_id,expires_at));
'''


def observe_due(db,now):
    """Persist the furthest observed expired-contract time, within caller's txn.

    This never grants grace access. Keep observations across policy edits so a
    clock rollback or process restart cannot rewind the same contract.
    """
    db.execute('''INSERT INTO plan_grace_observations(user_id,plan_id,expires_at,observed_at)
        SELECT l.user_id,p.plan_id,l.expires_at,? FROM plan_lifecycle l
        JOIN user_plans p ON p.user_id=l.user_id
        WHERE l.expires_at<=? AND NOT EXISTS
          (SELECT 1 FROM shared_users s WHERE s.user_id=l.user_id)
        ON CONFLICT(user_id,plan_id,expires_at) DO UPDATE SET
          observed_at=max(plan_grace_observations.observed_at,excluded.observed_at)''',(now,now))


def candidate(db,user_id,now=None):
    """Read a candidate in the caller transaction; return selection and clock.

    This is not permission to execute it. All policy consumers must share the
    same SQLite snapshot and the same non-rewinding contract time.
    """
    from plan_lifecycle import policy
    from node_groups import allowed
    if not db.in_transaction:
        raise RuntimeError('grace selection requires a caller-owned transaction')
    if type(user_id) is not int or user_id<1: raise Error('用户编号无效')
    bound=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(user_id,)).fetchone()
    if bound is None: raise Error('用户没有绑定套餐',404)
    if db.execute('SELECT 1 FROM shared_users WHERE user_id=?',(user_id,)).fetchone():
        raise Error('委托身份不能预览所有者套餐降级',409)
    original=bound[0]; rule=GracePolicies.result(db,original); life=policy(db,user_id)
    enabled,end,active=life if life else (True,None,True)
    now=time.time() if now is None else now; target=None; until=end
    observed=db.execute('''SELECT observed_at FROM plan_grace_observations
        WHERE user_id=? AND plan_id=? AND expires_at=?''',(user_id,original,end)).fetchone()
    if observed: now=max(now,observed[0])
    if not enabled: state='disabled'
    elif end is None or (now<end and active):
        state='active'; target=original
    elif end is not None and now>=end and rule and now<end+rule['days']*86400:
        state='grace'; target=rule['target_plan_id']; until=end+rule['days']*86400
    else: state='expired'
    # Existing node_users are the only candidate identities. A preview
    # never creates access to target-plan-only nodes or exposes UUIDs.
    nodes={row[0] for row in db.execute('SELECT node_id FROM node_users WHERE user_id=?',(user_id,))}
    if target is None: nodes=set()
    elif db.execute('SELECT 1 FROM plan_node_scopes WHERE plan_id=?',(target,)).fetchone():
        nodes &= allowed(db,target)
    return dict(user_id=user_id,original_plan_id=original,candidate_plan_id=target,
                state=state,valid_until=until,node_ids=sorted(nodes),
                execution_ready=False),now


class GracePolicies:
    def __init__(self,registry): self.registry=registry

    @staticmethod
    def exists(db,ident):
        if type(ident) is not int or ident<1: raise Error('套餐编号无效')
        if not db.execute('SELECT 1 FROM plans WHERE id=?',(ident,)).fetchone():
            raise Error('套餐不存在',404)

    @staticmethod
    def result(db,ident):
        row=db.execute('SELECT target_plan_id,days FROM plan_grace_policies WHERE plan_id=?',(ident,)).fetchone()
        return dict(row) if row else None

    def get(self,ident):
        with self.registry.connect() as db:
            db.execute('BEGIN')
            self.exists(db,ident)
            return self.result(db,ident)

    def save(self,ident,data):
        if not isinstance(data,dict) or set(data)!={'target_plan_id','days'}:
            raise Error('降级规则需要 target_plan_id 和 days')
        target,days=data['target_plan_id'],data['days']
        if type(days) is not int or not 1<=days<=365: raise Error('宽限期须在 1~365 天之间')
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.exists(db,ident); self.exists(db,target)
            # Do not permit self references or circular downgrade chains.
            seen={ident}; current=target
            while current is not None:
                if current in seen: raise Error('降级套餐不能形成循环',409)
                seen.add(current)
                row=db.execute('SELECT target_plan_id FROM plan_grace_policies WHERE plan_id=?',(current,)).fetchone()
                current=row[0] if row else None
            db.execute('''INSERT INTO plan_grace_policies VALUES(?,?,?) ON CONFLICT(plan_id)
                DO UPDATE SET target_plan_id=excluded.target_plan_id,days=excluded.days''',(ident,target,days))
            return self.result(db,ident)

    def delete(self,ident):
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.exists(db,ident)
            db.execute('DELETE FROM plan_grace_policies WHERE plan_id=?',(ident,))

    def preview(self,user_id):
        """Read-only candidate selection; never an authorization decision.

        Quota transitions and execution still have to be integrated before
        callers may activate a grace candidate. Preserve original credentials.
        """
        with self.registry.connect() as db:
            db.execute('BEGIN')
            return candidate(db,user_id)[0]
