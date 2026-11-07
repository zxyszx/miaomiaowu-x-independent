"""Resolve plan policy without changing the identity's bound contract.

The explicit selector and candidate snapshot are internal plumbing for future
grace execution; neither grants access. Existing callers omit the selector and
retain the original binding and override order. Live callers expire temporary
rules as before; read-only inspection can use a frozen evaluation time.
"""
import json
from core import Error

_BOUND=object()


def selected(db,user_id,plan_id=_BOUND):
    if plan_id is not _BOUND:
        if type(plan_id) is not int or plan_id<1: raise Error('执行套餐编号无效')
        if not db.execute('SELECT 1 FROM plans WHERE id=?',(plan_id,)).fetchone():
            raise Error('执行套餐不存在',404)
        return plan_id
    row=db.execute('SELECT plan_id FROM user_plans WHERE user_id=?',(user_id,)).fetchone()
    return row[0] if row else None


def payload(db,table,key,ident,column='payload'):
    # Table/key/column arguments are module-owned constants, never HTTP input.
    row=db.execute(f'SELECT {column} FROM {table} WHERE {key}=?',(ident,)).fetchone()
    return json.loads(row[0]) if row else None


def temporary(db,user_id,now=None):
    # Live callers already expire rules. A read-only candidate can instead
    # ignore an ended rule at its frozen evaluation time without deleting it.
    row=db.execute('SELECT payload,expires_at FROM temporary_bandwidth WHERE user_id=?',(user_id,)).fetchone()
    return json.loads(row['payload']) if row and (now is None or row['expires_at']>now) else None


def bandwidth(db,user_id,*,plan_id=_BOUND,now=None):
    plan_id=selected(db,user_id,plan_id)
    value=temporary(db,user_id,now)
    if value is not None: return value
    value=payload(db,'user_bandwidth','user_id',user_id)
    if value is not None: return value
    return payload(db,'plans','id',plan_id,'bandwidth')


def connections(db,user_id,*,plan_id=_BOUND):
    plan_id=selected(db,user_id,plan_id)
    value=payload(db,'user_connections','user_id',user_id)
    if value is not None: return value
    value=payload(db,'plan_connections','plan_id',plan_id)
    return value if value is not None else {'max_connections':None}


def node_override(db,user_id,node_id,kind,*,plan_id=_BOUND,now=None):
    if kind not in ('connections','bandwidth'): raise ValueError('unknown node policy kind')
    plan_id=selected(db,user_id,plan_id)
    if kind=='bandwidth' and temporary(db,user_id,now) is not None: return None
    tables=('user_connections',) if kind=='connections' else ('user_bandwidth',)
    for table in tables:
        if db.execute(f'SELECT 1 FROM {table} WHERE user_id=?',(user_id,)).fetchone(): return None
    row=db.execute(f'SELECT payload FROM plan_node_{kind} WHERE plan_id=? AND node_id=?',
                   (plan_id,node_id)).fetchone()
    return json.loads(row[0]) if row else None


def candidate_snapshot(db,user_id,now=None):
    """Coherent internal inspection, NOT an executable Agent configuration.

    Selection, scope, overrides and current cycle all use one caller-owned
    transaction. A target's aggregate quota is not implemented yet: retain an
    explicit blocker instead of presenting directional budgets as that cap.
    This function neither changes bindings nor rewrites usage or reservations.
    """
    from plan_grace import candidate
    from usage_cycles import boundary
    choice,now=candidate(db,user_id,now)
    target=choice['candidate_plan_id']
    result={**choice,'policies':None,'cycle':None,
            'blockers':['execution_not_integrated','plan_aggregate_quota_unresolved']}
    if target is not None:
        result['policies']={
            'bandwidth':bandwidth(db,user_id,plan_id=target,now=now),
            'connections':connections(db,user_id,plan_id=target),
            'nodes':[{'node_id':nid,
                      'bandwidth':node_override(db,user_id,nid,'bandwidth',plan_id=target,now=now),
                      'connections':node_override(db,user_id,nid,'connections',plan_id=target)}
                     for nid in choice['node_ids']]}
    cycle=boundary(db,user_id,now)
    if cycle is not None:
        ident,end=cycle
        result['cycle']={'id':ident,'ends_at':end,'budget':None}
        if ident is not None:
            row=db.execute('SELECT upload_bytes,download_bytes FROM usage_cycle_budgets WHERE cycle_id=?',(ident,)).fetchone()
            if row is not None: result['cycle']['budget']=dict(row)
        else: result['blockers'].append('no_open_usage_cycle')
    return result
