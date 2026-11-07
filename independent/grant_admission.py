"""Transaction-scoped prerequisite for the FIRST aggregate execution grant.

This does not issue a grant, enable a protocol or reserve any bytes. A future
issuer must call this inside its own write transaction and allocate there;
the returned balance is never a portable authorization or an Agent quota.
Legacy execution remains fenced by the existing drain and sealed checkpoint.
"""
import hashlib
import json
import time
from core import Error
from drain_seal import inspect
from total_policy import current


def require_sealed_legacy(db,cycle):
    if not db.in_transaction:
        raise RuntimeError('grant admission requires the caller transaction')
    row=db.execute('SELECT * FROM usage_cycles WHERE id=?',(cycle,)).fetchone()
    now=time.time()
    if row is None or row['expired'] or not row['starts_at']<=now<row['ends_at']:
        raise Error('周期不存在、已失效或不在执行窗口',409)
    sealed,digest=inspect(db,cycle)
    if digest is None: raise Error('旧计量周期尚未完成封存',409)
    # Recheck durable evidence in the SAME transaction. A stored seal alone
    # cannot authorize execution after missing/unknown or changed accounting.
    try:
        fresh,_=inspect(db,cycle,revalidate=True)
    except (ValueError,TypeError,KeyError,IndexError) as exc:
        raise Error('旧计量证据格式或计数损坏',409) from exc
    if fresh['blockers']: raise Error('旧计量证据存在缺失或未归属用量',409)
    fresh=dict(fresh,state='captured_legacy_evidence_sealed')
    canonical=json.dumps(fresh,sort_keys=True,separators=(',',':'))
    if hashlib.sha256(canonical.encode()).hexdigest()!=digest or fresh!=sealed:
        raise Error('当前计量证据与原封存不一致',409)
    policy=current(db,cycle)
    if policy is None: raise Error('缺少明确的合计额度策略',409)
    consumed=fresh['final_upload_bytes']+fresh['final_download_bytes']
    cap=policy['total_bytes']
    # Internal None means unlimited; zero means deny. Never convert zero to
    # unlimited here (the original plan's zero convention belongs at import).
    remaining=None if cap is None else max(0,cap-consumed)
    return dict(cycle_id=cycle,user_id=row['user_id'],seal_digest=digest,
                policy_id=policy['id'],total_bytes=cap,
                confirmed_bytes=consumed,available_bytes=remaining,
                prerequisite_only=True,ready_for_execution=False)
