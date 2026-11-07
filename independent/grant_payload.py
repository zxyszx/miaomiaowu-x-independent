"""Local grant payload adapter; deliberately not runtime installation or ACK.

Only an explicitly supplied, validated staged envelope can construct this
adapter. No configuration listener, capability declaration or master execution
record is enabled here. Reservations include unknown writes across restart.
"""
import copy
from allocation_quota import AllocationQuota
from grant_agent import validate


class GrantPayloadQuota(AllocationQuota):
    def __init__(self,path,stage,value):
        value=copy.deepcopy(value)
        content=validate(value,stage.server_id,stage.session_id)
        client=content['client']
        budget=client.get('cycle_budget')
        usage=client.get('cycle_usage')
        if budget is not None and usage is None:
            raise ValueError('方向额度缺少明确基线')
        if usage is None and content['baseline_bytes'] != 0:
            raise ValueError('非零授予基线缺少方向归属')
        limits={}
        for label,field in (('upload','upload_bytes'),('download','download_bytes')):
            limit=None if budget is None else budget[field]
            limits[label]=None if limit is None else max(0,limit-usage[field])
        receipt=stage.stage(value)
        if receipt.get('ledger_kind')!='first_grant':
            raise ValueError('首次授予执行需要唯一first_grant暂存')
        scope=dict(allocation_id=content['grant_id'],policy_id=content['policy_id'],
                   cycle_id=content['cycle_id'],server_id=content['server_id'],
                   session_id=content['session_id'],node_id=content['node_id'],
                   user_id=client['user_id'],credential=client['uuid'],
                   reserved_bytes=content['reserved_bytes'],issued_at=content['issued_at'],
                   valid_until=content['valid_until'])
        super().__init__(path,scope,constraints=dict(digest=receipt['digest'],**limits))

    def _freeze_snapshot(self, db, snapshot):
        from grant_delivery import freeze_snapshot
        return freeze_snapshot(db, snapshot)
