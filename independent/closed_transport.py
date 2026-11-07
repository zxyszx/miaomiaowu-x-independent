"""Receipt binding for fragment transport; no scheduler or evidence deletion."""

def fragment_receipt(cycle_id,envelope,fragment,receipt):
    """Accept only a receipt for this exact manifest, part and original identity.

    Callers must obtain envelope/fragment from verified local ClosedFragments,
    and must not dispatch before actual runtime closure and all frozen tail ACKs.
    This helper does not assert either precondition or mutate delivery state.
    """
    m=envelope['manifest']; index=fragment['body']['index']
    if (not isinstance(receipt,dict) or receipt.get('accepted') is not True or
        type(receipt.get('complete')) is not bool or
        type(receipt.get('server_id')) is not int or receipt['server_id']!=m['server_id'] or
        receipt.get('cycle_id')!=cycle_id or receipt.get('session_id')!=m['session_id'] or
        receipt.get('manifest_digest')!=envelope['digest'] or
        type(receipt.get('part')) is not int or receipt['part']!=index or
        receipt.get('ready_for_execution') is not False):
        raise ValueError('主控未确认原身份最终分片')
    keys={'accepted','complete','server_id','cycle_id','session_id','manifest_digest','part','ready_for_execution'}
    if receipt['complete']:
        keys|={'digest','duplicate'}
        if receipt.get('digest')!=m['checkpoint_digest'] or type(receipt.get('duplicate')) is not bool:
            raise ValueError('主控未确认原完整检查点')
    if set(receipt)!=keys: raise ValueError('最终分片回执字段无效')
    return receipt
