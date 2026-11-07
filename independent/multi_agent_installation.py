"""Multi installation/applied, isolated from all first-only journals."""
import uuid
from core import Error
from multi_agent_lifecycle import KIND,encoded,gated,identity,receipt

def fact_for(value,execution_id):
    if type(execution_id) is not str or str(uuid.UUID(execution_id))!=execution_id:
        raise Error('multi执行编号无效')
    from multi_agent_test_runtime import configuration
    from node_config import digest
    fact=dict(ledger_kind=KIND,ledger_id=value['grant_id'],execution_id=execution_id,
        cycle_id=value['cycle_id'],policy_id=value['policy_id'],server_id=value['server_id'],
        session_id=value['session_id'],node_id=value['node_id'],
        user_id=value['client']['user_id'],credential=value['client']['uuid'],
        configuration_digest=value['configuration_digest'],binding_digest=value['digest'],
        reserved_bytes=value['reserved_bytes'],baseline_bytes=value['baseline_bytes'],
        revision=value['revision'],issued_at=value['issued_at'],
        execute_until=value['valid_until'],settle_until=value['valid_until'],
        runtime_digest=digest(configuration(value)),protocol='vless')
    if 'tail_authority' in value:
        a=value['tail_authority']
        if execution_id!=a['execution_id']:raise Error('tail执行编号不可更换',409)
        fact.update(tail_protocol=a['schema'],authority_digest=a['body_digest'],
                    tail_binding_digest=a['body_digest'],settle_until=a['settle_until'])
    return fact

def checked(registry,db,token,data):
    if type(data) is not dict or set(data)!={'fact'} or type(data['fact']) is not dict:
        raise Error('multi事实字段无效')
    fact=data['fact']
    if not {'ledger_kind','ledger_id','session_id','binding_digest','execution_id'}<=set(fact):
        raise Error('multi事实身份缺失')
    value,_=identity(registry,db,token,fact['ledger_kind'],fact['ledger_id'],
                     fact['session_id'],fact['binding_digest'])
    expected=fact_for(value,fact['execution_id'])
    if encoded(fact)!=encoded(expected): raise Error('multi事实与绑定不匹配',409)
    return value,expected

class MultiAgentInstallation:
    def __init__(self,registry): self.registry=registry

    def accept(self,token,data):
        gated(self.registry)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            value,fact=checked(self.registry,db,token,data)
            ident=value['grant_id']; raw=encoded(fact)
            lifecycle=db.execute('SELECT state FROM usage_multi_lifecycle WHERE allocation_id=?',
                                 (ident,)).fetchone()
            if lifecycle is None or lifecycle['state'] not in ('installing','installed','execution_recorded'):
                raise Error('multi尚未处于安装状态',409)
            old=db.execute('SELECT fact FROM usage_multi_installations WHERE allocation_id=?',
                           (ident,)).fetchone()
            applied=db.execute('SELECT * FROM usage_multi_applied WHERE allocation_id=?',
                               (ident,)).fetchone()
            if old is not None:
                from multi_agent_lifecycle import chain
                if old['fact']!=raw: raise Error('multi安装事实不可变',409)
                chain(db,ident,value)
                return installation_receipt(ident,fact)
            if applied is not None: raise Error('multi安装链损坏',409)
            db.execute('INSERT INTO usage_multi_installations VALUES(?,?)',(ident,raw))
            db.execute('INSERT INTO usage_multi_applied VALUES(?,?,?,?)',
                       (ident,value['revision'],value['digest'],value['valid_until']))
            return installation_receipt(ident,fact)

def installation_receipt(ident,fact):
    result=receipt(ident,'installation_recorded')
    if 'tail_protocol' in fact:result['installation']=fact
    return result
