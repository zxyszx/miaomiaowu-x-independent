"""Immutable isolated multi execution claim; no meter or settlement."""
import time
from core import Error
from multi_agent_installation import checked
from multi_agent_lifecycle import gated,encoded,chain,receipt

class MultiAgentExecution:
    def __init__(self,registry): self.registry=registry

    def record(self,token,data):
        gated(self.registry)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            value,fact=checked(self.registry,db,token,data)
            ident=value['grant_id']; raw=encoded(fact)
            if chain(db,ident,value)!=raw: raise Error('multi执行与原安装不匹配',409)
            lifecycle=db.execute('SELECT state FROM usage_multi_lifecycle WHERE allocation_id=?',
                                 (ident,)).fetchone()
            if lifecycle is None or lifecycle['state'] not in ('installed','execution_recorded'):
                raise Error('multi尚未installed',409)
            old=db.execute('SELECT fact FROM usage_multi_execution WHERE allocation_id=?',
                           (ident,)).fetchone()
            if old is not None:
                if old['fact']!=raw: raise Error('multi执行事实不可变',409)
                return execution_receipt(ident,fact)
            if lifecycle['state']=='execution_recorded': raise Error('multi执行链损坏',409)
            db.execute('INSERT INTO usage_multi_execution VALUES(?,?,?)',(ident,raw,time.time()))
            db.execute('UPDATE usage_multi_lifecycle SET state=?,updated_at=? WHERE allocation_id=?',
                       ('execution_recorded',time.time(),ident))
            return execution_receipt(ident,fact)

def execution_receipt(ident,fact):
    result=receipt(ident,'execution_recorded')
    if 'tail_protocol' in fact:result['execution']=fact
    return result
