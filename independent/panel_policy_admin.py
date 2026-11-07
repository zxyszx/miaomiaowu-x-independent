"""Exact administrator session adapter for existing policy Registry methods."""
import re
from core import Error

KINDS=frozenset(('automatic_bandwidth','bandwidth_policy','user_bandwidth','connections_policy','user_connections','temporary_bandwidth','server_bandwidth'))
ID=r'0*[1-9][0-9]*'

def route(path, method):
    if path == '/api/automatic-bandwidth' and method in ('GET','PUT','DELETE'):
        return 'automatic_bandwidth',None,False
    m=re.fullmatch(r'/api/users/('+ID+r')/(automatic-bandwidth|bandwidth-policy|bandwidth|connections-policy|connections|temporary-bandwidth)',path)
    if m:
        uid=int(m[1]); name=m[2]
        allowed={'automatic-bandwidth':('GET','PUT','DELETE'),'bandwidth-policy':('GET',),'bandwidth':('GET','PUT','DELETE'),'connections-policy':('GET',),'connections':('GET','PUT','DELETE'),'temporary-bandwidth':('GET','PUT','DELETE')}[name]
        if method in allowed:
            kind={'automatic-bandwidth':'automatic_bandwidth','bandwidth-policy':'bandwidth_policy','bandwidth':'user_bandwidth','connections-policy':'connections_policy','connections':'user_connections','temporary-bandwidth':'temporary_bandwidth'}[name]
            return kind,uid,False
    m=re.fullmatch(r'/api/servers/('+ID+r')/bandwidth',path)
    if m and method in ('GET','PUT'): return 'server_bandwidth',int(m[1]),False
    return None

def execute(registry, db, kind, ident, method, data):
    if kind not in KINDS: raise Error('策略接口未注册',403)
    value=None if method=='DELETE' else data
    if kind=='automatic_bandwidth': return registry.automatic_bandwidth(ident,value,inherit=method=='DELETE',db=db)
    if kind=='bandwidth_policy': return registry.bandwidth_policy(ident,db=db)
    if kind=='user_bandwidth': return registry.user_bandwidth(ident,value,inherit=method=='DELETE',db=db)
    if kind=='connections_policy': return registry.connections_policy(ident,db=db)
    if kind=='user_connections': return registry.user_connections(ident,value,inherit=method=='DELETE',db=db)
    if kind=='temporary_bandwidth': return registry.temporary_bandwidth(ident,value,remove=method=='DELETE',db=db)
    return registry.server_bandwidth(ident,value,db=db)
