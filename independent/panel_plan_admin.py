"""Exact plan admission; business stays in caller-TX methods owned by02."""
import re

from core import Error
from plan_nodes import PlanNodes
from plan_node_connections import PlanNodeConnections
from plan_node_bandwidth import PlanNodeBandwidth

KINDS=frozenset(('plans','plan_nodes','plan_node_connections','plan_node_bandwidth'))
IDENT=r'0*[1-9][0-9]*'


def route(path,method):
    match=re.fullmatch(r'/api/plans(?:/('+IDENT+r'))?',path)
    if match and method in (('GET','POST') if match[1] is None else ('PUT','DELETE')):
        return 'plans',int(match[1]) if match[1] else None,False
    match=re.fullmatch(r'/api/plans/('+IDENT+r')/nodes',path)
    if match and method in ('GET','PUT','DELETE'):
        return 'plan_nodes',int(match[1]),False
    match=re.fullmatch(r'/api/plans/('+IDENT+r')/nodes/('+IDENT+r')/(connections|bandwidth)',path)
    if match and method in ('GET','PUT','DELETE'):
        return 'plan_node_'+match[3],(int(match[1]),int(match[2])),False
    return None


def execute(registry,db,kind,ident,method,data,versions):
    """Never authenticates or owns the transaction; called after final check."""
    if kind not in KINDS: raise Error('套餐管理接口未注册',403)
    version=versions[0] if versions else None
    if kind=='plans':
        if method=='GET': return 200,registry.plans(versioned=True,db=db)
        if method in ('POST','PUT'):
            if method=='POST' and versions: raise Error('创建套餐不接受编辑版本',400)
            return (201 if method=='POST' else 200),registry.save_plan(
                data,ident,if_match=version,versioned=True,db=db)
        registry.delete_plan(ident,if_match=version,db=db)
        return 200,{'deleted':True}
    if kind=='plan_nodes':
        scope=PlanNodes(registry)
        if method=='GET': return 200,scope.get(ident,db=db)
        if method=='PUT': return 200,scope.save(ident,data,db=db)
        return 200,scope.clear(ident,db=db)
    pid,nid=ident
    if kind=='plan_node_connections':
        return 200,PlanNodeConnections(registry).manage(pid,nid,
            data if method=='PUT' else None,inherit=method=='DELETE',db=db)
    return 200,PlanNodeBandwidth(registry).manage(pid,nid,
        data if method=='PUT' else None,inherit=method=='DELETE',
        if_match=version if method!='GET' else None,versioned=True,db=db)
