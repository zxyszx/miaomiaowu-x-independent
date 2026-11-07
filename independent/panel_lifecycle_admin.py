"""Exact admin adapter for DEV-039 assignment and lifecycle callers."""
import re
from core import Error

KINDS=frozenset(('user_plan','plan_lifecycle'))
ID=r'0*[1-9][0-9]*'

def route(path,method):
    m=re.fullmatch(r'/api/users/('+ID+r')/(plan|plan-lifecycle)',path)
    if not m: return None
    name=m[2]; allowed=('PUT',) if name=='plan' else ('GET','PUT')
    if method not in allowed: return None
    return ('user_plan' if name=='plan' else 'plan_lifecycle'),int(m[1]),False

def execute(registry,db,kind,ident,method,data):
    if kind=='user_plan': return registry.assign_plan(ident,data,db=db)
    from plan_lifecycle import Lifecycle
    return Lifecycle(registry).manage(ident,data if method=='PUT' else None,db=db)
