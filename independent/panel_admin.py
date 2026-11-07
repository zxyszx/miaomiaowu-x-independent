"""Small explicit legacy SQL allowlist, sharing the identity write transaction."""
import re
from urllib.parse import urlsplit

from core import Error
from panel_handlers import _body,_csrf,transport
from panel_identity import hashed


def route(path,method):
    # No prefix/unknown-method admission. Positive IDs preserve legacy dispatch.
    match=re.fullmatch(r'/api/(servers|nodes|users)(?:/(0*[1-9][0-9]*))?',path)
    if match:
        kind,ident=match.groups()
        if method in (('GET','POST') if ident is None else ('PUT','DELETE')):
            return kind,int(ident) if ident else None,False
    match=re.fullmatch(r'/api/manage/servers(?:/(0*[1-9][0-9]*))?',path)
    if match and method in (('GET','POST') if match[1] is None else ('PUT',)):
        return 'servers',int(match[1]) if match[1] else None,True
    match=re.fullmatch(r'/api/quotas(?:/(servers|nodes|users))?',path)
    if match and method==('GET' if match[1] is None else 'PUT'):
        return 'quotas',match[1],False
    from panel_plan_admin import route as plan_route
    selected=plan_route(path,method)
    if selected is not None: return selected
    from panel_policy_admin import route as policy_route
    selected=policy_route(path,method)
    if selected is not None: return selected
    from panel_lifecycle_admin import route as lifecycle_route
    return lifecycle_route(path,method)


def unbind(kernel,db,now,uid,actor):
    bindings=db.execute('SELECT account_id FROM panel_resource_bindings WHERE root_user_id=?',(uid,)).fetchall()
    for binding in bindings:
        ident=binding[0]
        db.execute('DELETE FROM panel_resource_bindings WHERE account_id=?',(ident,))
        kernel._revoke(db,now,ident)
        kernel._audit(db,now,'resource_unbind_delete',actor,ident)
        kernel._hit('admin.unbound')


def serve(handler,*,legacy_delete=False):
    runtime=getattr(handler.server,'panel_runtime',None)
    if runtime is None or not runtime.running: raise Error('身份运行保护未启动',503)
    if not legacy_delete: transport(handler)
    return runtime.invoke(lambda:_serve(handler,runtime.identity,legacy_delete))


def _serve(handler,kernel,legacy_delete):
    parsed=urlsplit(handler.path); selected=route(parsed.path,handler.command)
    if legacy_delete: selected=('users',int(parsed.path.strip('/').split('/')[2]),False)
    cookie=handler.panel_cookies.get(handler.server.panel_config.session)
    # Authenticate even unsupported /api routes so ordinary subjects remain403.
    if not legacy_delete:
        with kernel._tx() as (db,now): kernel._session(db,now,cookie,admin=True)
    if selected is None or (not legacy_delete and (parsed.query or parsed.fragment)):
        raise Error('此管理接口尚未开放面板会话',403)
    kind,ident,managed=selected; method=handler.command
    data=None; csrf=None
    if not legacy_delete and method!='GET':
        csrf=_csrf(handler)
        # Strict JSON for DELETE too (empty object), and field validation stays
        # with the original Registry for each resource/CAS shape.
        data=_body(handler,None)
        if method=='DELETE' and data!={}: raise Error('删除请求不接受额外字段',400)
    versions=handler.headers.get_all('If-Match',[])
    if len(versions)>1: raise Error('配置版本重复',400)
    version=versions[0] if versions else None
    registry=handler.server.registry
    with kernel._tx(True) as (db,now):
        if legacy_delete: actor='legacy_admin'
        else:
            row=kernel._session(db,now,cookie,admin=True)
            if method!='GET': kernel._csrf(db,cookie,csrf)
            actor=row['id']
            if now-row['last_seen']>=60: db.execute('UPDATE panel_sessions SET last_seen=? WHERE id_hash=?',(now,hashed(cookie)))
        kernel._hit('admin.authorized')
        status=200
        from panel_plan_admin import KINDS as PLAN_KINDS,execute as plan_execute
        from panel_policy_admin import KINDS as POLICY_KINDS,execute as policy_execute
        from panel_lifecycle_admin import KINDS as LIFECYCLE_KINDS,execute as lifecycle_execute
        if kind in PLAN_KINDS:
            status,result=plan_execute(registry,db,kind,ident,method,data,versions)
        elif kind in POLICY_KINDS:
            result=policy_execute(registry,db,kind,ident,method,data)
        elif kind in LIFECYCLE_KINDS:
            result=lifecycle_execute(registry,db,kind,ident,method,data)
        elif kind=='quotas':
            if method=='PUT':
                if type(data) is not dict or set(data)!={'limit'}: raise Error('配额请求字段无效',400)
                registry.set_quota(ident,data['limit'],db=db)
            result=registry.quotas(db=db)
        elif method=='GET': result=registry.list(kind,versioned=managed or kind=='nodes',db=db)
        elif method in ('POST','PUT'):
            if method=='POST' and versions: raise Error('创建资源不接受编辑版本',400)
            if managed and method=='PUT' and not versions: raise Error('必须提供单个服务器记录版本',400)
            result=registry.save(kind,data,ident,versioned=managed or kind=='nodes',
                                 if_match=version if managed or kind=='nodes' else None,db=db)
            status=201 if method=='POST' else 200
        else:
            if kind=='users': unbind(kernel,db,now,ident,actor)
            registry.delete(kind,ident,db=db); result={'deleted':True}
        kernel._hit('admin.business_done')
        # Fixed event vocabulary/target excludes body, cookie, UUID and headers.
        kernel._audit(db,now,'admin_'+method.lower()+'_'+kind,actor,str(ident) if ident is not None else None)
    return handler.reply(status,result)
