"""P2a identity endpoints only. No legacy admin business admission."""
import json
import re
import ssl
from urllib.parse import urlsplit

from core import Error

PREFIX='/panel/api/v1/'


def serve(handler):
    transport(handler)
    runtime=getattr(handler.server,'panel_runtime',None)
    if runtime is None or not runtime.running: raise Error('身份运行保护未启动',503)
    return runtime.invoke(lambda:_serve(handler,handler.server.panel_config,runtime.identity))


def transport(handler):
    config=handler.server.panel_config
    if config.test:
        if handler.client_address[0] not in ('127.0.0.1','::1') or handler.server.server_address[0] not in ('127.0.0.1','::1'):
            raise Error('测试身份仅允许loopback',403)
    elif not isinstance(handler.connection,ssl.SSLSocket):
        raise Error('面板身份必须使用TLS',403)


def _body(handler,fields):
    if handler.headers.get_all('Content-Type',[]) != ['application/json']:
        raise Error('需要application/json',415)
    lengths=handler.headers.get_all('Content-Length',[])
    if len(lengths)!=1 or not re.fullmatch('[0-9]+',lengths[0]) or handler.headers.get_all('Transfer-Encoding',[]):
        raise Error('请求体长度无效',400)
    length=int(lengths[0])
    if not 0<length<=16384: raise Error('请求体为空或过大',413)
    handler.connection.settimeout(10)
    raw=handler.rfile.read(length)
    if len(raw)!=length: raise Error('请求体不完整',400)
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise Error('JSON字段重复',400)
            result[key]=value
        return result
    try: value=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError,UnicodeError): raise Error('JSON无效',400)
    if type(value) is not dict or (fields is not None and (set(value)!=set(fields) or any(type(v) is not str for v in value.values()))):
        raise Error('身份请求字段无效',400)
    return value


def _csrf(handler):
    values=handler.headers.get_all('X-CSRF-Token',[])
    if len(values)!=1 or not re.fullmatch('[A-Za-z0-9_-]{43}',values[0]): raise Error('CSRF无效',403)
    return values[0]


def _cookie(handler,config,name,value='',clear=False):
    handler.panel_response_headers.append(('Set-Cookie',f'{name}={value}; Path=/; HttpOnly; SameSite=Strict'
        +('' if config.test else '; Secure')+('; Max-Age=0' if clear else ('; Max-Age=300' if name==config.challenge else ''))))


def _serve(handler,config,kernel):
    path=urlsplit(handler.path)
    if path.query or path.fragment or not path.path.startswith(PREFIX): raise Error('身份接口不存在',404)
    name=path.path[len(PREFIX):]
    routes={'login-challenge':'GET','login':'POST','me':'GET','password':'POST','logout':'POST','revoke-all':'POST','resources':'GET'}
    if name not in routes: raise Error('身份接口不存在',404)
    if handler.command!=routes[name]: raise Error('身份方法不允许',405)
    cookies=handler.panel_cookies
    if name=='login-challenge':
        token,csrf=kernel.challenge(); _cookie(handler,config,config.challenge,token)
        return handler.reply(200,{'csrf_token':csrf})
    if name=='login':
        data=_body(handler,('login','password')); csrf=_csrf(handler)
        if config.session in cookies: raise Error('登录不能携带现有会话',400)
        challenge=cookies.get(config.challenge)
        if challenge is None: raise Error('需要登录挑战',403)
        _cookie(handler,config,config.challenge,clear=True)
        kernel.consume_challenge(challenge,csrf)
        token=kernel.login(data['login'],data['password'],handler.client_address[0])
        _cookie(handler,config,config.session,token)
        return handler.reply(200,{'authenticated':True})
    token=cookies.get(config.session)
    if token is None: raise Error('需要面板会话',401)
    if name=='me': return handler.reply(200,kernel.session_info(token))
    if name=='resources': return handler.reply(200,{'resources':kernel.resources(token)})
    fields={'password':('old_password','new_password'),'revoke-all':('password',),'logout':()}[name]
    data=_body(handler,fields); csrf=_csrf(handler)
    if name=='password': kernel.change_password(token,data['old_password'],data['new_password'],csrf=csrf)
    elif name=='revoke-all': kernel.revoke_all(token,data['password'],csrf=csrf)
    else: kernel.logout(token,csrf=csrf)
    _cookie(handler,config,config.session,clear=True)
    return handler.reply(200,{'completed':True})
