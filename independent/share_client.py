#!/usr/bin/env python3
"""Independent server-share recipient CLI; invitation/access tokens never in argv."""
import argparse
import json
import os
from pathlib import Path
import ssl
import sys
import tempfile
import urllib.request
import urllib.error
from urllib.parse import quote,urlsplit
import uuid


def endpoint(value):
    if not isinstance(value,str) or any(c.isspace() or ord(c)<32 for c in value): raise ValueError('分享来源地址无效')
    url=urlsplit(value)
    if url.scheme not in ('http','https') or not url.hostname or url.username or url.password or url.query or url.fragment or url.path not in ('','/'):
        raise ValueError('分享来源需要完整主机地址，不含路径或凭据')
    if url.scheme!='https' and url.hostname not in ('127.0.0.1','localhost','::1'): raise ValueError('非本机分享来源必须使用HTTPS')
    if url.port is not None and not 1<=url.port<=65535: raise ValueError('分享来源端口无效')
    return value.rstrip('/')


def request(base,token,path,method='GET',data=None):
    base=endpoint(base)
    if not isinstance(token,str) or not 32<=len(token)<=128 or any(c.isspace() or ord(c)<32 for c in token): raise ValueError('分享凭据无效')
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args,**kwargs): return None
    context=ssl.create_default_context(cafile=os.environ.get('SSL_CERT_FILE') or None)
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect(),urllib.request.HTTPSHandler(context=context))
    body=None if data is None else json.dumps(data,allow_nan=False).encode()
    req=urllib.request.Request(base+path,method=method,data=body,headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
    with opener.open(req,timeout=10) as response: raw=response.read(1024*1024+1)
    if len(raw)>1024*1024: raise ValueError('分享响应过大')
    value=json.loads(raw)
    if not isinstance(value,(dict,list)): raise ValueError('分享响应无效')
    return value


def claim(base,token,destination):
    path=Path(destination)
    if path.exists() or path.is_symlink(): raise ValueError('分享配置已存在')
    base=endpoint(base); result=request(base,token,'/share/claim','POST',{})
    if not isinstance(result,dict) or result.get('status')!='active': raise ValueError('分享来源未确认接入')
    access=result.get('access_token'); ident=result.get('id'); sid=result.get('server_id')
    if not isinstance(access,str) or not 32<=len(access)<=128 or type(sid) is not int or sid<1 or str(uuid.UUID(ident))!=ident:
        raise ValueError('分享接入响应无效')
    path=Path(destination); path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    # Refuse overwriting an existing identity after consuming a new invite.
    # main checks this before the request; library callers receive the same rule.
    config={'source':base,'share_id':ident,'server_id':sid,'token':access}
    fd,temporary=tempfile.mkstemp(prefix='.share-',dir=path.parent)
    try:
        with os.fdopen(fd,'w') as handle:
            json.dump(config,handle); handle.flush(); os.fsync(handle.fileno())
        # Link is atomic and does not replace another concurrent claim's file.
        os.link(temporary,path)
    finally: os.unlink(temporary)
    return {key:value for key,value in result.items() if key!='access_token'}


def load(path):
    path=Path(path)
    if path.stat().st_mode & 0o077: raise ValueError('分享配置应仅由当前用户读写（chmod 600）')
    if path.stat().st_size>4096: raise ValueError('分享配置过大')
    value=json.loads(path.read_text())
    if not isinstance(value,dict) or set(value)!={'source','share_id','server_id','token'}: raise ValueError('分享配置无效')
    endpoint(value['source'])
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path,required=True)
    actions=parser.add_subparsers(dest='action',required=True)
    p=actions.add_parser('claim'); p.add_argument('--source',required=True)
    for name in ('status','nodes'): actions.add_parser(name)
    p=actions.add_parser('create'); p.add_argument('--data',type=Path,required=True)
    for name in ('update','delete','authorize','revoke'):
        p=actions.add_parser(name); p.add_argument('--node',type=int,required=True)
        if name=='update': p.add_argument('--data',type=Path,required=True)
        if name in ('authorize','revoke'): p.add_argument('--reference',required=True)
    args=parser.parse_args()
    try:
        if args.action=='claim':
            if args.config.exists() or args.config.is_symlink(): raise ValueError('配置已存在，请使用新的配置文件接入')
            token=sys.stdin.readline(130).strip()
            result=claim(args.source,token,args.config)
        else:
            config=load(args.config); path='/share/status' if args.action=='status' else '/share/nodes'
            method='GET'; data=None
            if args.action in ('create','update'):
                if args.data.stat().st_size>16384: raise ValueError('节点配置过大')
                data=json.loads(args.data.read_text()); method='POST' if args.action=='create' else 'PUT'
            if args.action in ('update','delete','authorize','revoke'):
                if args.node<1: raise ValueError('节点编号无效')
                path+='/'+str(args.node)
            if args.action=='delete': method='DELETE'
            if args.action in ('authorize','revoke'):
                import re
                if not re.fullmatch('[A-Za-z0-9_-]{1,64}',args.reference): raise ValueError('用户标识无效')
                path+='/users/'+quote(args.reference,safe='')
                method='PUT' if args.action=='authorize' else 'DELETE'; data={} if method=='PUT' else None
            result=request(config['source'],config['token'],path,method,data)
        print(json.dumps(result,ensure_ascii=False)); return 0
    except urllib.error.HTTPError as error:
        messages={401:'分享凭据已失效，请由所有者重新邀请。',403:'此操作超出分享允许的范围。',404:'分享或节点不存在。',409:'操作冲突，请检查端口、资源配额或节点状态。'}
        print(messages.get(error.code,'分享来源未完成请求，请稍后重试。'),file=sys.stderr); return 1
    except Exception:
        # No response body, URL, authorization or node configuration in errors.
        print('分享操作未完成，请检查来源连接、凭据、权限和配置。',file=sys.stderr); return 1


if __name__=='__main__': raise SystemExit(main())
