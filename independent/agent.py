#!/usr/bin/env python3
"""Independent heartbeat and configuration staging agent; no command execution."""
import argparse
import json
import os
from pathlib import Path
import socket
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from urllib.parse import urlsplit
from node_config import validate_snapshot
from domain_pool import probe_tls
from agent_liveness import REQUEST_TIMEOUT, heartbeat_receipt, session_receipt

def probe_domain(domain, port):
    """Run the bounded public TLS 1.3/h2 probe on this Agent host."""
    if not isinstance(domain, str) or not domain or type(port) is not int:
        raise ValueError('探测目标无效')
    return probe_tls(domain, port)

def request(base, token, path, data=None):
    url=urlsplit(base)
    if url.scheme not in ('http','https') or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError('主控地址无效')
    if url.scheme!='https' and url.hostname not in ('127.0.0.1','localhost','::1'):
        raise ValueError('非本机主控必须使用 HTTPS')
    body=None if data is None else json.dumps(data,allow_nan=False).encode()
    req=urllib.request.Request(base.rstrip('/')+path,data=body,
        headers={'Content-Type':'application/json','Authorization':'Bearer '+token})
    # Redirects must not forward agent credentials to a different destination.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args,**kwargs): return None
    # Make the CA override explicit for isolated deployments and test fixtures.
    # macOS Python 3.9 does not consistently apply SSL_CERT_FILE through the
    # implicit urllib HTTPS handler, which otherwise makes a trusted private
    # CA look untrusted.  With no override, retain the platform trust store.
    ca_file=os.environ.get('SSL_CERT_FILE')
    https=urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=ca_file or None))
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect(),https)
    with opener.open(req,timeout=REQUEST_TIMEOUT) as response:
        raw=response.read(1024*1024+1)
    if len(raw)>1024*1024: raise ValueError('主控响应过大')
    value=json.loads(raw)
    if not isinstance(value,dict): raise ValueError('主控响应无效')
    return value

def begin_session(base,token,session,server_id=None):
    value=request(base,token,'/agent/session',{'session_id':session})
    if server_id is None:
        if type(value) is not dict or set(value) != {'accepted','server_id','session_id'}:
            raise ValueError('主控未精确确认 Agent 会话身份')
        server_id=value.get('server_id')
    return session_receipt(value,server_id,session)


def send(base, token, uptime, session=None, server_id=None):
    data={'hostname':socket.gethostname(),'uptime':uptime}
    if session is not None: data['session_id']=session
    value=request(base,token,'/agent/heartbeat',data)
    if server_id is None:
        if type(value) is not dict or set(value) != {'accepted','server_id','session_id'}:
            raise ValueError('主控未精确确认 Agent 心跳身份')
        server_id=value.get('server_id')
    return heartbeat_receipt(value,server_id,session)

def sync(base, token, state_dir, session=None):
    snapshot=validate_snapshot(request(base,token,'/agent/config'))
    state_dir=Path(state_dir)
    state_dir.mkdir(mode=0o700,parents=True,exist_ok=True)
    destination=state_dir/'staged-config.json'
    if destination.exists():
        previous=validate_snapshot(json.loads(destination.read_text()))
        if snapshot['server_id']!=previous['server_id']:
            raise ValueError('服务器身份已变化，请使用独立状态目录')
        if snapshot['revision']<previous['revision']:
            raise ValueError('拒绝旧配置版本')
        if snapshot['revision']==previous['revision'] and snapshot['digest']!=previous['digest']:
            raise ValueError('相同配置版本内容不一致')
    temporary=None
    try:
        with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=state_dir,delete=False) as file:
            temporary=Path(file.name)
            json.dump(snapshot,file,ensure_ascii=False,allow_nan=False)
            file.flush();os.fsync(file.fileno())
        temporary.replace(destination)
    finally:
        if temporary is not None: temporary.unlink(missing_ok=True)
    # A staged receipt confirms validated local storage, never a running listener.
    data={'revision':snapshot['revision'],'digest':snapshot['digest']}
    if session is not None: data['session_id']=session
    receipt=request(base,token,'/agent/config/ack',data)
    if receipt.get('accepted') is not True or receipt.get('state')!='staged' or receipt.get('revision')!=snapshot['revision']:
        raise ValueError('主控未确认配置回执')
    return receipt

def probe_pending(base, token):
    job = request(base, token, '/agent/probe-job').get('job')
    if not job: return None
    result = probe_domain(job['domain'], job['port'])
    return request(base, token, '/agent/probe-job/'+job['job_id'], {'lease_token':job['lease_token'],'result':result})

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',type=Path)
    parser.add_argument('--probe-domain')
    parser.add_argument('--probe-port',type=int,default=443)
    parser.add_argument('--once',action='store_true')
    parser.add_argument('--state',type=Path,help='独立 Agent 配置状态目录')
    args=parser.parse_args()
    if args.probe_domain:
        print(json.dumps(probe_domain(args.probe_domain,args.probe_port),ensure_ascii=False),flush=True)
        return 0
    if args.config is None:
        parser.error('--config 是必需的；或使用 --probe-domain')
    config=json.loads(args.config.read_text())
    if not isinstance(config,dict) or set(config)!={'master','token'}:
        raise SystemExit('配置应包含 master 和 token')
    start=time.monotonic()
    session=str(uuid.uuid4())
    while True:
        try:
            begin_session(config['master'],config['token'],session)
            send(config['master'],config['token'],time.monotonic()-start,session)
            sync(config['master'],config['token'],args.state or args.config.resolve().parent/'agent-state',session)
            # Domain probing is optional work, not a prerequisite for staging a
            # current configuration. Expired receipts and network failures must
            # not starve configuration changes or terminate the polling loop.
            try:
                probe_pending(config['master'],config['token'])
            except (OSError,ValueError,TypeError,KeyError) as error:
                if isinstance(error,urllib.error.HTTPError) and error.code in (401,403):
                    raise
                print('Optional domain probe failed; configuration staged; task may retry after lease expiry',
                      file=sys.stderr,flush=True)
            print('Heartbeat accepted; configuration staged (not running)',flush=True)
        except (OSError,ValueError,TypeError):
            print('Agent sync failed; check connectivity, credentials and configuration',file=sys.stderr,flush=True)
            if args.once: return 1
        if args.once: return 0
        time.sleep(15)

if __name__=='__main__': sys.exit(main())
