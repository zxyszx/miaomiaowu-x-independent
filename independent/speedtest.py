"""Bounded local/loopback VLESS download measurement, independent of vendor UI."""
import asyncio
import base64
from contextlib import asynccontextmanager
import ipaddress
import json
import os
from pathlib import Path
import socket
import ssl
import threading
import tempfile
import time
from urllib.parse import urlsplit
import uuid

from core import Error
from domain_pool import fields, _public_address

DNS_SLOTS=threading.BoundedSemaphore(2)


class TunnelTLS:
    """TLS over an already connected tunnel; strip VLESS reply before TLS input.

    Xray can defer its VLESS reply until the target responds. Send ClientHello
    first rather than waiting for that reply before starting target TLS.
    """
    def __init__(self,reader,writer,context,hostname,vless=False):
        self.reader=reader; self.writer=writer; self.prefix=vless
        self.incoming=ssl.MemoryBIO(); self.outgoing=ssl.MemoryBIO()
        self.tls=context.wrap_bio(self.incoming,self.outgoing,server_side=False,server_hostname=hostname)
        self.buffer=b''

    async def flush(self):
        while data:=self.outgoing.read(): self.writer.write(data)
        await self.writer.drain()

    async def receive(self):
        if self.prefix:
            version,extra=await self.reader.readexactly(2)
            if version!=0: raise Error('invalid_vless_response')
            if extra: await self.reader.readexactly(extra)
            self.prefix=False
        data=await self.reader.read(65536)
        if not data: raise Error('incomplete_download')
        self.incoming.write(data)

    async def handshake(self):
        while True:
            try:
                self.tls.do_handshake(); await self.flush(); return
            except ssl.SSLWantReadError:
                await self.flush(); await self.receive()

    async def write(self,data):
        while data:
            try: data=data[self.tls.write(data):]
            except ssl.SSLWantReadError: await self.flush(); await self.receive()
            await self.flush()

    async def read(self,size):
        if self.buffer:
            data,self.buffer=self.buffer[:size],self.buffer[size:]
            return data
        while True:
            try: return self.tls.read(size)
            except ssl.SSLWantReadError: await self.flush(); await self.receive()

    async def headers(self):
        data=b''
        while b'\r\n\r\n' not in data:
            chunk=await self.read(4096)
            if not chunk: raise Error('incomplete_download')
            data+=chunk
            if len(data)>16384: raise Error('invalid_http_response')
        head,tail=data.split(b'\r\n\r\n',1)
        self.buffer=tail+self.buffer
        return head+b'\r\n\r\n'


def options(data):
    if not isinstance(data,dict) or set(data) not in (
            {'url','threads','size_mib','mode'}, {'url','threads','size_mib','mode','node_id','user_id'}):
        raise Error('需要测速地址、来源、并发数和下载量')
    try:
        url=urlsplit(data['url'])
        port=url.port or 443
        if (url.scheme!='https' or not url.hostname or url.username or url.password or url.fragment
                or any(ord(c)<33 or ord(c)>126 for c in data['url']) or len(data['url'])>2048):
            raise ValueError()
        fields(dict(domain=url.hostname,port=port,enabled=True))
    except (TypeError,ValueError,AttributeError): raise Error('测速地址应为不含凭据的 HTTPS 域名地址') from None
    if type(data['threads']) is not int or data['threads'] not in (1,8,16,32,64): raise Error('并发数应为1、8、16、32或64')
    if type(data['size_mib']) is not int or data['size_mib'] not in (1,4,8,16): raise Error('下载总量应为1、4、8或16 MiB')
    if data['mode'] not in ('local','node'): raise Error('测速来源无效')
    if data['mode']=='local' and set(data)!={'url','threads','size_mib','mode'}: raise Error('本机测速不能带节点凭据')
    if data['mode']=='node' and any(type(data.get(k)) is not int or data[k]<1 for k in ('node_id','user_id')):
        raise Error('节点测速需要有效节点和已授权用户')
    return dict(data)


async def resolve(host,port,deadline):
    # DNS may hang in the OS. Keep those workers bounded even after a task times out.
    if not DNS_SLOTS.acquire(blocking=False): raise Error('DNS测速任务繁忙',503)
    done=threading.Event(); result=[]
    def worker():
        try: result.append(socket.getaddrinfo(host,port,type=socket.SOCK_STREAM))
        except OSError: result.append(None)
        finally: DNS_SLOTS.release(); done.set()
    try: threading.Thread(target=worker,daemon=True).start()
    except BaseException: DNS_SLOTS.release(); raise
    while not done.is_set():
        if time.monotonic()>=deadline: raise TimeoutError()
        await asyncio.sleep(.01)
    if not result[0]: raise Error('dns_error')
    if any(not _public_address(row[4][0]) for row in result[0]): raise Error('non_public_address')
    return result[0][0][4][0]


class SpeedTests:
    def __init__(self,registry,*,local_server_id=None,xray_binary=None,resolver=resolve,context=None,timeout=30):
        self.registry=registry; self.local_server_id=local_server_id
        self.xray_binary=Path(xray_binary) if xray_binary else None
        self.resolver=resolver; self.context=context or ssl.create_default_context()
        self.context.set_alpn_protocols(['http/1.1'])
        self.timeout=timeout; self.lock=threading.Lock(); self.active={}; self.closed=False
        with registry.connect() as db:
            db.execute('CREATE TABLE IF NOT EXISTS speed_tests(id TEXT PRIMARY KEY, settings TEXT NOT NULL, status TEXT NOT NULL, created_at REAL NOT NULL, finished_at REAL, result TEXT)')
            db.execute("UPDATE speed_tests SET status='interrupted',finished_at=?,result=? WHERE status='running'",
                       (time.time(),json.dumps({'error':'service_restarted'})))

    def get(self,ident=None):
        with self.registry.connect() as db:
            rows=db.execute('SELECT * FROM speed_tests'+(' WHERE id=?' if ident else ' ORDER BY created_at DESC LIMIT 100'),(ident,) if ident else ()).fetchall()
        if ident and not rows: raise Error('测速记录不存在',404)
        values=[dict(id=r['id'],settings=json.loads(r['settings']),status=r['status'],created_at=r['created_at'],
                     finished_at=r['finished_at'],result=json.loads(r['result']) if r['result'] else None) for r in rows]
        return values[0] if ident else values

    def node(self,settings):
        if settings['mode']=='local': return None
        with self.registry.connect() as db:
            row=db.execute('SELECT n.server_id,c.payload,u.credential FROM nodes n JOIN node_configs c ON c.node_id=n.id JOIN node_users u ON u.node_id=n.id WHERE n.id=? AND u.user_id=?',
                           (settings['node_id'],settings['user_id'])).fetchone()
        if not row: raise Error('节点不存在或用户未授权',404)
        config=json.loads(row['payload'])
        if self.local_server_id is None or row['server_id']!=self.local_server_id:
            raise Error('此测速端未绑定该节点服务器',409)
        if config['protocol']!='vless' or not config['enabled']:
            raise Error('当前测速端仅支持启用的本机 VLESS 节点',409)
        if 'reality' in config and (not self.xray_binary or not self.xray_binary.is_file()):
            raise Error('REALITY 测速需要配置本机 Xray 客户端',409)
        host=config['listen_host']
        if host in ('0.0.0.0','::') and 'reality' in config: host='127.0.0.1' if host=='0.0.0.0' else '::1'
        if not ipaddress.ip_address(host).is_loopback: raise Error('节点不是本机回环入口',409)
        return dict(host=host,port=config['listen_port'],credential=row['credential'],reality=config.get('reality'))

    @asynccontextmanager
    async def client(self,route,address,port):
        if not route or not route.get('reality'):
            yield route; return
        # Derive only a public key; private key travels via stdin, never argv/logs.
        reality=route['reality']; process=None
        with tempfile.TemporaryDirectory(prefix='independent-speed-') as directory:
            try:
                derive=await asyncio.create_subprocess_exec('openssl','pkey','-inform','DER','-pubout','-outform','DER',
                    stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL)
                try:
                    public,_=await derive.communicate(bytes.fromhex('302e020100300506032b656e04220420')+
                        base64.urlsafe_b64decode(reality['private_key']+'='))
                finally:
                    if derive.returncode is None: derive.kill(); await derive.wait()
                if derive.returncode or len(public)!=44: raise Error('reality_public_key_failed')
                password=base64.urlsafe_b64encode(public[-32:]).decode().rstrip('=')
                with socket.socket() as reserved:
                    reserved.bind(('127.0.0.1',0)); listen=reserved.getsockname()[1]
                config={'log':{'loglevel':'none'},'inbounds':[{'listen':'127.0.0.1','port':listen,
                    'protocol':'dokodemo-door','settings':{'address':address,'port':port,'network':'tcp'}}],
                    'outbounds':[{'protocol':'vless','settings':{'vnext':[{'address':route['host'],'port':route['port'],
                        'users':[{'id':route['credential'],'encryption':'none'}]}]},
                        'streamSettings':{'network':'raw','security':'reality','realitySettings':{
                            'serverName':reality['server_name'],'fingerprint':'chrome','password':password,
                            'shortId':reality['short_id']}}}]}
                path=Path(directory)/'client.json'
                with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as stream:
                    json.dump(config,stream)
                process=await asyncio.create_subprocess_exec(str(self.xray_binary),'run','-config',str(path),
                    stdout=asyncio.subprocess.DEVNULL,stderr=asyncio.subprocess.DEVNULL)
                for _ in range(60):
                    if process.returncode is not None: raise Error('reality_client_failed')
                    try:
                        _,ready=await asyncio.open_connection('127.0.0.1',listen)
                        ready.close(); await ready.wait_closed(); break
                    except OSError: await asyncio.sleep(.025)
                else: raise Error('reality_client_timeout')
                yield dict(host='127.0.0.1',port=listen,bridge=True)
            finally:
                if process and process.returncode is None:
                    try: process.terminate()
                    except ProcessLookupError: pass
                    try: await asyncio.wait_for(process.wait(),2)
                    except TimeoutError:
                        try: process.kill()
                        except ProcessLookupError: pass
                        await process.wait()

    def start(self,data):
        settings=options(data)
        self.node(settings)  # Reject unsupported sources before consuming a job slot.
        ident=uuid.uuid4().hex
        with self.lock:
            if self.closed: raise Error('测速服务已停止',503)
            if len(self.active)>=2: raise Error('已有两个测速任务，请等待或取消',409)
            with self.registry.connect() as db:
                db.execute('INSERT INTO speed_tests(id,settings,status,created_at) VALUES(?,?,?,?)',
                           (ident,json.dumps(settings),'running',time.time()))
            entry={'loop':None,'task':None,'cancel':False}
            thread=threading.Thread(target=self._worker,args=(ident,settings,entry),daemon=True)
            entry['thread']=thread; self.active[ident]=entry
            try: thread.start()
            except BaseException:
                self.active.pop(ident); self._finish(ident,'failed',{'error':'worker_start_failed'}); raise
        return self.get(ident)

    def _finish(self,ident,status,result):
        with self.registry.connect() as db:
            db.execute("UPDATE speed_tests SET status=?,finished_at=?,result=? WHERE id=? AND status='running'",
                       (status,time.time(),json.dumps(result,allow_nan=False),ident))

    def cancel(self,ident):
        self.get(ident)
        with self.lock:
            entry=self.active.get(ident)
            if entry:
                entry['cancel']=True
                self._finish(ident,'cancelled',{'error':'cancelled'})
                if entry['task'] is not None: entry['loop'].call_soon_threadsafe(entry['task'].cancel)
        return self.get(ident)

    def close(self):
        with self.lock: self.closed=True; entries=list(self.active.items())
        for ident,_ in entries: self.cancel(ident)
        for _,entry in entries: entry['thread'].join(2)

    def _worker(self,ident,settings,entry):
        loop=asyncio.new_event_loop(); asyncio.set_event_loop(loop)
        try:
            with self.lock:
                task=loop.create_task(self.measure(settings)); entry.update(loop=loop,task=task)
                if entry['cancel']: task.cancel()
            try:
                result=loop.run_until_complete(asyncio.wait_for(task,self.timeout))
                self._finish(ident,'completed',result)
            except asyncio.CancelledError: self._finish(ident,'cancelled',{'error':'cancelled'})
            except (TimeoutError,asyncio.TimeoutError): self._finish(ident,'failed',{'error':'timeout'})
            except ssl.SSLCertVerificationError: self._finish(ident,'failed',{'error':'certificate_error'})
            except Error as error: self._finish(ident,'failed',{'error':str(error)})
            except (OSError,ssl.SSLError,asyncio.IncompleteReadError): self._finish(ident,'failed',{'error':'connection_error'})
            except Exception: self._finish(ident,'failed',{'error':'measurement_failed'})
        finally:
            pending=asyncio.all_tasks(loop)
            for task in pending: task.cancel()
            if pending: loop.run_until_complete(asyncio.gather(*pending,return_exceptions=True))
            with self.lock: self.active.pop(ident,None)
            loop.close()

    async def measure(self,settings):
        started=time.monotonic(); url=urlsplit(settings['url']); port=url.port or 443
        address=await self.resolver(url.hostname,port,started+self.timeout)
        route=self.node(settings)  # Recheck authorization after DNS, not just at task creation.
        total=settings['size_mib']*1024*1024
        sizes=[total//settings['threads']+(i<total%settings['threads']) for i in range(settings['threads'])]
        first=[]
        async def download(size):
            writer=None
            try:
                if route:
                    reader,writer=await asyncio.open_connection(route['host'],route['port'])
                    if not route.get('bridge'):
                        ip=ipaddress.ip_address(address)
                        writer.write(b'\0'+uuid.UUID(route['credential']).bytes+b'\0\1'+port.to_bytes(2,'big')+
                                     (b'\1' if ip.version==4 else b'\3')+ip.packed)
                        await writer.drain()
                else:
                    reader,writer=await asyncio.open_connection(address,port)
                secured=TunnelTLS(reader,writer,self.context,url.hostname,vless=bool(route and not route.get('bridge')))
                await secured.handshake()
                path=url.path or '/'
                if url.query: path+='?'+url.query
                host=url.hostname if port==443 else f'{url.hostname}:{port}'
                await secured.write(f'GET {path} HTTP/1.1\r\nHost: {host}\r\nRange: bytes=0-{size-1}\r\nAccept-Encoding: identity\r\nConnection: close\r\n\r\n'.encode('ascii'))
                header=await secured.headers()
                if len(header)>16384: raise Error('invalid_http_response')
                lines=header.decode('iso-8859-1').split('\r\n')
                status=lines[0].split(' ')
                if len(status)<2 or status[1] not in ('200','206'): raise Error('http_status_'+(status[1] if len(status)>1 else 'invalid'))
                headers={}
                for line in lines[1:]:
                    if line:
                        key,sep,value=line.partition(':')
                        if not sep or key.lower() in headers: raise Error('invalid_http_response')
                        headers[key.lower()]=value.strip()
                if ('transfer-encoding' in headers or headers.get('content-encoding','identity').lower()!='identity'):
                    raise Error('unsupported_response_encoding')
                try: length=int(headers['content-length'])
                except (KeyError,ValueError): raise Error('missing_content_length') from None
                if length<size: raise Error('response_too_small')
                if status[1]=='206' and not headers.get('content-range','').startswith(f'bytes 0-{size-1}/'):
                    raise Error('invalid_content_range')
                remaining=size
                while remaining:
                    data=await secured.read(min(65536,remaining))
                    if not data: raise Error('incomplete_download')
                    if not first: first.append(time.monotonic())
                    remaining-=len(data)
                return size
            finally:
                if writer:
                    writer.close()
                    try: await asyncio.wait_for(writer.wait_closed(),.5)
                    except (OSError,TimeoutError): pass
        async with self.client(route,address,port) as route:
            tasks=[asyncio.create_task(download(size)) for size in sizes]
            try: counts=await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    if not task.done(): task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
        elapsed=time.monotonic()-started
        return {'bytes':sum(counts),'elapsed_seconds':round(elapsed,6),
                'download_bps':round(sum(counts)/max(elapsed,.000001),3),
                'ttfb_ms':round((first[0]-started)*1000,3),'source':settings['mode']}
