"""Real child processes, durable guard, crash/restart and quarantine recovery."""
import json
import http.client
import contextlib
import os
from pathlib import Path
import secrets
import select
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from core import Registry,Error
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        (self.root/'data').mkdir(); self.registry=Registry(self.root/'data'/'r.db')
        self.control=self.root/'control'; self.now=2000000000.
        self.kernel=PanelIdentity(self.registry.path,self.control,clock=lambda:self.now)
        secret=secrets.token_urlsafe(24); self.kernel.initialize('admin',secret)
        self.token=self.kernel.login('admin',secret,'local')

    def tearDown(self): self.temp.cleanup()

    def child(self,code,token=''):
        return subprocess.run([sys.executable,'-c',code,str(self.registry.path),str(self.control)],
                              input=token,text=True,capture_output=True,cwd=Path(__file__).parent,timeout=15)

    START='''import os,sys
from core import Error
from panel_identity import PanelIdentity
from panel_runtime import PanelRuntime
k=PanelIdentity(sys.argv[1],sys.argv[2],clock=lambda:2000000000.)
'''

    def assert_restart_denied(self):
        result=self.child(self.START+'''
try: r=PanelRuntime(k)
except Error as e: sys.exit(72 if e.status==503 else 73)
sys.exit(74)
''')
        self.assertEqual(result.returncode,72,result.stderr)

    def test_continuous_all_anchor_writes_fail_process_exits_and_restart_refuses(self):
        original=(self.control/'anchor.json').read_bytes()
        result=self.child(self.START+'''
r=PanelRuntime(k)
k.clock=lambda:2000001800.
def fail(*args): raise OSError('all anchor writes unavailable')
k._anchor=fail
from http.server import ThreadingHTTPServer
from server import Handler
from panel_authorization import PanelConfig,TEST
import http.client,threading
server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
server.panel_config=PanelConfig('http://127.0.0.1:'+str(server.server_port),loopback_test=True)
server.panel_runtime=r
threading.Thread(target=server.serve_forever,daemon=True).start()
conn=http.client.HTTPConnection('127.0.0.1',server.server_port)
conn.request('GET','/panel/api/v1/me',headers={'Cookie':TEST[0]+'='+sys.stdin.read()})
conn.getresponse()
sys.exit(75)
''',self.token)
        self.assertEqual(result.returncode,78,result.stderr)
        self.assertEqual((self.control/'anchor.json').read_bytes(),original)
        self.assertEqual(json.loads(original)['state'],'clean')
        self.assertTrue((self.control/'running.json').exists())
        self.assert_restart_denied()
        # The same old disk still authenticates in the standalone offline kernel;
        # the HTTP runtime admission guard is what must forbid restart.
        result=self.child(self.START+'k.authenticate(sys.stdin.read())',self.token)
        self.assertEqual(result.returncode,0,result.stderr)

    def test_unclean_crash_cannot_restart_even_with_clean_anchor(self):
        result=self.child(self.START+'r=PanelRuntime(k)\nos._exit(79)')
        self.assertEqual(result.returncode,79); self.assert_restart_denied()

    def test_graceful_restart_preserves_session(self):
        for _ in range(2):
            result=self.child(self.START+'r=PanelRuntime(k)\nk.authenticate(sys.stdin.read())\nr.close()',self.token)
            self.assertEqual(result.returncode,0,result.stderr)
        self.assertFalse((self.control/'running.json').exists())

    def test_quarantine_restore_is_required_and_old_session_never_revives(self):
        backup=self.root/'backup.db'
        with sqlite3.connect(self.registry.path) as src,sqlite3.connect(backup) as dest: src.backup(dest)
        self.assertEqual(self.child(self.START+'r=PanelRuntime(k)\nos._exit(79)').returncode,79)
        self.assert_restart_denied()
        self.kernel.restore(backup,'recovered',secrets.token_urlsafe(24))
        runtime=PanelRuntime(self.kernel)
        try:
            with self.assertRaises(Error) as caught: self.kernel.authenticate(self.token)
            self.assertEqual(caught.exception.status,401)
        finally: runtime.close()

    def test_second_process_is_denied_while_first_holds_guard(self):
        runtime=PanelRuntime(self.kernel)
        try: self.assert_restart_denied()
        finally: runtime.close()

    def test_guard_missing_schema_or_dirty_anchor_never_serves(self):
        with self.kernel._lock():
            anchor=self.kernel._read_anchor(); self.kernel._anchor({**anchor,'state':'dirty'},'test')
        with self.assertRaises(Error) as caught: PanelRuntime(self.kernel)
        self.assertEqual(caught.exception.status,503)
        self.assertFalse((self.control/'running.json').exists())

    def test_symlink_marker_is_rejected(self):
        target=self.root/'elsewhere'; target.write_text('{}'); target.chmod(0o600)
        (self.control/'running.json').symlink_to(target)
        with self.assertRaises(Error) as caught: PanelRuntime(self.kernel)
        self.assertEqual(caught.exception.status,503)
        self.assertEqual(target.read_text(),'{}')

    def test_real_server_main_sigterm_restart_preserves_session_sigkill_blocks(self):
        data=self.root/'main-data'; data.mkdir(); control=self.root/'main-control'
        registry=Registry(data/'registry.db'); kernel=PanelIdentity(registry.path,control)
        secret=secrets.token_urlsafe(24); kernel.initialize('admin',secret)
        token=kernel.login('admin',secret,'local')
        with socket.socket() as probe:
            probe.bind(('127.0.0.1',0)); port=probe.getsockname()[1]
        command=[sys.executable,'server.py','--data',str(data),'--port',str(port),'--panel-control',str(control),
                 '--panel-origin',f'http://127.0.0.1:{port}','--panel-loopback-test']
        for graceful in (True,False):
            child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,cwd=Path(__file__).parent)
            try:
                ready,_,_=select.select([child.stdout],[],[],10)
                self.assertTrue(ready,'server must announce readiness')
                self.assertIn('Independent API:',child.stdout.readline())
                conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                conn.request('GET','/panel/api/v1/me',headers={'Cookie':'mmw_panel_test='+token})
                response=conn.getresponse(); self.assertEqual(response.status,200); response.read(); conn.close()
                child.terminate() if graceful else child.kill()
                out,err=child.communicate(timeout=10)
                self.assertNotIn(token,out+err); self.assertNotIn(secret,out+err)
                if graceful:
                    self.assertEqual(child.returncode,0,err)
                    self.assertFalse((control/'running.json').exists())
                else: self.assertTrue((control/'running.json').exists())
            finally:
                if child.poll() is None: child.kill(); child.communicate()
        result=subprocess.run(command,capture_output=True,text=True,cwd=Path(__file__).parent,timeout=10)
        self.assertNotEqual(result.returncode,0)
        self.assertNotIn('Independent API:',result.stdout)
        self.assertIn('上次运行未安全结束',result.stderr)

    # Boundary observers only; the child calls the real server.main(), server
    # class, Handler, scrypt, identity transactions and production shutdown path.
    SLOW_MAIN='''import sys,threading
import server,panel_handlers,panel_identity,panel_runtime
stage=sys.argv.pop(1)
original_body=panel_handlers._body
original_derive=panel_identity.derive
original_info=panel_identity.PanelIdentity.session_info
original_hit=panel_identity.PanelIdentity._hit
original_worker=server.PanelHTTPServer.process_request_thread
original_close=server.PanelHTTPServer.server_close
original_runtime_close=panel_runtime.PanelRuntime.close
workers=[]
def reached():
    print('REACHED',flush=True)
    if stage!='body':
        if sys.stdin.readline()!='release\\n': raise RuntimeError('boundary release missing')
def body(*args):
    if stage=='body': reached()
    return original_body(*args)
def derive(*args):
    if stage=='kdf-before': reached()
    result=original_derive(*args)
    if stage=='kdf-after': reached()
    return result
def info(*args):
    if stage in ('identity-before','worker-error','interrupted','killed'):
        reached()
        if stage=='worker-error': raise RuntimeError('injected worker failure')
    result=original_info(*args)
    if stage=='identity-after': reached()
    return result
def hit(self,point):
    if stage=='identity-locked' and point=='dirty.before_fsync': reached()
    return original_hit(self,point)
def worker(*args):
    workers.append(threading.current_thread())
    try: return original_worker(*args)
    finally: print('WORKER_DONE',flush=True)
def close(*args):
    print('DRAINING',flush=True)
    return original_close(*args)
def cleanup(*args):
    assert workers and not any(t.is_alive() for t in workers)
    print('CLEANUP',flush=True)
    return original_runtime_close(*args)
panel_handlers._body=body
panel_identity.PanelIdentity.session_info=info
panel_identity.PanelIdentity._hit=hit
server.PanelHTTPServer.process_request_thread=worker
server.PanelHTTPServer.server_close=close
panel_runtime.PanelRuntime.close=cleanup
# Do not intercept constructor's KDF self-test: wait for actual requests.
original_serve=server.PanelHTTPServer.serve_forever
def serve(*args,**kwargs):
    panel_identity.derive=derive
    return original_serve(*args,**kwargs)
server.PanelHTTPServer.serve_forever=serve
server.main()
'''

    @contextlib.contextmanager
    def slow_main(self,stage):
        directory=tempfile.TemporaryDirectory(dir=self.root)
        root=Path(directory.name); data=root/'data'; data.mkdir(); control=root/'control'
        registry=Registry(data/'registry.db'); kernel=PanelIdentity(registry.path,control)
        secret=secrets.token_urlsafe(24); kernel.initialize('admin',secret)
        token=kernel.login('admin',secret,'fixture'); challenge,csrf=kernel.challenge()
        with socket.socket() as probe:
            probe.bind(('127.0.0.1',0)); port=probe.getsockname()[1]
        args=['--data',str(data),'--port',str(port),'--panel-control',str(control),
              '--panel-origin',f'http://127.0.0.1:{port}','--panel-loopback-test']
        child=subprocess.Popen([sys.executable,'-u','-c',self.SLOW_MAIN,stage,*args],stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1,cwd=Path(__file__).parent)
        # Binary, unbuffered event reads avoid TextIO's read-ahead defeating select.
        events=[]
        def event(expected):
            deadline=time.monotonic()+10; raw=bytearray()
            while time.monotonic()<deadline:
                ready,_,_=select.select([child.stdout.fileno()],[],[],max(0,deadline-time.monotonic()))
                if not ready: break
                value=os.read(child.stdout.fileno(),1)
                if not value: break
                raw.extend(value)
                if value==b'\n':
                    line=raw.decode().strip(); raw.clear(); events.append(line)
                    if expected in line: return
            self.fail('missing child boundary event: '+expected+'; events='+repr(events))
        conn=None
        try:
            event('Independent API:')
            conn=http.client.HTTPConnection('127.0.0.1',port,timeout=10)
            login=stage in ('body','kdf-before','kdf-after')
            payload=json.dumps({'login':'admin','password':secret}).encode() if login else None
            if login:
                conn.putrequest('POST','/panel/api/v1/login')
                for key,value in [('Origin',f'http://127.0.0.1:{port}'),('Cookie','mmw_panel_test_challenge='+challenge),
                    ('X-CSRF-Token',csrf),('Content-Type','application/json'),('Content-Length',str(len(payload)))]: conn.putheader(key,value)
                conn.endheaders()
                conn.send(payload[:1] if stage=='body' else payload)
            else: conn.request('GET','/panel/api/v1/me',headers={'Cookie':'mmw_panel_test='+token})
            event('REACHED')
            yield child,conn,control,token,args,event,events,payload
        finally:
            if conn: conn.close()
            if child.poll() is None: child.kill()
            out,err=child.communicate(timeout=10)
            self.assertNotIn(token,out+err); self.assertNotIn(secret,out+err)
            directory.cleanup()

    def restart_main(self,args,token,*,denied=False):
        command=[sys.executable,'server.py',*args]
        if denied:
            result=subprocess.run(command,capture_output=True,text=True,cwd=Path(__file__).parent,timeout=10)
            self.assertNotEqual(result.returncode,0)
            self.assertNotIn('Independent API:',result.stdout)
            self.assertIn('上次运行未安全结束',result.stderr)
            return
        child=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,cwd=Path(__file__).parent)
        try:
            ready,_,_=select.select([child.stdout],[],[],10); self.assertTrue(ready)
            self.assertIn('Independent API:',child.stdout.readline())
            port=int(args[args.index('--port')+1])
            conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
            conn.request('GET','/panel/api/v1/me',headers={'Cookie':'mmw_panel_test='+token})
            response=conn.getresponse(); self.assertEqual(response.status,200); response.read(); conn.close()
            child.terminate(); out,err=child.communicate(timeout=10); self.assertEqual(child.returncode,0,err)
        finally:
            if child.poll() is None: child.kill(); child.communicate()

    def test_real_main_sigterm_drains_body_kdf_and_identity_boundaries(self):
        for stage in ('body','kdf-before','kdf-after','identity-before','identity-locked','identity-after'):
            with self.subTest(stage=stage),self.slow_main(stage) as values:
                child,conn,control,token,args,event,events,payload=values
                marker=control/'running.json'; original=marker.read_bytes()
                child.terminate(); event('DRAINING')
                self.assertIsNone(child.poll()); self.assertEqual(marker.read_bytes(),original)
                self.assertNotIn('WORKER_DONE',events); self.assertNotIn('CLEANUP',events)
                # Once draining, listener closes and no new request is admitted.
                if stage=='body': conn.send(payload[1:])
                else: child.stdin.write('release\n'); child.stdin.flush()
                response=conn.getresponse(); self.assertEqual(response.status,200); response.read()
                event('CLEANUP')
                self.assertLess(events.index('WORKER_DONE'),events.index('CLEANUP'))
                child.wait(timeout=10); self.assertEqual(child.returncode,0)
                self.assertFalse(marker.exists())
                self.restart_main(args,token)

    def test_real_main_interrupted_drain_or_sigkill_retains_marker(self):
        for stage in ('interrupted','killed'):
            with self.subTest(stage=stage),self.slow_main(stage) as values:
                child,conn,control,token,args,event,events,payload=values
                marker=control/'running.json'; original=marker.read_bytes()
                child.terminate(); event('DRAINING'); self.assertEqual(marker.read_bytes(),original)
                child.terminate() if stage=='interrupted' else child.kill()
                child.wait(timeout=10)
                self.assertEqual(child.returncode,78 if stage=='interrupted' else -9)
                self.assertEqual(marker.read_bytes(),original)
                self.assertNotIn('CLEANUP',events); self.restart_main(args,token,denied=True)

    def test_real_main_worker_exception_retains_marker_and_rejects_restart(self):
        with self.slow_main('worker-error') as values:
            child,conn,control,token,args,event,events,payload=values
            original=(control/'running.json').read_bytes()
            child.terminate(); event('DRAINING')
            child.stdin.write('release\n'); child.stdin.flush()
            child.wait(timeout=10); self.assertEqual(child.returncode,78)
            self.assertEqual((control/'running.json').read_bytes(),original)
            self.assertNotIn('CLEANUP',events); self.restart_main(args,token,denied=True)
