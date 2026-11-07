"""Real main-process IPv4 listening and browser-origin authorization."""
import http.client
import json
import select
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PY=sys.executable
ROOT=Path(__file__).resolve().parent

class HostListenTests(unittest.TestCase):
    def invalid(self,*args):
        with tempfile.TemporaryDirectory() as name:
            data=Path(name)/'unused'
            r=subprocess.run([PY,str(ROOT/'server.py'),'--data',str(data),*args],cwd=ROOT,text=True,capture_output=True,timeout=10)
            self.assertEqual(r.returncode,2,r.stderr)
            self.assertFalse(data.exists(),'rejected arguments must not create runtime state')

    def test_invalid_host_port_and_external_origins(self):
        for host in ('http://127.0.0.1','127.0.0.1:1','bad host','','localhost','::1','999.1.1.1'):
            self.invalid('--host',host)
        for port in ('0','-1','65536','abc'): self.invalid('--port',port)
        for origin in ('http://0.0.0.0:45300','http://user@host','http://@host','http://host:','http://host/','http://host?x=1','http://host#x','http://*.example','http://host:65536','null','http://bad host','ftp://example.com','http://[bad','http://host:0'):
            self.invalid('--external-origin',origin)
        self.invalid('--host','0.0.0.0')

    def test_loopback_test_cannot_bind_public_and_origin_conflict(self):
        self.invalid('--host','0.0.0.0','--panel-loopback-test','--panel-origin','http://127.0.0.1:45300','--panel-control','unused')
        self.invalid('--external-origin','http://one.example','--panel-origin','https://two.example')

    def start(self,root,host=None,origin=None):
        with socket.socket() as s:
            s.bind(('0.0.0.0' if host=='0.0.0.0' else '127.0.0.1',0)); port=s.getsockname()[1]
        args=[PY,str(ROOT/'server.py'),'--data',str(root),'--port',str(port)]
        if host is not None: args+=['--host',host]
        if origin is not None: args+=['--external-origin',origin.format(port=port)]
        process=subprocess.Popen(args,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        self.addCleanup(self.stop,process)
        self.assertTrue(select.select([process.stdout],[],[],10)[0],'main startup timeout')
        line=process.stdout.readline(); self.assertIn('Independent API:',line)
        return process,port,(root/'admin-token').read_text().strip()

    def stop(self,process):
        if process.poll() is None: process.terminate()
        out,err=process.communicate(timeout=10)
        self.assertEqual(process.returncode,0,err)

    def request(self,host,port,path,method='GET',data=None,origin=None,token=None):
        connection=http.client.HTTPConnection(host,port,timeout=5)
        headers={}
        if origin is not None: headers['Origin']=origin
        if token is not None: headers['Authorization']='Bearer '+token
        if data is not None: headers['Content-Type']='application/json'
        connection.request(method,path,json.dumps(data) if data is not None else None,headers)
        response=connection.getresponse(); body=response.read(); result=(response.status,dict(response.getheaders()),body)
        connection.close(); return result

    def exercise(self,host,port,origin,token):
        for path in ('/health','/manage/bandwidth','/manage/plans','/manage/nodes','/manage/servers'):
            result=self.request(host,port,path,origin=origin)
            self.assertEqual(result[0],200,(path,result[2]))
            self.assertNotIn('Access-Control-Allow-Origin',result[1])
        result=self.request(host,port,'/api/users','POST',{'name':'browser-origin-user'},origin,token)
        self.assertEqual(result[0],201,result[2]); user=json.loads(result[2])
        result=self.request(host,port,'/api/users',origin=origin,token=token)
        self.assertEqual(result[0],200,result[2]); self.assertIn(user['id'],[row['id'] for row in json.loads(result[2])])
        for bad in ('http://evil.example','null',origin+'/'):
            self.assertEqual(self.request(host,port,'/api/users','POST',{'name':'denied'},bad,token)[0],403)
        self.assertEqual(self.request(host,port,'/api/users','POST',{'name':'unauth'},origin)[0],401)
        rows=json.loads(self.request(host,port,'/api/users',origin=origin,token=token)[2])
        self.assertEqual([row['name'] for row in rows],['browser-origin-user'])
        connection=http.client.HTTPConnection(host,port,timeout=5)
        connection.putrequest('GET','/health')
        connection.putheader('Origin',origin); connection.putheader('Origin',origin)
        connection.endheaders(); response=connection.getresponse()
        self.assertEqual(response.status,400); response.read(); connection.close()

    def test_default_loopback_browser_write_and_legacy_no_origin(self):
        with tempfile.TemporaryDirectory() as name:
            process,port,token=self.start(Path(name))
            self.exercise('127.0.0.1',port,f'http://127.0.0.1:{port}',token)
            self.assertEqual(self.request('127.0.0.1',port,'/api/users',token=token)[0],200)
            self.stop(process)

    def test_non_loopback_real_main_browser_write_read_and_cross_origin_rejection(self):
        # Discover an assigned non-loopback IPv4 without sending application data.
        with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as s:
            s.connect(('192.0.2.1',9)); address=s.getsockname()[0]
        self.assertFalse(address.startswith('127.'),'non-loopback address required for this evidence')
        with tempfile.TemporaryDirectory() as name:
            process,port,token=self.start(Path(name),'0.0.0.0',f'http://{address}:{{port}}')
            self.exercise(address,port,f'http://{address}:{port}',token)
            self.stop(process)

if __name__=='__main__': unittest.main()
