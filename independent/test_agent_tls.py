"""Real TLS handshakes and CLI staging, with optional isolated Colima client."""
import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from http.server import ThreadingHTTPServer

from core import Registry
from server import Handler


class AgentTlsTests(unittest.TestCase):
    def setUp(self):
        # Colima shares this repository path; keep only disposable fixtures here.
        directory=Path(__file__).parent/'data'
        directory.mkdir(mode=0o700,parents=True,exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='tls-qa-', dir=directory)
        self.root = Path(self.temp.name).resolve()
        self.addCleanup(self.temp.cleanup)
        self.remote = os.environ.get('SUBNEST_TLS_CROSS_VM') == '1'
        self.host = 'host.lima.internal' if self.remote else 'localhost'
        def openssl(*args):
            subprocess.run(['openssl', *args], cwd=self.root, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                '-subj', '/CN=Disposable Agent QA CA', '-keyout', 'ca.key', '-out', 'ca.pem')
        openssl('req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN='+self.host,
                '-keyout', 'server.key', '-out', 'server.csr')
        (self.root/'extensions').write_text('subjectAltName=DNS:'+self.host+
            '\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n')
        openssl('x509', '-req', '-in', 'server.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key',
                '-CAcreateserial', '-days', '1', '-extfile', 'extensions', '-out', 'server.pem')
        self.hits = []
        hits = self.hits
        class RecordingHandler(Handler):
            def handle_api(handler):
                hits.append(handler.path)
                if handler.path == '/redirect':
                    handler.send_response(302)
                    handler.send_header('Location', '/agent/config')
                    handler.end_headers()
                else:
                    super(RecordingHandler, handler).handle_api()
            do_GET = do_POST = do_PUT = do_DELETE = handle_api
        self.server = ThreadingHTTPServer(('0.0.0.0' if self.remote else '127.0.0.1', 0), RecordingHandler)
        self.server.registry = Registry(self.root/'registry.db')
        self.server.token = 'disposable-admin-fixture'
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.root/'server.pem', self.root/'server.key')
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        def stop():
            self.server.shutdown(); self.server.server_close(); thread.join(5)
        self.addCleanup(stop)
        self.base = f'https://{self.host}:{self.server.server_port}'
        sid = self.server.registry.save('servers', {'name':'TLS fixture'})['id']
        self.token = self.server.registry.provision_agent(sid)['token']

    def child(self, code, trusted=True):
        env = dict(os.environ)
        env.pop('SSL_CERT_FILE', None)
        env.pop('SSL_CERT_DIR', None)
        if trusted:
            env['SSL_CERT_FILE'] = str(self.root/'ca.pem')
        code = ('import sys; sys.path.insert(0, '+repr(str(Path(__file__).parent.resolve()))+');\n'+code)
        if self.remote:
            command = ['colima', 'ssh', '--', 'env',
                       'SSL_CERT_FILE='+str(self.root/('ca.pem' if trusted else 'absent.pem')),
                       'python3', '-c', code]
        else:
            command = [sys.executable, '-c', code]
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_trusted_https_staging_and_retired_session_rejection(self):
        a, b = str(uuid.uuid4()), str(uuid.uuid4())
        self.child(f'''
from agent import begin_session, send, sync
import urllib.error
base, token = {self.base!r}, {self.token!r}
begin_session(base, token, {a!r})
send(base, token, 1, {a!r})
sync(base, token, {str(self.root/'state')!r}, {a!r})
begin_session(base, token, {b!r})
try:
    send(base, token, 100, {a!r})
except urllib.error.HTTPError as error:
    assert error.code == 403
else:
    raise AssertionError('retired session accepted')
send(base, token, 101, {b!r})
sync(base, token, {str(self.root/'state')!r}, {b!r})
''')
        status = self.server.registry.agent_status()[0]
        self.assertTrue(status['online'])
        self.assertIsNotNone(status['staged_revision'])
        self.assertFalse(status['listeners_current'])
        self.assertTrue((self.root/'state'/'staged-config.json').exists())

    def test_untrusted_certificate_rejected_before_http_request(self):
        self.child(f'''
from agent import request
import ssl, urllib.error
try:
    request({self.base!r}, {self.token!r}, '/agent/config')
except urllib.error.URLError as error:
    assert isinstance(error.reason, ssl.SSLCertVerificationError), repr(error.reason)
else:
    raise AssertionError('untrusted certificate accepted')
''', trusted=False)
        self.assertEqual(self.hits, [])
        self.assertFalse(self.server.registry.agent_status()[0]['online'])

    def test_hostname_mismatch_rejected_before_http_request(self):
        # The VM's host alias resolves to this address; the cert has DNS SAN only.
        self.child(f'''
from agent import request
import socket, ssl, urllib.error
address = socket.gethostbyname({self.host!r})
try:
    request('https://'+address+':{self.server.server_port}', {self.token!r}, '/agent/config')
except urllib.error.URLError as error:
    assert isinstance(error.reason, ssl.SSLCertVerificationError), repr(error.reason)
else:
    raise AssertionError('hostname mismatch accepted')
''')
        self.assertEqual(self.hits, [])

    def test_https_redirect_is_not_followed(self):
        self.child(f'''
from agent import request
import urllib.error
try:
    request({self.base!r}, {self.token!r}, '/redirect')
except urllib.error.HTTPError as error:
    assert error.code == 302
else:
    raise AssertionError('redirect followed')
''')
        self.assertEqual(self.hits, ['/redirect'])
