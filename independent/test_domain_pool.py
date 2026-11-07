import concurrent.futures
from contextlib import contextmanager
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.error

from core import Error, Registry
from domain_pool import DomainPool, fields, probe_tls, _tls_probe, _public_address
import test_core


def available(latency=12):
    return dict(ok=True, error=None, tls_version='TLSv1.3', alpn='h2', latency_ms=latency)


class DomainPoolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'registry.db'
        self.pool = DomainPool(Registry(self.path))

    def tearDown(self):
        self.temp.cleanup()

    def add(self, domain='fixture.example'):
        return self.pool.save(dict(domain=domain, port=443, enabled=True))

    def test_validation_normalization_and_duplicate(self):
        self.assertEqual(self.add('FIXTURE.Example.')['domain'], 'fixture.example')
        self.assertEqual(fields(dict(domain='例子.测试', port=443, enabled=True))['domain'], 'xn--fsqu00a.xn--0zwm56d')
        with self.assertRaises(Error) as caught:
            self.add()
        self.assertEqual(caught.exception.status, 409)
        for domain in ('https://a.example', '*.example', 'a.example:443', '127.0.0.1', 'a/b.example', 'a.example..',
                       'a..example', 'localhost', ' a.example', '-bad.example', 'a' * 64 + '.example'):
            with self.subTest(domain=domain), self.assertRaises(Error): self.add(domain)
        for change in ({'port': True}, {'port': 0}, {'enabled': 1}, {'extra': 'x'}):
            with self.subTest(change=change), self.assertRaises(Error):
                self.pool.save(dict(domain='a.example', port=443, enabled=True, **{}) | change)

    def test_restart_idempotence_edit_invalidation_and_delete(self):
        item = self.add()
        with mock.patch('domain_pool.probe_tls', return_value=available()): self.pool.probe(item['id'])
        restored = DomainPool(Registry(self.path))
        saved = restored.list()[0]
        self.assertEqual(restored.save(dict(domain='fixture.example', port=443, enabled=True), item['id']), saved)
        self.assertEqual(restored.select()['id'], item['id'])
        changed = restored.save(dict(domain='fixture.example', port=8443, enabled=True), item['id'])
        self.assertIsNone(changed['result'])
        self.assertEqual(changed['status'], 'not_checked')
        with self.assertRaises(Error): restored.select()
        restored.delete(item['id'])
        with self.assertRaises(Error): restored.delete(item['id'])
        with self.assertRaises(Error): restored.probe(item['id'])

    def test_concurrent_duplicate_only_creates_one_record(self):
        def create(_):
            try: self.add(); return True
            except Error as error:
                self.assertEqual(error.status, 409)
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            self.assertEqual(sum(pool.map(create, range(16))), 1)

    def test_selection_excludes_disabled_failed_expired_and_future_results(self):
        first, second = self.add('first.example'), self.add('second.example')
        with mock.patch('domain_pool.probe_tls', return_value=available(30)): self.pool.probe(first['id'])
        with mock.patch('domain_pool.probe_tls', return_value=available(10)): self.pool.probe(second['id'])
        self.assertEqual(self.pool.select()['id'], second['id'])
        self.pool.save(dict(domain='second.example', port=443, enabled=False), second['id'])
        with self.assertRaises(Error): self.pool.probe(second['id'])
        self.assertEqual(self.pool.select()['id'], first['id'])
        checked = self.pool.list()[0]['checked_at']
        for instant in (checked + 301, checked - 1):
            with mock.patch('domain_pool.time.time', return_value=instant), self.assertRaises(Error): self.pool.select()
        with mock.patch('domain_pool.probe_tls', return_value={'ok': False, 'error': 'timeout'}):
            self.assertEqual(self.pool.probe(first['id'])['status'], 'unavailable')
        with self.assertRaises(Error): self.pool.select()

    def test_old_probe_cannot_overwrite_edit_delete_or_newer_probe(self):
        for change in ('edit', 'delete', 'newer'):
            with self.subTest(change=change):
                item = self.add(change + '.example')
                def old_result(*_):
                    if change == 'edit': self.pool.save(dict(domain='changed.example', port=443, enabled=True), item['id'])
                    elif change == 'delete': self.pool.delete(item['id'])
                    else:
                        with mock.patch('domain_pool.probe_tls', return_value={'ok': False, 'error': 'dns_error'}):
                            self.pool.probe(item['id'])
                    return available()
                with mock.patch('domain_pool.probe_tls', side_effect=old_result), self.assertRaises(Error) as caught:
                    self.pool.probe(item['id'])
                self.assertEqual(caught.exception.status, 409)
                if change == 'newer':
                    self.assertEqual(self.pool.list()[-1]['result']['error'], 'dns_error')

    def test_busy_probe_does_not_leave_record_checking(self):
        item = self.add()
        with mock.patch('domain_pool.probe_tls', side_effect=Error('busy', 503)), self.assertRaises(Error):
            self.pool.probe(item['id'])
        self.assertEqual(self.pool.list()[0]['status'], 'not_checked')

    def test_existing_registry_without_domain_table_gains_pool_without_data_loss(self):
        old = Path(self.temp.name) / 'old.db'
        with sqlite3.connect(old) as db:
            db.executescript('CREATE TABLE servers(id INTEGER PRIMARY KEY, name TEXT NOT NULL);'
                             'CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);'
                             'CREATE TABLE nodes(id INTEGER PRIMARY KEY,name TEXT NOT NULL,server_id INTEGER NOT NULL);'
                             "INSERT INTO users VALUES(1,'existing-user');")
        registry = Registry(old)
        self.assertEqual(registry.list('users'), [{'id': 1, 'name': 'existing-user'}])
        self.assertEqual(DomainPool(registry).list(), [])
        self.assertEqual(DomainPool(registry).save(dict(domain='fixture.example', port=443, enabled=True))['id'], 1)


class ProbeBoundaryTests(unittest.TestCase):
    def test_rejects_all_nonpublic_and_mixed_dns_answers_before_connect(self):
        for address in ('127.0.0.1', '10.0.0.1', '169.254.169.254', '::1', '::ffff:127.0.0.1', '224.0.0.1'):
            self.assertFalse(_public_address(address))
        answers = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('8.8.8.8', 443)),
                   (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 443))]
        with mock.patch('domain_pool.ssl.HAS_TLSv1_3', True), \
                mock.patch('domain_pool.socket.getaddrinfo', return_value=answers), mock.patch('domain_pool.socket.socket') as connection:
            self.assertEqual(_tls_probe('fixture.example', 443, time.monotonic()+1)['error'], 'non_public_address')
            connection.assert_not_called()

    def test_dns_failure_and_deadline_and_capacity_are_explicit(self):
        with mock.patch('domain_pool.ssl.HAS_TLSv1_3', True), \
                mock.patch('domain_pool.socket.getaddrinfo', side_effect=socket.gaierror('fixture')):
            self.assertEqual(probe_tls('fixture.example', 443)['error'], 'dns_error')
        release, entered = threading.Event(), threading.Event()
        def blocked(*_):
            entered.set()
            release.wait(2)
            return available()
        try:
            with mock.patch('domain_pool._tls_probe', side_effect=blocked), \
                    mock.patch('domain_pool.PROBE_TIMEOUT', .03), \
                    mock.patch('domain_pool._PROBE_SLOTS', threading.BoundedSemaphore(1)):
                self.assertEqual(probe_tls('fixture.example', 443)['error'], 'timeout')
                self.assertTrue(entered.is_set())
                with self.assertRaises(Error) as caught: probe_tls('fixture.example', 443)
                self.assertEqual(caught.exception.status, 503)
                release.set()
        finally:
            release.set()

    def test_unsupported_local_tls_does_not_misreport_remote_domain(self):
        with mock.patch('domain_pool.ssl.HAS_TLSv1_3', False), mock.patch('domain_pool.socket.getaddrinfo') as resolver:
            self.assertEqual(_tls_probe('fixture.example', 443, time.monotonic()+1)['error'], 'unsupported_local_tls')
            resolver.assert_not_called()


class DomainPoolHttpTests(unittest.TestCase):
    setUp = test_core.ApiTests.setUp
    tearDown = test_core.ApiTests.tearDown
    request = test_core.ApiTests.request

    def test_authenticated_crud_probe_and_selection(self):
        for path, method in (('/api/reality-domains', 'GET'), ('/api/reality-domains/selection', 'GET'),
                             ('/api/reality-domains/1/probe', 'POST')):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.request(path, method, auth=False)
            self.assertEqual(caught.exception.code, 401)
        body = dict(domain='fixture.example', port=443, enabled=True)
        item = json.load(self.request('/api/reality-domains', 'POST', body))
        path = '/api/reality-domains/%d' % item['id']
        with mock.patch('domain_pool.probe_tls', return_value=available()):
            self.assertEqual(json.load(self.request(path+'/probe', 'POST'))['status'], 'available')
        self.assertEqual(json.load(self.request('/api/reality-domains/selection'))['id'], item['id'])
        json.load(self.request(path, 'PUT', {**body, 'enabled': False}))
        with self.assertRaises(urllib.error.HTTPError) as caught: self.request('/api/reality-domains/selection')
        self.assertEqual(caught.exception.code, 409)
        json.load(self.request(path, 'DELETE'))
        self.assertEqual(json.load(self.request('/api/reality-domains')), [])


@unittest.skipUnless(shutil.which('openssl') and ssl.HAS_TLSv1_3,
                     'OpenSSL CLI and Python TLS 1.3 required for isolated TLS fixture')
class RealTlsProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.cert, cls.key = (Path(cls.temp.name)/name for name in ('cert.pem', 'key.pem'))
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                        '-subj', '/CN=fixture.example', '-addext', 'subjectAltName=DNS:fixture.example',
                        '-keyout', str(cls.key), '-out', str(cls.cert)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    @contextmanager
    def target(self, alpn='h2', tls12=False):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(self.cert, self.key)
        context.set_alpn_protocols([alpn])
        if tls12: context.maximum_version = ssl.TLSVersion.TLSv1_2
        server = socket.socket()
        server.bind(('127.0.0.1', 0)); server.listen(); server.settimeout(.1)
        port, stop = server.getsockname()[1], threading.Event()
        def serve():
            while not stop.is_set():
                try: connection, _ = server.accept()
                except socket.timeout: continue
                except OSError: break
                with connection:
                    connection.settimeout(1)
                    try:
                        with context.wrap_socket(connection, server_side=True): pass
                    except (ssl.SSLError, OSError): pass
        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        answers = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', port))]
        try:
            # Only this controlled fixture bypasses public-IP filtering. The API
            # has no option to disable filtering or certificate verification.
            with mock.patch('domain_pool._public_address', return_value=True), \
                    mock.patch('domain_pool.socket.getaddrinfo', return_value=answers) as resolver:
                yield port, resolver
        finally:
            stop.set(); server.close(); thread.join(2)
            self.assertFalse(thread.is_alive())

    def test_real_tls13_h2_trusted_certificate_and_pinned_resolution(self):
        with self.target() as (port, resolver):
            context = ssl.create_default_context(cafile=str(self.cert))
            result = _tls_probe('fixture.example', port, time.monotonic()+2, context)
            self.assertTrue(result['ok'])
            self.assertEqual((result['tls_version'], result['alpn']), ('TLSv1.3', 'h2'))
            resolver.assert_called_once()

    def test_real_untrusted_and_wrong_hostname_certificates_are_rejected(self):
        with self.target() as (port, _):
            self.assertEqual(_tls_probe('fixture.example', port, time.monotonic()+2)['error'], 'certificate_error')
            trusted = ssl.create_default_context(cafile=str(self.cert))
            self.assertEqual(_tls_probe('wrong.example', port, time.monotonic()+2, trusted)['error'], 'certificate_error')

    def test_real_tls12_and_missing_h2_are_not_selected_as_eligible(self):
        with self.target(tls12=True) as (port, _):
            trusted = ssl.create_default_context(cafile=str(self.cert))
            self.assertEqual(_tls_probe('fixture.example', port, time.monotonic()+2, trusted)['error'], 'tls_error')
        with self.target(alpn='http/1.1') as (port, _):
            trusted = ssl.create_default_context(cafile=str(self.cert))
            self.assertEqual(_tls_probe('fixture.example', port, time.monotonic()+2, trusted)['error'], 'unsupported_tls_or_alpn')
