"""Test-only same-origin host. Private stdin controls; no credential HTTP endpoint."""
import json
from pathlib import Path
import signal
import secrets
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from urllib.parse import urlsplit

from core import Registry
from plan_node_bandwidth import PlanNodeBandwidth
from plan_nodes import PlanNodes
from server import Handler

WEB = Path(__file__).resolve().parents[1] / 'web'
FILES = {name: ('text/html; charset=utf-8' if name.endswith('.html') else
    'text/css; charset=utf-8' if name.endswith('.css') else 'text/javascript; charset=utf-8') for name in
    ('bandwidth-live.html', 'bandwidth-live.js', 'bandwidth-live.css', 'bandwidth-form.js',
     'bandwidth-form.css', 'bandwidth-resource.js', 'bandwidth-http.js', 'field-values.js')}


class FixtureHandler(Handler):
    def do_GET(self):
        path = urlsplit(self.path).path
        name = path.removeprefix('/')
        if name not in FILES:
            return self.handle_api()
        body = (WEB / name).read_bytes()
        self.send_response(200)
        for key, value in {'Content-Type': FILES[name], 'Content-Length': str(len(body)),
                'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
                'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src data:; form-action 'none'; base-uri 'none'; frame-ancestors 'none'"}.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)


def valid_token(value):
    return isinstance(value, str) and 16 <= len(value) <= 128 and all(33 <= ord(c) <= 126 for c in value)


class Fixture:
    def __enter__(self):
        self.temp = tempfile.TemporaryDirectory(prefix='mmwx-browser-')
        self.registry = Registry(Path(self.temp.name) / 'registry.db')
        host = self.registry.save('servers', {'name': 'temporary-browser'})['id']
        self.nodes = [self.registry.save('nodes', {'name': str(i), 'server_id': host})['id'] for i in (1, 2)]
        for index, ident in enumerate(self.nodes):
            self.registry.configure_node(ident, dict(protocol='vless', listen_host='127.0.0.1', listen_port=24500+index, enabled=True))
        self.plan = self.registry.save_plan(dict(name='temporary-browser-plan', bandwidth=dict(upload_bps=32768, download_bps=None),
            connections={'max_connections': None}))['id']
        assert self.plan == 1 and self.nodes == [1, 2]
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), FixtureHandler)
        self.server.registry = self.registry
        self.server.token = secrets.token_urlsafe(32)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        return self

    def __exit__(self, *args):
        self.server.shutdown(); self.server.server_close(); self.worker.join(timeout=5)
        self.temp.cleanup()

    def command(self, data):
        action = data.get('command')
        if action == 'prepare_management':
            self.registry.delete_plan(self.plan)
            for node in self.nodes: self.registry.delete('nodes', node)
            host = self.registry.list('servers')[0]['id']
            with self.registry.connect() as db:
                for node, name in ((37, '<img src=x onerror=alert(1)>'), (83, '第二节点')):
                    db.execute('INSERT INTO nodes(id,name,server_id) VALUES(?,?,?)', (node,name,host))
                for plan, name in ((17,'普通套餐'),(29,'单节点套餐'),(43,'空套餐')):
                    db.execute('INSERT INTO plans(id,name,bandwidth) VALUES(?,?,?)', (plan,name,json.dumps({'upload_bps':32768,'download_bps':None})))
            self.nodes = [37,83]; self.plan = 17
            for index, node in enumerate(self.nodes):
                self.registry.configure_node(node, dict(protocol='vless',listen_host='127.0.0.1',listen_port=24500+index,enabled=True))
            PlanNodes(self.registry).save(29, {'node_ids':[83]})
            PlanNodes(self.registry).save(43, {'node_ids':[]})
            return {'ok':True}
        if action == 'scope':
            PlanNodes(self.registry).clear(data['plan']) if data.get('clear') else PlanNodes(self.registry).save(data['plan'], {'node_ids':data['node_ids']})
            return {'ok':True}
        if action == 'grace':
            from plan_grace import GracePolicies
            policy=GracePolicies(self.registry)
            policy.delete(data['plan']) if data.get('remove') else policy.save(data['plan'], {'target_plan_id':data['target'],'days':1})
            return {'ok':True}
        if action == 'drop_plan':
            self.registry.delete_plan(data['plan'])
            return {'ok':True}
        if action == 'authorize' and valid_token(data.get('token')):
            self.server.token = data['token']
            return {'ok': True, 'origin': f'http://127.0.0.1:{self.server.server_port}'}
        if action == 'snapshot':
            policy = PlanNodeBandwidth(Registry(self.registry.path))
            return {'overrides': [policy.manage(data.get('plan',self.plan), n)['override'] for n in self.nodes]}
        if action == 'change':
            PlanNodeBandwidth(self.registry).manage(self.plan, self.nodes[0], data['bandwidth'])
            return {'ok': True}
        if action == 'stop':
            return {'stopped': True}
        return {'error': 'invalid fixture command'}


def main():
    def stop(*_):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, stop)
    with Fixture() as fixture:
        for line in sys.stdin:
            try:
                result = fixture.command(json.loads(line))
            except Exception:
                result = {'error': 'fixture command failed'}
            print(json.dumps(result), flush=True)
            if result.get('stopped'): break


if __name__ == '__main__':
    main()
