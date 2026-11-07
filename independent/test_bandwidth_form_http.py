"""Frontend transport against the real Handler, ephemeral loopback and temporary DB."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer

from core import Registry
from plan_node_bandwidth import PlanNodeBandwidth
from plan_nodes import PlanNodes
from server import Handler


class BandwidthFormHTTPTests(unittest.TestCase):
    def test_frontend_transport_real_api_and_persistence(self):
        node = shutil.which('node')
        self.assertIsNotNone(node, 'Node.js required for frontend integration; not skipped')
        with tempfile.TemporaryDirectory(prefix='mmwx-dev023-') as directory:
            registry = Registry(Path(directory) / 'registry.db')
            host = registry.save('servers', {'name': 'isolated-http'})['id']
            nodes = [registry.save('nodes', {'name': name, 'server_id': host})['id'] for name in ('a', 'b')]
            for index, ident in enumerate(nodes):
                registry.configure_node(ident, dict(protocol='vless', listen_host='127.0.0.1', listen_port=24500+index, enabled=True))
            plan = registry.save_plan(dict(name='isolated-plan', bandwidth=dict(upload_bps=32768, download_bps=None),
                connections={'max_connections': None}))['id']
            restricted = registry.save_plan(dict(name='isolated-restricted', bandwidth=dict(upload_bps=32768, download_bps=None),
                connections={'max_connections': None}))['id']
            PlanNodes(registry).save(restricted, {'node_ids': [nodes[0]]})
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            server.registry = registry
            server.token = secrets.token_urlsafe(32)
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                config = dict(baseURL=f'http://127.0.0.1:{server.server_port}', token=server.token,
                    agentToken=registry.provision_agent(host)['token'], plan=plan, restricted=restricted, nodes=nodes)
                script = Path(__file__).resolve().parents[1] / 'tests' / 'bandwidth-http-integration.cjs'
                # Generated secrets travel over child stdin, never argv, files or logs.
                result = subprocess.run([node, str(script)], input=json.dumps(config), capture_output=True,
                    text=True, timeout=45, env={'PATH': os.environ.get('PATH', '')})
                output = result.stdout + result.stderr
                self.assertTrue(server.token not in output, 'Child diagnostics must not disclose generated administrator credential')
                self.assertTrue(config['agentToken'] not in output, 'Child diagnostics must not disclose generated agent credential')
                self.assertEqual(result.returncode, 0, output)
                self.assertIn('HTTP integration: 10/10 passed', output)
                # Inspect independent persisted state through a fresh Registry instance.
                reopened = Registry(registry.path)
                self.assertEqual(PlanNodeBandwidth(reopened).manage(plan, nodes[0])['override'], {'upload_bps': 750000, 'download_bps': None})
                self.assertEqual(PlanNodeBandwidth(reopened).manage(plan, nodes[1])['override'], {'upload_bps': 1, 'download_bps': 1073741824})
                self.assertIsNone(PlanNodeBandwidth(reopened).manage(restricted, nodes[1])['override'])
                print('Frontend real HTTP integration: 10/10 passed; persisted state verified', flush=True)
            finally:
                server.shutdown()
                server.server_close()
                worker.join(timeout=5)
                self.assertFalse(worker.is_alive(), 'Fixture API thread must stop before temporary DB cleanup')

