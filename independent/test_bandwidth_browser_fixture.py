"""Check the test host's boundary and cleanup without starting execution runtimes."""
import secrets
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from bandwidth_browser_fixture import Fixture


class BrowserFixtureTests(unittest.TestCase):
    def test_static_whitelist_no_secrets_and_real_auth_boundary(self):
        with Fixture() as fixture:
            token = secrets.token_urlsafe(32)
            origin = fixture.command({'command': 'authorize', 'token': token})['origin']
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(origin+'/bandwidth-live.html', timeout=3) as reply:
                self.assertEqual(reply.status, 200)
                self.assertIn("connect-src 'self'", reply.headers['Content-Security-Policy'])
                self.assertEqual(reply.headers['Cache-Control'], 'no-store')
                self.assertTrue(token not in reply.read().decode(), 'Static HTML must not embed generated credential')
            for path in ('/api/plans/1/nodes/1/bandwidth', '/server.py', '/.local-backup'):
                with self.assertRaises(urllib.error.HTTPError) as caught:
                    opener.open(origin+path, timeout=3)
                self.assertEqual(caught.exception.code, 401)
            self.assertEqual(fixture.command({'command': 'snapshot'}), {'overrides': [None, None]})
            temp = Path(fixture.temp.name)
        self.assertFalse(temp.exists())
        self.assertFalse(fixture.worker.is_alive())

    def test_private_commands_rotate_and_change_only_temporary_state(self):
        with Fixture() as fixture:
            first, second = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
            fixture.command({'command': 'authorize', 'token': first})
            origin = fixture.command({'command': 'authorize', 'token': second})['origin']
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            request = urllib.request.Request(origin+'/api/plans/1/nodes/1/bandwidth', headers={'Authorization': 'Bearer '+first})
            with self.assertRaises(urllib.error.HTTPError) as caught: opener.open(request, timeout=3)
            self.assertEqual(caught.exception.code, 401)
            fixture.command({'command': 'change', 'bandwidth': {'upload_bps': 500000, 'download_bps': None}})
            self.assertEqual(fixture.command({'command': 'snapshot'})['overrides'][0], {'upload_bps': 500000, 'download_bps': None})
            self.assertIn('error', fixture.command({'command': 'authorize', 'token': ''}))
