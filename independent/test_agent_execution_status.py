"""Administrator Grant status is per-Agent, exact and non-secret."""
import json
import sqlite3
import threading
import time
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from unittest.mock import patch
import uuid

from core import Error
from server import Handler
import test_grant_execution as fixture


class AgentExecutionStatusTests(unittest.TestCase):
    setUp = fixture.GrantExecutionTests.setUp
    tearDown = fixture.GrantExecutionTests.tearDown
    setup_installed = fixture.GrantExecutionTests.setup_installed
    execution_fact = fixture.GrantExecutionTests.execution_fact
    execute = fixture.GrantExecutionTests.execute

    def complete(self):
        self.setup_installed()
        receipt = self.execute()
        self.r.heartbeat(self.token, dict(
            hostname='grant-agent', uptime=1, session_id=self.session))
        return receipt

    def legacy_agent(self, name='legacy'):
        sid = self.r.save('servers', {'name': name})['id']
        token = self.r.provision_agent(sid)['token']
        session = str(uuid.uuid4())
        self.r.begin_session(token, {'session_id': session})
        self.r.heartbeat(token, dict(
            hostname=name, uptime=1, session_id=session))
        configuration = self.r.configuration(token)
        ack = {key: configuration[key] for key in ('revision', 'digest')}
        ack['session_id'] = session
        self.r.acknowledge_configuration(token, ack)
        self.r.acknowledge_runtime(token, ack)
        return sid, token, session

    def by_server(self):
        return {row['server_id']: row for row in self.r.agent_status()}

    def test_two_agents_are_stably_isolated_and_legacy_is_explicitly_empty(self):
        self.complete()
        other, other_token, other_session = self.legacy_agent()
        rows = self.r.agent_status()
        self.assertEqual([row['server_id'] for row in rows],
                         sorted(row['server_id'] for row in rows))
        current = self.by_server()[self.g['server_id']]
        legacy = self.by_server()[other]
        self.assertTrue(current['online'])
        self.assertTrue(current['grant_execution']['current'])
        self.assertEqual(current['grant_execution']['current_count'], 1)
        self.assertTrue(legacy['online'])
        self.assertTrue(legacy['listeners_current'])
        self.assertEqual(legacy['grant_execution']['records'], [])
        self.assertFalse(legacy['grant_execution']['current'])
        self.assertFalse(legacy['grant_execution']['ready_for_execution'])

        self.r.provision_agent(other)
        with self.assertRaises(Error):
            self.r.heartbeat(other_token, dict(
                hostname='old', uptime=2, session_id=other_session))
        self.assertTrue(self.by_server()[self.g['server_id']]
                        ['grant_execution']['current'])

    def test_offline_rotation_and_revocation_keep_history_but_not_current(self):
        receipt = self.complete()
        status = self.by_server()[self.g['server_id']]
        with patch('core.time.time', return_value=status['last_seen'] + 45):
            offline = self.by_server()[self.g['server_id']]
        self.assertFalse(offline['online'])
        self.assertFalse(offline['grant_execution']['current'])
        self.assertTrue(offline['grant_execution']['records'][0]
                        ['execution_recorded'])

        self.r.begin_session(self.token, {'session_id': str(uuid.uuid4())})
        retired = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertFalse(retired['current'])
        self.assertEqual(retired['records'][0]['integrity'], 'stale_identity')
        self.assertTrue(retired['records'][0]['execution_recorded'])

        self.r.provision_agent(self.g['server_id'])
        rotated = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertFalse(rotated['current'])
        self.assertEqual(rotated['records'][0]['integrity'], 'stale_identity')
        self.assertTrue(rotated['records'][0]['execution_recorded'])
        self.r.revoke_agent(self.g['server_id'])
        revoked = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertFalse(revoked['current'])
        self.assertTrue(revoked['records'][0]['execution_recorded'])
        self.assertNotIn(receipt['execution']['execution_id'],
                         json.dumps(revoked))

    def test_lifecycle_and_expiry_are_audited_but_fail_closed(self):
        self.complete()
        original = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertEqual(original['records'][0]['lifecycle'], 'installed')
        self.assertTrue(original['records'][0]['current'])
        with self.r.connect() as db:
            valid_until = db.execute(
                'SELECT valid_until FROM usage_first_total_grants').fetchone()[0]
        with patch('core.time.time', return_value=valid_until):
            expired = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertTrue(expired['records'][0]['expired'])
        self.assertFalse(expired['current'])

        for state in ('installing', 'blocked', 'closed'):
            with self.subTest(state=state), self.r.connect() as db:
                db.execute('UPDATE usage_first_total_lifecycle SET state=?',
                           (state,))
            value = self.by_server()[self.g['server_id']]['grant_execution']
            self.assertEqual(value['records'][0]['lifecycle'], state)
            self.assertTrue(value['records'][0]['execution_recorded'])
            self.assertFalse(value['current'])

    def test_exact_binding_deadline_expires_before_outer_grant(self):
        receipt = self.complete()
        with self.r.connect() as db:
            grant_until = db.execute(
                'SELECT valid_until FROM usage_first_total_grants').fetchone()[0]
            configuration = db.execute(
                'SELECT payload FROM usage_first_total_configurations').fetchone()
            value = json.loads(configuration['payload'])
            boundary = min(time.time() + 5, grant_until - 1)
            self.assertGreater(boundary, value['issued_at'])
            self.assertLess(boundary, grant_until)
            value['valid_until'] = boundary
            from node_config import digest
            encoded = json.dumps(value, sort_keys=True, separators=(',', ':'))
            checksum = digest(value)
            value['digest'] = checksum
            from grant_ack import fact_for
            installation = fact_for(value)
            execution = dict(installation,
                             execution_id=receipt['execution']['execution_id'])
            db.execute(
                'UPDATE usage_first_total_configurations SET payload=?,digest=?',
                (encoded, checksum))
            db.execute(
                'UPDATE usage_first_total_installations SET fact=?',
                (json.dumps(installation, sort_keys=True,
                            separators=(',', ':')),))
            db.execute(
                'UPDATE usage_first_total_applied SET config_version=?, '
                'config_digest=?,valid_until=?',
                (installation['revision'], installation['digest'], boundary))
            db.execute(
                'UPDATE usage_first_total_lifecycle SET configuration_digest=?',
                (checksum,))
            db.execute(
                'UPDATE usage_first_total_execution SET fact=?',
                (json.dumps(execution, sort_keys=True,
                            separators=(',', ':')),))

        with patch('core.time.time', return_value=boundary - .001):
            before = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertTrue(before['current'])
        self.assertFalse(before['records'][0]['expired'])
        self.assertEqual(before['records'][0]['integrity'], 'verified')
        with patch('core.time.time', return_value=boundary):
            at_boundary = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertFalse(at_boundary['current'])
        self.assertTrue(at_boundary['records'][0]['expired'])
        self.assertTrue(at_boundary['records'][0]['execution_recorded'])
        self.assertEqual(at_boundary['records'][0]['integrity'], 'verified')
        with patch('core.time.time', return_value=boundary + .001):
            after = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertFalse(after['current'])
        self.assertTrue(after['records'][0]['expired'])

    def test_registry_and_http_status_are_repeatable_read_only_snapshots(self):
        self.complete()
        deadline = time.time() + 5
        self.r.temporary_bandwidth(self.uid, {
            'bandwidth': {'upload_bps': 12345, 'download_bps': 23456},
            'expires_at': deadline,
        })

        def database_state():
            with sqlite3.connect(self.r.path) as db:
                return (
                    db.execute(
                        'SELECT user_id,payload,expires_at FROM '
                        'temporary_bandwidth ORDER BY user_id').fetchall(),
                    db.execute(
                        'SELECT server_id,revision,staged_revision,'
                        'staged_digest,staged_at FROM server_configs '
                        'ORDER BY server_id').fetchall(),
                )

        before = database_state()
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.registry = self.r
        server.token = 'admin-read-only'
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        request = urllib.request.Request(
            f'http://127.0.0.1:{server.server_port}/api/agent-status',
            headers={'Authorization': 'Bearer admin-read-only'})

        with patch('core.time.time', return_value=deadline):
            first = self.r.agent_status()
            second = self.r.agent_status()
            with urllib.request.urlopen(request, timeout=5) as response:
                http_first = json.load(response)
            with urllib.request.urlopen(request, timeout=5) as response:
                http_second = json.load(response)
        self.assertEqual(first, second)
        self.assertEqual(http_first, http_second)
        self.assertEqual(first, http_first)
        self.assertEqual(database_state(), before)

    def test_missing_and_corrupt_evidence_never_becomes_current(self):
        self.complete()
        with self.r.connect() as db:
            db.execute("UPDATE usage_first_total_execution SET fact='not-json'")
        corrupt = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertTrue(corrupt['records'][0]['execution_recorded'])
        self.assertEqual(corrupt['records'][0]['integrity'], 'invalid')
        self.assertFalse(corrupt['current'])

        with self.r.connect() as db:
            db.execute('DELETE FROM usage_first_total_execution')
        missing = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertFalse(missing['records'][0]['execution_recorded'])
        self.assertEqual(missing['records'][0]['integrity'], 'incomplete')
        self.assertFalse(missing['current'])

        with self.r.connect() as db:
            db.execute('DROP TABLE usage_first_total_execution')
        partial = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertEqual(partial['evidence_status'], 'partial')
        self.assertFalse(partial['current'])

    def test_corrupt_grant_and_self_consistent_private_identity_fail_closed(self):
        self.complete()
        with self.r.connect() as db:
            baseline = db.execute(
                'SELECT baseline_bytes FROM usage_first_total_grants').fetchone()[0]
            db.execute("UPDATE usage_first_total_grants SET baseline_bytes='bad'")
        corrupt = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertEqual(corrupt['records'][0]['integrity'], 'invalid')
        self.assertFalse(corrupt['current'])

        with self.r.connect() as db:
            db.execute('UPDATE usage_first_total_grants SET baseline_bytes=?',
                       (baseline,))
            row = db.execute(
                'SELECT payload FROM usage_first_total_configurations').fetchone()
            value = json.loads(row['payload'])
            value['client']['uuid'] = str(uuid.uuid4())
            from node_config import digest
            encoded = json.dumps(value, sort_keys=True, separators=(',', ':'))
            checksum = digest(value)
            db.execute(
                'UPDATE usage_first_total_configurations SET payload=?,digest=?',
                (encoded, checksum))
        identity = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertEqual(identity['records'][0]['integrity'], 'invalid')
        self.assertFalse(identity['current'])

    def test_staged_installed_or_online_alone_are_never_current(self):
        self.setup_installed()
        self.r.heartbeat(self.token, dict(
            hostname='grant-agent', uptime=1, session_id=self.session))
        installed = self.by_server()[self.g['server_id']]['grant_execution']
        self.assertEqual(installed['records'][0]['lifecycle'], 'installed')
        self.assertFalse(installed['records'][0]['execution_recorded'])
        self.assertFalse(installed['current'])

        empty, _, _ = self.legacy_agent('online-only')
        online = self.by_server()[empty]
        self.assertTrue(online['online'])
        self.assertTrue(online['listeners_current'])
        self.assertFalse(online['grant_execution']['current'])

    def test_real_admin_http_contract_contains_no_sensitive_identity(self):
        receipt = self.complete()
        empty = self.r.save('servers', {'name': 'empty'})['id']
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        server.registry = self.r
        server.token = 'admin-fixture'
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(thread.join)
        self.addCleanup(server.shutdown)
        request = urllib.request.Request(
            f'http://127.0.0.1:{server.server_port}/api/agent-status',
            headers={'Authorization': 'Bearer admin-fixture'})
        with urllib.request.urlopen(request, timeout=5) as response:
            rows = json.load(response)
            self.assertEqual(response.headers['Cache-Control'], 'no-store')
        body = json.dumps(rows, sort_keys=True)
        self.assertEqual(next(row for row in rows if row['server_id'] == empty)
                         ['grant_execution']['records'], [])
        for secret in (self.token, self.session, self.g['grant_id'],
                       self.item['credential'],
                       receipt['execution']['execution_id']):
            self.assertNotIn(secret, body)
        for forbidden in ('token', 'token_hash', 'session_id', 'grant_id',
                          'execution_id', 'credential', 'uuid', 'node_config',
                          'payload'):
            self.assertNotIn(f'"{forbidden}"', body)
        self.assertIn('"ready_for_execution": false', body)


for name in ('fixtures', 'fixture', 'setup_total', 'setup_drain', 'applied',
             'report', 'data', 'envelope', 'accept', 'sealed', 'request',
             'prepare', 'start', 'start_http', 'fetch', 'post', 'ack',
             'setup_lifecycle', 'lifecycle'):
    setattr(AgentExecutionStatusTests, name,
            getattr(fixture.GrantExecutionTests, name))
AgentExecutionStatusTests.encode = staticmethod(
    fixture.GrantExecutionTests.encode)


if __name__ == '__main__':
    unittest.main()
