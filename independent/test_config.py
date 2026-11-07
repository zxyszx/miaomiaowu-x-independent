import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import urllib.error

from agent import sync
from core import Error, Registry
from node_config import digest, validate_snapshot
import test_core

PROFILE = dict(listen_host='127.0.0.1', listen_port=24001,
               target_host='example.test', target_port=443, enabled=True)


class ConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'registry.db'
        self.registry = Registry(self.path)
        self.server = self.registry.save('servers', {'name': 'first'})['id']
        self.other = self.registry.save('servers', {'name': 'second'})['id']
        self.token = self.registry.provision_agent(self.server)['token']
        self.other_token = self.registry.provision_agent(self.other)['token']
        self.node = self.node_on(self.server)

    def tearDown(self):
        self.temp.cleanup()

    def node_on(self, server):
        return self.registry.save('nodes', {'name': 'fixture', 'server_id': server})['id']

    def ack(self, snapshot):
        return self.registry.acknowledge_configuration(self.token,
            {key: snapshot[key] for key in ('revision', 'digest')})

    def test_restarted_agent_invalidates_old_receipts_before_new_configuration(self):
        self.registry.heartbeat(self.token,{'uptime':120,'hostname':'fixture'})
        current=self.registry.configuration(self.token)
        receipt={key:current[key] for key in ('revision','digest')}
        self.ack(current)
        self.registry.acknowledge_runtime(self.token,receipt)
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])
        self.registry.heartbeat(self.token,{'uptime':.1,'hostname':'fixture'})
        status=self.registry.agent_status()[0]
        self.assertTrue(status['online'])
        self.assertFalse(status['listeners_current'])
        self.assertIsNone(status['staged_revision'])
        self.assertIsNone(status['applied_revision'])
        with self.assertRaises(Error): self.registry.acknowledge_runtime(self.token,receipt)
        self.ack(current)
        self.registry.acknowledge_runtime(self.token,receipt)
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])

    def test_normal_or_invalid_heartbeat_preserves_execution_receipts(self):
        self.registry.heartbeat(self.token,{'uptime':120,'hostname':'fixture'})
        current=self.registry.configuration(self.token)
        receipt={key:current[key] for key in ('revision','digest')}
        self.ack(current)
        self.registry.acknowledge_runtime(self.token,receipt)
        self.registry.heartbeat(self.token,{'uptime':121,'hostname':'fixture'})
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])
        with self.assertRaises(Error):
            self.registry.heartbeat(self.token,{'uptime':.1,'hostname':''})
        self.assertTrue(self.registry.agent_status()[0]['listeners_current'])

    def test_scoped_snapshot_and_persistence(self):
        self.registry.configure_node(self.node, PROFILE)
        other_node = self.node_on(self.other)
        self.registry.configure_node(other_node, PROFILE)
        snapshot = validate_snapshot(self.registry.configuration(self.token))
        self.assertEqual([node['id'] for node in snapshot['nodes']], [self.node])
        self.assertEqual(self.ack(snapshot)['state'], 'staged')
        restored = Registry(self.path)
        self.assertEqual(restored.configuration(self.token), snapshot)
        self.assertEqual(restored.agent_status()[0]['staged_revision'], snapshot['revision'])
        with self.assertRaises(Error): self.ack(restored.configuration(self.other_token))
        restored.revoke_agent(self.server)
        self.assertIsNone(restored.agent_status()[0]['staged_revision'])
        with self.assertRaises(Error): restored.configuration(self.token)

    def test_bad_profiles_rejected_without_revision_change(self):
        before = self.registry.configuration(self.token)
        for changes in ({'listen_host': True}, {'listen_port': True}, {'listen_port': 0},
                        {'target_port': 65536}, {'target_host': 'host; command'},
                        {'enabled': 1}, {'extra': 'ignored?'}):
            with self.subTest(changes=changes), self.assertRaises(Error):
                self.registry.configure_node(self.node, {**PROFILE, **changes})
        self.assertEqual(self.registry.configuration(self.token), before)

    def test_port_conflict_and_move_rollback(self):
        self.registry.configure_node(self.node, PROFILE)
        same_server = self.node_on(self.server)
        with self.assertRaises(Error): self.registry.configure_node(same_server, PROFILE)
        other_node = self.node_on(self.other)
        self.registry.configure_node(other_node, PROFILE)
        before = self.registry.configuration(self.other_token)
        with self.assertRaises(Error):
            self.registry.save('nodes', {'name': 'moved', 'server_id': self.server}, other_node)
        self.assertEqual(self.registry.configuration(self.other_token), before)
        self.registry.configure_node(same_server, {**PROFILE, 'enabled': False})

    def test_revisions_idempotency_stale_ack_and_delete(self):
        self.registry.configure_node(self.node, PROFILE)
        first = self.registry.configuration(self.token)
        self.registry.configure_node(self.node, PROFILE)
        self.assertEqual(self.registry.configuration(self.token), first)
        self.ack(first)
        self.registry.configure_node(self.node, {**PROFILE, 'target_port': 8443})
        current = self.registry.configuration(self.token)
        self.assertGreater(current['revision'], first['revision'])
        with self.assertRaises(Error) as caught: self.ack(first)
        self.assertEqual(caught.exception.status, 409)
        with self.assertRaises(Error): self.ack({**current, 'digest': '0' * 64})
        self.assertNotEqual(self.registry.agent_status()[0]['desired_revision'],
                            self.registry.agent_status()[0]['staged_revision'])
        self.registry.delete('nodes', self.node)
        empty = self.registry.configuration(self.token)
        self.assertEqual(empty['nodes'], [])
        self.assertGreater(empty['revision'], current['revision'])

    def test_tampered_snapshot_is_not_written_or_acknowledged(self):
        self.registry.configure_node(self.node, PROFILE)
        snapshot = self.registry.configuration(self.token)
        snapshot['nodes'][0]['config']['target_port'] = 1234
        state = Path(self.temp.name) / 'agent-state'
        with mock.patch('agent.request', return_value=snapshot) as request:
            with self.assertRaises(ValueError): sync('http://localhost', self.token, state)
        self.assertEqual(request.call_count, 1)
        self.assertFalse(state.exists())

    def test_agent_rejects_rollback_identity_change_and_same_version_mutation(self):
        current = self.registry.configuration(self.token)
        state = Path(self.temp.name) / 'state'
        state.mkdir()
        destination = state / 'staged-config.json'
        destination.write_text(json.dumps(current))
        variants = [dict(current, revision=current['revision'] - 1),
                    dict(current, server_id=self.other), dict(current, server_name='changed')]
        for snapshot in variants:
            snapshot['digest'] = digest({k: v for k, v in snapshot.items() if k != 'digest'})
            with self.subTest(snapshot=snapshot), mock.patch('agent.request', return_value=snapshot) as request:
                with self.assertRaises(ValueError): sync('http://localhost', self.token, state)
                self.assertEqual(request.call_count, 1)
            self.assertEqual(json.loads(destination.read_text()), current)

    def test_failed_replace_preserves_previous_and_does_not_ack(self):
        state = Path(self.temp.name) / 'state'
        state.mkdir()
        original = self.registry.configuration(self.token)
        destination = state / 'staged-config.json'
        destination.write_text(json.dumps(original))
        self.registry.configure_node(self.node, PROFILE)
        newer = self.registry.configuration(self.token)
        with mock.patch('agent.request', return_value=newer) as request, \
             mock.patch('agent.Path.replace', side_effect=OSError('disk failure')):
            with self.assertRaises(OSError): sync('http://localhost', self.token, state)
        self.assertEqual(request.call_count, 1)
        self.assertEqual(json.loads(destination.read_text()), original)
        self.assertEqual(list(state.iterdir()), [destination])

    def test_ack_failure_keeps_valid_file_and_can_retry(self):
        self.registry.configure_node(self.node, PROFILE)
        snapshot = self.registry.configuration(self.token)
        state = Path(self.temp.name) / 'state'
        with mock.patch('agent.request', side_effect=[snapshot, OSError('connection lost')]):
            with self.assertRaises(OSError): sync('http://localhost', self.token, state)
        self.assertEqual(json.loads((state / 'staged-config.json').read_text()), snapshot)
        self.assertIsNone(self.registry.agent_status()[0]['staged_revision'])
        def respond(base, token, path, data=None):
            return self.registry.configuration(token) if data is None else self.registry.acknowledge_configuration(token, data)
        with mock.patch('agent.request', side_effect=respond):
            self.assertTrue(sync('http://localhost', self.token, state)['accepted'])
        self.assertEqual(self.registry.agent_status()[0]['staged_revision'], snapshot['revision'])

    def test_node_move_updates_both_server_revisions(self):
        self.registry.configure_node(self.node, PROFILE)
        before = self.registry.configuration(self.token)
        other_before = self.registry.configuration(self.other_token)
        self.registry.save('nodes', {'name': 'moved', 'server_id': self.other}, self.node)
        after = self.registry.configuration(self.token)
        other_after = self.registry.configuration(self.other_token)
        self.assertEqual(after['nodes'], [])
        self.assertEqual(other_after['nodes'][0]['id'], self.node)
        self.assertGreater(after['revision'], before['revision'])
        self.assertGreater(other_after['revision'], other_before['revision'])


class ConfigurationHttpTests(unittest.TestCase):
    setUp = test_core.ApiTests.setUp
    tearDown = test_core.ApiTests.tearDown
    request = test_core.ApiTests.request
    def test_actual_http_config_staging_retry_and_revocation(self):
        server = json.load(self.request('/api/servers', 'POST', {'name': 'local'}))
        node = json.load(self.request('/api/nodes', 'POST', {'name': 'node', 'server_id': server['id']}))
        self.request(f'/api/nodes/{node["id"]}/config', 'PUT', PROFILE).close()
        credentials = json.load(self.request(f'/api/servers/{server["id"]}/agent', 'POST'))
        state = Path(self.temp.name) / 'state'
        receipt = sync(self.base, credentials['token'], state)
        self.assertEqual(receipt['state'], 'staged')
        saved = state / 'staged-config.json'
        self.assertEqual(saved.stat().st_mode & 0o777, 0o600)
        snapshot = validate_snapshot(json.loads(saved.read_text()))
        self.assertEqual(snapshot['nodes'][0]['config'], PROFILE)
        self.assertEqual(sync(self.base, credentials['token'], state), receipt)
        status = json.load(self.request('/api/agent-status'))[0]
        self.assertEqual(status['desired_revision'], status['staged_revision'])
        self.assertFalse(status['online'])  # Staging alone is not a heartbeat or runtime health.
        self.request(f'/api/servers/{server["id"]}/agent', 'DELETE').close()
        with self.assertRaises(urllib.error.HTTPError): sync(self.base, credentials['token'], state)
        self.assertEqual(json.loads(saved.read_text()), snapshot)


if __name__ == '__main__':
    unittest.main()
