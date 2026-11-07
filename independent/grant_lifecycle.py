"""Durable lifecycle coordinator for the explicit grant protocol.

This is orchestration state only.  It never turns a staged grant or an
installation receipt into an execution grant and always reports
``ready_for_execution=False``.
"""
import json
import time

from core import Error
from grant_capabilities import INSTALLATION, protocols

SCHEMA = '''
CREATE TABLE IF NOT EXISTS usage_first_total_lifecycle(
 grant_id TEXT PRIMARY KEY REFERENCES usage_first_total_grants(grant_id),
 server_id INTEGER NOT NULL, session_id TEXT NOT NULL,
 configuration_digest TEXT NOT NULL, state TEXT NOT NULL,
 updated_at REAL NOT NULL);
'''

STATES = frozenset(('installing', 'installed', 'closed', 'blocked'))


def _receipt(grant_id, state, *, ready=False):
    return dict(accepted=True, grant_id=grant_id, state=state,
                ready_for_execution=ready)


class GrantLifecycle:
    def __init__(self, registry):
        self.registry = registry

    def _identity(self, db, token, grant_id, session_id):
        r = self.registry
        sid = r._agent_id(db, token)
        r._check_session(db, sid, session_id)
        grant = db.execute(
            'SELECT * FROM usage_first_total_grants WHERE grant_id=?',
            (grant_id,)).fetchone()
        if grant is None:
            raise Error('授予不存在', 404)
        fingerprint = db.execute(
            'SELECT token_hash FROM agents WHERE server_id=?', (sid,)).fetchone()[0]
        if (grant['server_id'] != sid or grant['session_id'] != session_id
                or grant['token_hash'] != fingerprint):
            raise Error('授予身份不匹配', 403)
        capability = db.execute(
            'SELECT * FROM grant_capabilities WHERE server_id=?', (sid,)).fetchone()
        if (capability is None or capability['session_id'] != session_id
                or capability['token_hash'] != fingerprint
                or INSTALLATION not in protocols(json.loads(capability['offer']))):
            raise Error('当前会话未协商安装协议', 409)
        configuration = db.execute(
            'SELECT * FROM usage_first_total_configurations WHERE grant_id=?',
            (grant_id,)).fetchone()
        if configuration is None:
            raise Error('授予尚未领取配置', 409)
        # The configuration digest is the immutable lifecycle identity.  Do
        # not rebuild or silently rebind it while a lifecycle is active.
        return sid, grant, configuration

    def begin(self, token, grant_id, session_id):
        self.registry._session_value(grant_id)
        self.registry._session_value(session_id)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid, grant, configuration = self._identity(db, token, grant_id, session_id)
            now = time.time()
            if not grant['issued_at'] <= now < grant['valid_until']:
                raise Error('授予窗口不可用', 409)
            old = db.execute(
                'SELECT * FROM usage_first_total_lifecycle WHERE grant_id=?',
                (grant_id,)).fetchone()
            if old is not None:
                if (old['server_id'], old['session_id'], old['configuration_digest']) != (
                        sid, session_id, configuration['digest']):
                    raise Error('授予生命周期身份冲突', 409)
                if old['state'] == 'blocked':
                    raise Error('授予生命周期已阻断', 409)
                return _receipt(grant_id, old['state'])
            db.execute('INSERT INTO usage_first_total_lifecycle VALUES(?,?,?,?,?,?)',
                       (grant_id, sid, session_id, configuration['digest'],
                        'installing', now))
            return _receipt(grant_id, 'installing')

    def mark_installed(self, token, grant_id, session_id):
        self.registry._session_value(grant_id)
        self.registry._session_value(session_id)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid, grant, configuration = self._identity(db, token, grant_id, session_id)
            if time.time() >= grant['valid_until']:
                raise Error('授予窗口不可用', 409)
            installation = db.execute(
                'SELECT fact FROM usage_first_total_installations WHERE grant_id=?',
                (grant_id,)).fetchone()
            applied = db.execute(
                'SELECT * FROM usage_first_total_applied WHERE grant_id=?',
                (grant_id,)).fetchone()
            if installation is None or applied is None:
                raise Error('缺少安装回执，不能推进生命周期', 409)
            old = db.execute(
                'SELECT * FROM usage_first_total_lifecycle WHERE grant_id=?',
                (grant_id,)).fetchone()
            if old is None:
                raise Error('生命周期尚未开始', 409)
            if (old['server_id'], old['session_id'], old['configuration_digest']) != (
                    sid, session_id, configuration['digest']):
                raise Error('授予生命周期身份冲突', 409)
            if old['state'] == 'blocked':
                raise Error('授予生命周期已阻断', 409)
            if old['state'] == 'closed':
                raise Error('授予生命周期已关闭', 409)
            db.execute("UPDATE usage_first_total_lifecycle SET state='installed',updated_at=? WHERE grant_id=?",
                       (time.time(), grant_id))
            return _receipt(grant_id, 'installed')

    def close(self, token, grant_id, session_id):
        self.registry._session_value(grant_id)
        self.registry._session_value(session_id)
        with self.registry.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid, grant, configuration = self._identity(db, token, grant_id, session_id)
            old = db.execute(
                'SELECT * FROM usage_first_total_lifecycle WHERE grant_id=?',
                (grant_id,)).fetchone()
            if old is None:
                raise Error('生命周期尚未开始', 409)
            if (old['server_id'], old['session_id'], old['configuration_digest']) != (
                    sid, session_id, configuration['digest']):
                raise Error('授予生命周期身份冲突', 409)
            if old['state'] == 'blocked':
                raise Error('授予生命周期已阻断', 409)
            if old['state'] == 'closed':
                return _receipt(grant_id, 'closed')
            if old['state'] != 'installed':
                raise Error('授予尚未安装，不能关闭', 409)
            db.execute("UPDATE usage_first_total_lifecycle SET state='closed',updated_at=? WHERE grant_id=?",
                       (time.time(), grant_id))
            return _receipt(grant_id, 'closed')

    def status(self, token, grant_id, session_id):
        self.registry._session_value(grant_id)
        self.registry._session_value(session_id)
        with self.registry.connect() as db:
            db.execute('BEGIN')
            sid, grant, configuration = self._identity(db, token, grant_id, session_id)
            row = db.execute('SELECT * FROM usage_first_total_lifecycle WHERE grant_id=?',
                             (grant_id,)).fetchone()
            if row is None:
                raise Error('生命周期尚未开始', 404)
            if (row['server_id'], row['session_id'], row['configuration_digest']) != (
                    sid, session_id, configuration['digest']):
                raise Error('授予生命周期身份冲突', 409)
            return _receipt(grant_id, row['state'])
