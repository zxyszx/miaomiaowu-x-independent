"""Authenticated master record for a grant execution attempt.

This is an immutable audit receipt after installation.  It does not enable
execution, grant bytes, replace runtime ACKs, or make the independent module
ready for production execution.
"""
import json
import math
import time
import uuid

from core import Error
from grant_capabilities import INSTALLATION, protocols

SCHEMA = '''
CREATE TABLE IF NOT EXISTS usage_first_total_execution(
 grant_id TEXT PRIMARY KEY REFERENCES usage_first_total_grants(grant_id),
 fact TEXT NOT NULL, recorded_at REAL NOT NULL);
'''

FIELDS = frozenset(('grant_id', 'server_id', 'session_id', 'revision',
                    'digest', 'runtime_digest', 'protocol', 'execution_id',
                    'valid_until'))


def _receipt(fact, recorded_at):
    return dict(accepted=True, state='execution_recorded',
                execution=dict(fact), recorded_at=recorded_at,
                ready_for_execution=False)


class GrantExecution:
    def __init__(self, registry):
        self.registry = registry

    @staticmethod
    def _validate_fact(fact):
        if type(fact) is not dict or set(fact) != FIELDS:
            raise Error('执行回执字段无效')
        if (type(fact['server_id']) is not int or type(fact['revision']) is not int
                or fact['server_id'] <= 0 or fact['revision'] < 0):
            raise Error('执行回执字段类型无效')
        if (type(fact['valid_until']) not in (int, float)
                or isinstance(fact['valid_until'], bool)
                or not math.isfinite(fact['valid_until'])):
            raise Error('执行回执有效期无效')
        for key in ('grant_id', 'session_id', 'execution_id'):
            if type(fact[key]) is not str or not fact[key] or len(fact[key]) > 128:
                raise Error('执行回执字段类型无效')
        for key in ('digest', 'runtime_digest'):
            if (type(fact[key]) is not str or len(fact[key]) != 64
                    or any(c not in '0123456789abcdef' for c in fact[key])):
                raise Error('执行回执摘要无效')
        if fact['protocol'] not in ('vless', 'reality'):
            raise Error('执行协议无效')
        try:
            uuid.UUID(fact['execution_id'])
        except (ValueError, AttributeError):
            raise Error('执行编号无效')
        return json.dumps(fact, sort_keys=True, separators=(',', ':'), allow_nan=False)

    def record(self, token, fact):
        encoded = self._validate_fact(fact)
        r = self.registry
        r._session_value(fact['grant_id'])
        r._session_value(fact['session_id'])
        with r.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            sid = r._agent_id(db, token)
            r._check_session(db, sid, fact['session_id'])
            fingerprint = db.execute(
                'SELECT token_hash FROM agents WHERE server_id=?', (sid,)).fetchone()[0]
            grant = db.execute(
                'SELECT * FROM usage_first_total_grants WHERE grant_id=?',
                (fact['grant_id'],)).fetchone()
            if (grant is None or grant['server_id'] != sid
                    or fact['server_id'] != sid
                    or grant['session_id'] != fact['session_id']
                    or grant['token_hash'] != fingerprint):
                raise Error('执行回执身份不匹配', 403)
            cap = db.execute(
                'SELECT * FROM grant_capabilities WHERE server_id=?', (sid,)).fetchone()
            if (cap is None or cap['session_id'] != fact['session_id']
                    or cap['token_hash'] != fingerprint
                    or INSTALLATION not in protocols(json.loads(cap['offer']))):
                raise Error('当前会话未协商安装协议', 409)
            configuration = db.execute(
                'SELECT * FROM usage_first_total_configurations WHERE grant_id=?',
                (fact['grant_id'],)).fetchone()
            applied = db.execute(
                'SELECT * FROM usage_first_total_applied WHERE grant_id=?',
                (fact['grant_id'],)).fetchone()
            lifecycle = db.execute(
                'SELECT * FROM usage_first_total_lifecycle WHERE grant_id=?',
                (fact['grant_id'],)).fetchone()
            if configuration is None or applied is None or lifecycle is None:
                raise Error('缺少授予安装或生命周期记录', 409)
            installation = db.execute(
                'SELECT fact FROM usage_first_total_installations WHERE grant_id=?',
                (fact['grant_id'],)).fetchone()
            expected_installation = {key: value for key, value in fact.items()
                                     if key != 'execution_id'}
            if (installation is None or installation['fact'] != json.dumps(
                    expected_installation, sort_keys=True, separators=(',', ':'),
                    allow_nan=False)):
                raise Error('执行回执与原安装事实不匹配', 409)
            if (lifecycle['server_id'], lifecycle['session_id'],
                    lifecycle['configuration_digest']) != (
                        sid, fact['session_id'], configuration['digest']):
                raise Error('授予生命周期身份不匹配', 409)
            if lifecycle['state'] != 'installed':
                raise Error('授予尚未处于已安装状态', 409)
            if (applied['config_version'], applied['config_digest'],
                    applied['valid_until']) != (fact['revision'], fact['digest'],
                                                 fact['valid_until']):
                raise Error('执行回执与安装摘要不匹配', 409)
            if fact['valid_until'] > grant['valid_until']:
                raise Error('执行回执有效期超过授予期限', 409)
            old = db.execute(
                'SELECT fact,recorded_at FROM usage_first_total_execution WHERE grant_id=?',
                (fact['grant_id'],)).fetchone()
            if old is not None:
                if old['fact'] != encoded:
                    raise Error('执行回执与原记录冲突', 409)
                return _receipt(fact, old['recorded_at'])
            if not grant['issued_at'] <= time.time() < fact['valid_until']:
                raise Error('授予窗口不可用', 409)
            now = time.time()
            db.execute('INSERT INTO usage_first_total_execution VALUES(?,?,?)',
                       (fact['grant_id'], encoded, now))
            return _receipt(fact, now)
